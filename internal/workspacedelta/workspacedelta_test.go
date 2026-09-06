package workspacedelta

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildClassifiesNoChangeAndReadOnlyMutation(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "README.md", "before\n", 0o640)
	baseline := captureTestBaseline(t, root, Options{})

	unchanged, err := Build(baseline, root, IntentRead)
	if err != nil {
		t.Fatalf("Build unchanged: %v", err)
	}
	if unchanged.Classification != ClassificationNoChange {
		t.Fatalf("classification = %q, want %q", unchanged.Classification, ClassificationNoChange)
	}
	if err := unchanged.Validate(); err != nil {
		t.Fatalf("validate unchanged result: %v", err)
	}

	writeTestFile(t, root, "README.md", "after\n", 0o600)
	modified, err := Build(baseline, root, IntentRead)
	if err != nil {
		t.Fatalf("Build read-only mutation: %v", err)
	}
	if modified.Classification != ClassificationReadOnlyModified {
		t.Fatalf("classification = %q, want %q", modified.Classification, ClassificationReadOnlyModified)
	}
	if len(modified.Changes) != 1 || modified.Changes[0].Path != "README.md" || modified.Changes[0].Kind != EntryFile {
		t.Fatalf("unexpected changes: %#v", modified.Changes)
	}
	if len(modified.Artifact) != 0 || modified.ArtifactDigest != "" || len(modified.Manifest) != 0 {
		t.Fatal("read-only mutation produced a publication artifact")
	}
	if err := modified.Validate(); err != nil {
		t.Fatalf("validate read-only result: %v", err)
	}
}

func TestBuildProducesDeterministicDeltaWithDeletionAndSymlinkMetadata(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "keep.txt", "old\n", 0o600)
	writeTestFile(t, root, "delete.txt", "remove me\n", 0o644)
	writeTestFile(t, root, ".git/config", "[safe]\n", 0o600)
	baseline := captureTestBaseline(t, root, Options{})

	writeTestFile(t, root, "keep.txt", "new\n", 0o666)
	if err := os.Remove(filepath.Join(root, "delete.txt")); err != nil {
		t.Fatalf("remove deleted file: %v", err)
	}
	writeTestFile(t, root, "bin/run", "#!/bin/sh\nexit 0\n", 0o711)
	if err := os.Symlink("keep.txt", filepath.Join(root, "latest")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	result, err := Build(baseline, root, IntentWrite)
	if err != nil {
		t.Fatalf("Build write delta: %v", err)
	}
	if result.Classification != ClassificationWriteDelta {
		t.Fatalf("classification = %q, want %q", result.Classification, ClassificationWriteDelta)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("validate write result: %v", err)
	}
	assertDigest(t, result.ManifestDigest, result.Manifest)
	assertDigest(t, result.ArtifactDigest, result.Artifact)

	entries, order := readTestArchive(t, result.Artifact)
	wantOrder := append([]string(nil), order...)
	sort.Strings(wantOrder)
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("tar order = %v, want sorted %v", order, wantOrder)
	}
	for _, name := range []string{
		"files/bin/run", "files/keep.txt", deletionsArchivePath, manifestArchivePath, symlinksArchivePath,
	} {
		if _, found := entries[name]; !found {
			t.Errorf("archive missing %q", name)
		}
	}
	for name := range entries {
		if strings.Contains(name, ".git") || name == "files/latest" {
			t.Errorf("unsafe or metadata-only path %q included as file payload", name)
		}
	}
	if got := string(entries["files/keep.txt"].data); got != "new\n" {
		t.Errorf("keep payload = %q", got)
	}
	if mode := entries["files/keep.txt"].header.Mode; mode != 0o644 {
		t.Errorf("normalized regular mode = %#o, want 0644", mode)
	}
	if mode := entries["files/bin/run"].header.Mode; mode != 0o755 {
		t.Errorf("normalized executable mode = %#o, want 0755", mode)
	}
	for name, archived := range entries {
		if !archived.header.ModTime.Equal(time.Unix(0, 0).UTC()) || archived.header.Uid != 0 || archived.header.Gid != 0 {
			t.Errorf("entry %q has nondeterministic metadata: mtime=%s uid=%d gid=%d", name, archived.header.ModTime, archived.header.Uid, archived.header.Gid)
		}
	}

	var deletions deletionDocument
	if err := json.Unmarshal(entries[deletionsArchivePath].data, &deletions); err != nil {
		t.Fatalf("decode deletion manifest: %v", err)
	}
	if len(deletions.Deletions) != 1 || deletions.Deletions[0] != (Deletion{Path: "delete.txt", Kind: EntryFile}) {
		t.Fatalf("deletions = %#v", deletions.Deletions)
	}
	var links symlinkDocument
	if err := json.Unmarshal(entries[symlinksArchivePath].data, &links); err != nil {
		t.Fatalf("decode symlink manifest: %v", err)
	}
	if len(links.Symlinks) != 1 || links.Symlinks[0] != (Symlink{Path: "latest", Target: "keep.txt", Mode: 0o777}) {
		t.Fatalf("symlinks = %#v", links.Symlinks)
	}
	var manifest Manifest
	if err := json.Unmarshal(result.Manifest, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	assertDigest(t, manifest.DeletionsDigest, entries[deletionsArchivePath].data)
	assertDigest(t, manifest.SymlinksDigest, entries[symlinksArchivePath].data)
}

func TestBuildOutputIsIndependentOfCreationOrderTimestampsAndNonExecutableModes(t *testing.T) {
	baselineRoot := t.TempDir()
	writeTestFile(t, baselineRoot, "old.txt", "old\n", 0o644)
	baseline := captureTestBaseline(t, baselineRoot, Options{})

	first := t.TempDir()
	second := t.TempDir()
	populate := func(t *testing.T, root string, reverse bool) {
		t.Helper()
		writeTestFile(t, root, "old.txt", "changed\n", 0o600)
		paths := []string{"zeta.txt", "nested/alpha.txt"}
		if reverse {
			paths[0], paths[1] = paths[1], paths[0]
		}
		for _, name := range paths {
			writeTestFile(t, root, name, name+"\n", 0o660)
		}
		if err := os.Symlink("nested/alpha.txt", filepath.Join(root, "current")); err != nil {
			t.Fatalf("create deterministic symlink: %v", err)
		}
	}
	populate(t, first, false)
	populate(t, second, true)
	old := time.Unix(1_000, 0)
	future := time.Unix(9_000_000, 0)
	if err := filepath.Walk(first, func(name string, _ os.FileInfo, err error) error {
		if err == nil {
			return os.Chtimes(name, old, old)
		}
		return err
	}); err != nil {
		t.Fatalf("set first timestamps: %v", err)
	}
	if err := filepath.Walk(second, func(name string, _ os.FileInfo, err error) error {
		if err == nil {
			return os.Chtimes(name, future, future)
		}
		return err
	}); err != nil {
		t.Fatalf("set second timestamps: %v", err)
	}

	one, err := Build(baseline, first, IntentWrite)
	if err != nil {
		t.Fatalf("Build first: %v", err)
	}
	two, err := Build(baseline, second, IntentWrite)
	if err != nil {
		t.Fatalf("Build second: %v", err)
	}
	if !bytes.Equal(one.Manifest, two.Manifest) || one.ManifestDigest != two.ManifestDigest {
		t.Fatalf("manifests differ:\n%s\n%s", one.Manifest, two.Manifest)
	}
	if !bytes.Equal(one.Artifact, two.Artifact) || one.ArtifactDigest != two.ArtifactDigest {
		t.Fatalf("artifacts are not deterministic: %s != %s", one.ArtifactDigest, two.ArtifactDigest)
	}
}

func TestCaptureRejectsUnsafeSymlinks(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, root string)
	}{
		{name: "absolute", setup: func(t *testing.T, root string) {
			if err := os.Symlink("/etc/passwd", filepath.Join(root, "escape")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "traversal", setup: func(t *testing.T, root string) {
			if err := os.Symlink("../../outside", filepath.Join(root, "escape")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "protected target", setup: func(t *testing.T, root string) {
			writeTestFile(t, root, ".git/config", "safe\n", 0o600)
			if err := os.Symlink(".git/config", filepath.Join(root, "escape")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "cycle", setup: func(t *testing.T, root string) {
			if err := os.Symlink("b", filepath.Join(root, "a")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("a", filepath.Join(root, "b")); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			test.setup(t, root)
			_, err := Capture(root, Options{})
			if !errors.Is(err, ErrUnsafeSymlink) {
				t.Fatalf("Capture error = %v, want ErrUnsafeSymlink", err)
			}
		})
	}
}

func TestReadOnlySkillsAliasIsAcceptedButRemainsProtected(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, ".claude/skills/example/SKILL.md", "baseline\n", 0o644)
	if err := os.MkdirAll(filepath.Join(root, ".agents"), 0o755); err != nil {
		t.Fatalf("create .agents: %v", err)
	}
	aliasPath := filepath.Join(root, readOnlySkillsAliasPath)
	if err := os.Symlink(readOnlySkillsAliasTarget, aliasPath); err != nil {
		t.Fatalf("create skills alias: %v", err)
	}

	baseline := captureTestBaseline(t, root, Options{})
	unchanged, err := Build(baseline, root, IntentWrite)
	if err != nil {
		t.Fatalf("Build unchanged alias: %v", err)
	}
	if unchanged.Classification != ClassificationNoChange {
		t.Fatalf("classification = %q, want %q", unchanged.Classification, ClassificationNoChange)
	}

	writeTestFile(t, root, ".claude/skills/example/SKILL.md", "modified\n", 0o644)
	if _, err := Build(baseline, root, IntentWrite); !errors.Is(err, ErrExcludedPathModified) {
		t.Fatalf("Build modified alias target error = %v, want ErrExcludedPathModified", err)
	}

	writeTestFile(t, root, ".claude/skills/example/SKILL.md", "baseline\n", 0o644)
	if err := os.Remove(aliasPath); err != nil {
		t.Fatalf("remove skills alias: %v", err)
	}
	if err := os.Symlink("../src", aliasPath); err != nil {
		t.Fatalf("replace skills alias: %v", err)
	}
	if _, err := Build(baseline, root, IntentWrite); !errors.Is(err, ErrExcludedPathModified) {
		t.Fatalf("Build replaced alias error = %v, want ErrExcludedPathModified", err)
	}
}

func TestReadOnlySkillsAliasExceptionIsExact(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		target string
		setup  func(t *testing.T, root string)
	}{
		{
			name: "different link path", path: ".agents/other", target: readOnlySkillsAliasTarget,
			setup: func(t *testing.T, root string) {
				writeTestFile(t, root, ".claude/skills/example/SKILL.md", "baseline\n", 0o644)
			},
		},
		{
			name: "different protected target", path: readOnlySkillsAliasPath, target: "../.claude/settings.json",
			setup: func(t *testing.T, root string) {
				writeTestFile(t, root, ".claude/settings.json", "{}\n", 0o644)
			},
		},
		{
			name: "missing target", path: readOnlySkillsAliasPath, target: readOnlySkillsAliasTarget,
			setup: func(t *testing.T, root string) {},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			test.setup(t, root)
			if err := os.MkdirAll(filepath.Dir(filepath.Join(root, test.path)), 0o755); err != nil {
				t.Fatalf("create alias parent: %v", err)
			}
			if err := os.Symlink(test.target, filepath.Join(root, test.path)); err != nil {
				t.Fatalf("create alias: %v", err)
			}
			if _, err := Capture(root, Options{}); !errors.Is(err, ErrUnsafeSymlink) {
				t.Fatalf("Capture error = %v, want ErrUnsafeSymlink", err)
			}
		})
	}
}

func TestBuildRejectsGitAndExcludedRootMutation(t *testing.T) {
	for _, protectedPath := range []string{".git/config", ".codex/config.toml"} {
		t.Run(protectedPath, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, "source.txt", "stable\n", 0o644)
			writeTestFile(t, root, protectedPath, "before\n", 0o600)
			baseline := captureTestBaseline(t, root, Options{})
			writeTestFile(t, root, protectedPath, "after\n", 0o600)
			_, err := Build(baseline, root, IntentWrite)
			if !errors.Is(err, ErrExcludedPathModified) {
				t.Fatalf("Build error = %v, want ErrExcludedPathModified", err)
			}
		})
	}
}

func TestBuildRejectsReservedInfrastructurePathMutation(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "source.txt", "stable\n", 0o644)
	baseline := captureTestBaseline(t, root, Options{})
	writeTestFile(t, root, ".orka-infrastructure/state", "attacker-controlled\n", 0o600)
	_, err := Build(baseline, root, IntentWrite)
	if !errors.Is(err, ErrReservedPath) {
		t.Fatalf("Build error = %v, want ErrReservedPath", err)
	}
}

func TestCaptureRejectsRootSymlinkAndTraversalPolicy(t *testing.T) {
	realRoot := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Fatalf("create root symlink: %v", err)
	}
	if _, err := Capture(linkRoot, Options{}); !errors.Is(err, ErrInvalidRoot) {
		t.Fatalf("Capture root symlink error = %v, want ErrInvalidRoot", err)
	}
	if _, err := Capture(realRoot, Options{AdditionalReservedNames: []string{"../escape"}}); err == nil {
		t.Fatal("Capture accepted traversing reserved path policy")
	}
	if err := validateArchivePath("../escape", DefaultLimits().MaxPathBytes); !errors.Is(err, ErrPathTraversal) {
		t.Fatalf("validateArchivePath error = %v, want ErrPathTraversal", err)
	}
}

func TestCaptureAndArtifactLimits(t *testing.T) {
	t.Run("single file", func(t *testing.T) {
		root := t.TempDir()
		writeTestFile(t, root, "large", "12345", 0o644)
		_, err := Capture(root, Options{Limits: Limits{MaxFileBytes: 4, MaxTotalBytes: 8}})
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Capture error = %v, want ErrLimitExceeded", err)
		}
	})
	t.Run("total bytes", func(t *testing.T) {
		root := t.TempDir()
		writeTestFile(t, root, "one", "123", 0o644)
		writeTestFile(t, root, "two", "456", 0o644)
		_, err := Capture(root, Options{Limits: Limits{MaxFileBytes: 4, MaxTotalBytes: 5}})
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Capture error = %v, want ErrLimitExceeded", err)
		}
	})
	t.Run("entry count", func(t *testing.T) {
		root := t.TempDir()
		writeTestFile(t, root, "one", "1", 0o644)
		writeTestFile(t, root, "two", "2", 0o644)
		_, err := Capture(root, Options{Limits: Limits{MaxEntries: 1}})
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Capture error = %v, want ErrLimitExceeded", err)
		}
	})
	t.Run("artifact bytes", func(t *testing.T) {
		root := t.TempDir()
		writeTestFile(t, root, "file", "before", 0o644)
		baseline := captureTestBaseline(t, root, Options{Limits: Limits{MaxArtifactBytes: 512}})
		writeTestFile(t, root, "file", "after", 0o644)
		_, err := Build(baseline, root, IntentWrite)
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("Build error = %v, want ErrLimitExceeded", err)
		}
	})
}

func TestBuildRejectsForgedBaseline(t *testing.T) {
	root := t.TempDir()
	if _, err := Build(&Snapshot{}, root, IntentWrite); !errors.Is(err, ErrInvalidBaseline) {
		t.Fatalf("Build error = %v, want ErrInvalidBaseline", err)
	}
}

func captureTestBaseline(t *testing.T, root string, options Options) *Snapshot {
	t.Helper()
	baseline, err := Capture(root, options)
	if err != nil {
		t.Fatalf("Capture baseline: %v", err)
	}
	if baseline.ManifestDigest() == "" {
		t.Fatal("baseline has no manifest digest")
	}
	return baseline
}

func writeTestFile(t *testing.T, root, name, content string, mode os.FileMode) {
	t.Helper()
	filePath := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(filePath, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if err := os.Chmod(filePath, mode); err != nil {
		t.Fatalf("chmod %s: %v", name, err)
	}
}

func assertDigest(t *testing.T, got string, value []byte) {
	t.Helper()
	sum := sha256.Sum256(value)
	want := DigestPrefix + hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
}

type archivedEntry struct {
	header tar.Header
	data   []byte
}

func readTestArchive(t *testing.T, artifact []byte) (map[string]archivedEntry, []string) {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(artifact))
	entries := make(map[string]archivedEntry)
	var order []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read tar entry %s: %v", header.Name, err)
		}
		copyHeader := *header
		entries[header.Name] = archivedEntry{header: copyHeader, data: data}
		order = append(order, header.Name)
	}
	return entries, order
}

func TestCaptureContentFlaggerMarksBaselinePaths(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "flagged.js"), []byte("secret marker content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "plain.js"), []byte("ordinary content"), 0o644); err != nil {
		t.Fatal(err)
	}

	snapshot, err := Capture(root, Options{ContentFlagger: func(content []byte) bool {
		return bytes.Contains(content, []byte("secret marker"))
	}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if !snapshot.BaselineContentFlagged("flagged.js") {
		t.Fatal("flagged.js was not marked")
	}
	if snapshot.BaselineContentFlagged("nested/plain.js") {
		t.Fatal("nested/plain.js was wrongly marked")
	}
	if snapshot.BaselineContentFlagged("absent.js") {
		t.Fatal("unknown path reported flagged")
	}
	var nilSnapshot *Snapshot
	if nilSnapshot.BaselineContentFlagged("flagged.js") {
		t.Fatal("nil snapshot reported flagged")
	}

	// Without a flagger nothing is marked.
	snapshot, err = Capture(root, Options{})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if snapshot.BaselineContentFlagged("flagged.js") {
		t.Fatal("flagger-less capture marked a path")
	}
}

func TestCaptureContentFingerprinterOnlyRunsForFlaggedFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "flagged.js"), []byte("secret marker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "plain.js"), []byte("ordinary\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var fingerprinted []string
	snapshot, err := Capture(root, Options{
		ContentFlagger: func(content []byte) bool { return bytes.Contains(content, []byte("secret marker")) },
		ContentFingerprinter: func(content []byte) []string {
			fingerprinted = append(fingerprinted, string(bytes.TrimSpace(content)))
			return []string{"fp"}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fingerprinted) != 1 || fingerprinted[0] != "secret marker" {
		t.Fatalf("fingerprinter ran for %v, want only the flagged file", fingerprinted)
	}
	if got := snapshot.BaselineContentFingerprints("plain.js"); got != nil {
		t.Fatalf("unflagged file fingerprints = %v, want none", got)
	}
}

func TestBuildDoesNotRerunContentPolicyOnPostCapture(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "app.js"), []byte("secret marker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var flaggerCalls, fingerprinterCalls atomic.Int32
	snapshot, err := Capture(root, Options{
		ContentFlagger:       func([]byte) bool { flaggerCalls.Add(1); return true },
		ContentFingerprinter: func([]byte) []string { fingerprinterCalls.Add(1); return []string{"fp"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	if flaggerCalls.Load() != 1 || fingerprinterCalls.Load() != 1 {
		t.Fatalf("baseline calls flagger=%d fingerprinter=%d, want 1/1", flaggerCalls.Load(), fingerprinterCalls.Load())
	}
	if err := os.WriteFile(filepath.Join(root, "app.js"), []byte("secret marker\nmore\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(snapshot, root, IntentWrite); err != nil {
		t.Fatal(err)
	}
	if flaggerCalls.Load() != 1 || fingerprinterCalls.Load() != 1 {
		t.Fatalf("post-capture reran content policy: flagger=%d fingerprinter=%d", flaggerCalls.Load(), fingerprinterCalls.Load())
	}
	if !snapshot.BaselineContentFlagged("app.js") || len(snapshot.BaselineContentFingerprints("app.js")) != 1 {
		t.Fatal("baseline flags were lost")
	}
}

func TestCaptureContentFingerprinterRecordsBaselineFragments(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "flagged.js"), []byte("a\nSECRET-LINE\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshot, err := Capture(root, Options{ContentFingerprinter: func(content []byte) []string {
		if strings.Contains(string(content), "SECRET-LINE") {
			return []string{"fp-secret-line"}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.BaselineContentFingerprints("flagged.js"); len(got) != 1 || got[0] != "fp-secret-line" {
		t.Fatalf("fingerprints = %v", got)
	}
	if got := snapshot.BaselineContentFingerprints("absent.js"); got != nil {
		t.Fatalf("absent path fingerprints = %v", got)
	}
	var nilSnapshot *Snapshot
	if got := nilSnapshot.BaselineContentFingerprints("flagged.js"); got != nil {
		t.Fatalf("nil snapshot fingerprints = %v", got)
	}
}
