package workspacedelta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const snapshotVersion uint32 = 1

// Capture validates and fingerprints a workspace tree without following
// symlinks. The returned Snapshot is suitable as a trusted pre-prompt baseline.
func Capture(root string, options Options) (*Snapshot, error) {
	return CaptureContext(context.Background(), root, options)
}

// CaptureContext is Capture with cancellation propagated through workspace
// traversal and regular-file hashing.
func CaptureContext(ctx context.Context, root string, options Options) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, fmt.Errorf("workspace delta options: %w", err)
	}
	return capture(ctx, root, normalized, false)
}

func capture(ctx context.Context, root string, options normalizedOptions, retainContent bool) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root = filepath.Clean(root)
	if root == "." || root == "" {
		return nil, ErrInvalidRoot
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("absolute workspace root: %w", err)
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil {
		return nil, pathError("inspect root", "", fmt.Errorf("%w: %v", ErrInvalidRoot, err))
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, pathError("inspect root", "", ErrInvalidRoot)
	}

	optionsDigest, err := options.digest()
	if err != nil {
		return nil, err
	}
	snapshot := &Snapshot{
		version:       snapshotVersion,
		entries:       make(map[string]entry),
		options:       options,
		optionsDigest: optionsDigest,
	}
	seenFiles := make(map[fileIdentity]string)
	var policy *contentPolicyPool
	if options.contentFlagger != nil || options.contentFingerprinter != nil {
		policy = newContentPolicyPool(ctx, options)
	}

	err = filepath.WalkDir(absolute, func(filePath string, dirEntry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return pathError("walk", relativeForError(absolute, filePath), walkErr)
		}
		if filePath == absolute {
			return nil
		}
		relative, err := canonicalRelativePath(absolute, filePath, options.limits.MaxPathBytes)
		if err != nil {
			return err
		}
		protected, err := options.classifyPath(relative)
		if err != nil {
			return err
		}
		if snapshot.entryCount >= options.limits.MaxEntries {
			return pathError("capture", relative, fmt.Errorf("%w: entry count exceeds %d", ErrLimitExceeded, options.limits.MaxEntries))
		}

		info, err := os.Lstat(filePath)
		if err != nil {
			return pathError("inspect", relative, err)
		}
		current := entry{path: relative, protected: protected}
		switch mode := info.Mode(); {
		case mode.IsDir():
			current.kind = EntryDirectory
			current.mode = normalizedMode(EntryDirectory, mode)
			current.sourceMode = uint32(mode.Perm())
		case mode.IsRegular():
			current, err = captureRegular(ctx, filePath, relative, info, protected, options, retainContent, snapshot.totalBytes, seenFiles, policy)
			if err != nil {
				return err
			}
			snapshot.totalBytes += current.size
		case mode&os.ModeSymlink != 0:
			current, err = captureSymlink(filePath, relative, info, protected, options)
			if err != nil {
				return err
			}
		default:
			return pathError("capture", relative, fmt.Errorf("%w: mode %s", ErrUnsafeFileType, mode.String()))
		}
		snapshot.entries[relative] = current
		snapshot.entryCount++
		return nil
	})
	if policy != nil {
		if err != nil {
			policy.cancel()
		}
		verdicts := policy.close()
		if err == nil {
			err = ctx.Err()
		}
		for entryPath, verdict := range verdicts {
			current := snapshot.entries[entryPath]
			current.flagged = verdict.flagged
			current.fingerprints = verdict.fingerprints
			snapshot.entries[entryPath] = current
		}
	}
	if err != nil {
		return nil, err
	}
	if err := validateSymlinkGraph(ctx, snapshot.entries, options); err != nil {
		return nil, err
	}
	rootAfter, err := os.Lstat(absolute)
	if err != nil || rootAfter.Mode()&os.ModeSymlink != 0 || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return nil, pathError("revalidate root", "", ErrInvalidRoot)
	}
	snapshot.manifestDigest, err = snapshotDigest(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func captureRegular(
	ctx context.Context,
	filePath, relative string,
	initial os.FileInfo,
	protected bool,
	options normalizedOptions,
	retainContent bool,
	currentTotal int64,
	seen map[fileIdentity]string,
	policy *contentPolicyPool,
) (entry, error) {
	if err := ctx.Err(); err != nil {
		return entry{}, err
	}
	initialIdentity, links, err := identityAndLinks(initial)
	if err != nil {
		return entry{}, pathError("inspect hardlinks", relative, err)
	}
	if links != 1 {
		return entry{}, pathError("inspect hardlinks", relative, ErrHardlinkAmbiguous)
	}
	if previous, exists := seen[initialIdentity]; exists {
		return entry{}, pathError("inspect hardlinks", relative, fmt.Errorf("%w: aliases %q", ErrHardlinkAmbiguous, previous))
	}
	if initial.Size() < 0 || initial.Size() > options.limits.MaxFileBytes {
		return entry{}, pathError("capture", relative, fmt.Errorf("%w: file exceeds %d bytes", ErrLimitExceeded, options.limits.MaxFileBytes))
	}
	if currentTotal > options.limits.MaxTotalBytes-initial.Size() {
		return entry{}, pathError("capture", relative, fmt.Errorf("%w: total bytes exceed %d", ErrLimitExceeded, options.limits.MaxTotalBytes))
	}

	file, err := openRegularNoFollow(filePath)
	if err != nil {
		return entry{}, pathError("open no-follow", relative, fmt.Errorf("%w: %v", ErrUnsafeFileType, err))
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return entry{}, pathError("inspect open file", relative, err)
	}
	openedIdentity, openedLinks, err := identityAndLinks(opened)
	if err != nil {
		return entry{}, pathError("inspect hardlinks", relative, err)
	}
	if !opened.Mode().IsRegular() || openedIdentity != initialIdentity || openedLinks != 1 || opened.Size() != initial.Size() {
		return entry{}, pathError("revalidate open file", relative, ErrHardlinkAmbiguous)
	}

	var policyWeight int64
	if policy != nil {
		policyWeight, err = policy.reserve(opened.Size())
		if err != nil {
			return entry{}, pathError("content policy", relative, err)
		}
		defer func() {
			if policyWeight != 0 {
				policy.release(policyWeight)
			}
		}()
	}

	hash := sha256.New()
	var content bytes.Buffer
	writer := io.Writer(hash)
	if retainContent || policy != nil {
		writer = io.MultiWriter(hash, &content)
	}
	read, err := io.Copy(writer, io.LimitReader(&contextReader{ctx: ctx, reader: file}, options.limits.MaxFileBytes+1))
	if err != nil {
		return entry{}, pathError("read", relative, err)
	}
	if read > options.limits.MaxFileBytes || read != opened.Size() {
		return entry{}, pathError("read", relative, fmt.Errorf("%w: file changed or exceeded %d bytes", ErrLimitExceeded, options.limits.MaxFileBytes))
	}
	openedAfter, err := file.Stat()
	if err != nil {
		return entry{}, pathError("revalidate open file", relative, err)
	}
	openedAfterIdentity, openedAfterLinks, err := identityAndLinks(openedAfter)
	if err != nil {
		return entry{}, pathError("inspect hardlinks", relative, err)
	}
	pathAfter, err := os.Lstat(filePath)
	if err != nil {
		return entry{}, pathError("revalidate path", relative, err)
	}
	pathAfterIdentity, pathAfterLinks, err := identityAndLinks(pathAfter)
	if err != nil {
		return entry{}, pathError("inspect hardlinks", relative, err)
	}
	if openedAfterIdentity != initialIdentity || pathAfterIdentity != initialIdentity || openedAfterLinks != 1 || pathAfterLinks != 1 ||
		openedAfter.Size() != read || pathAfter.Size() != read || !openedAfter.Mode().IsRegular() || !pathAfter.Mode().IsRegular() {
		return entry{}, pathError("revalidate file", relative, ErrHardlinkAmbiguous)
	}
	seen[initialIdentity] = relative
	result := entry{
		path: relative, kind: EntryFile, mode: normalizedMode(EntryFile, openedAfter.Mode()),
		size: read, digest: DigestPrefix + hex.EncodeToString(hash.Sum(nil)), protected: protected,
		sourceMode: uint32(openedAfter.Mode().Perm()),
	}
	if retainContent {
		result.content = append([]byte(nil), content.Bytes()...)
	}
	// The content policy verdict is evaluated behind the walk and applied to
	// the entry once every file has been captured; the buffer is handed over
	// and not touched again here.
	if policy != nil {
		if err := policy.submit(relative, content.Bytes(), policyWeight); err != nil {
			return entry{}, pathError("content policy", relative, err)
		}
		policyWeight = 0
	}
	return result, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}

func captureSymlink(filePath, relative string, initial os.FileInfo, protected bool, options normalizedOptions) (entry, error) {
	initialIdentity, initialLinks, err := identityAndLinks(initial)
	if err != nil {
		return entry{}, pathError("inspect symlink", relative, err)
	}
	if initialLinks != 1 {
		return entry{}, pathError("inspect hardlinks", relative, ErrHardlinkAmbiguous)
	}
	target, err := os.Readlink(filePath)
	if err != nil {
		return entry{}, pathError("read symlink", relative, err)
	}
	resolved, err := validateSymlinkTarget(relative, target, options)
	if err != nil {
		return entry{}, err
	}
	protected = protected || isReadOnlySkillsAlias(relative, target, resolved)
	after, err := os.Lstat(filePath)
	if err != nil {
		return entry{}, pathError("revalidate symlink", relative, err)
	}
	afterIdentity, afterLinks, err := identityAndLinks(after)
	if err != nil {
		return entry{}, pathError("inspect symlink", relative, err)
	}
	if after.Mode()&os.ModeSymlink == 0 || afterIdentity != initialIdentity || afterLinks != 1 {
		return entry{}, pathError("revalidate symlink", relative, ErrUnsafeSymlink)
	}
	return entry{
		path: relative, kind: EntrySymlink, mode: normalizedMode(EntrySymlink, after.Mode()),
		target: target, protected: protected, sourceMode: uint32(after.Mode().Perm()),
	}, nil
}

func normalizedMode(kind EntryKind, mode os.FileMode) int64 {
	switch kind {
	case EntryDirectory:
		return 0o755
	case EntryFile:
		if mode.Perm()&0o111 != 0 {
			return 0o755
		}
		return 0o644
	case EntrySymlink:
		return 0o777
	default:
		return 0
	}
}

func relativeForError(root, filePath string) string {
	relative, err := filepath.Rel(root, filePath)
	if err != nil || relative == "." {
		return ""
	}
	return filepath.ToSlash(relative)
}

type snapshotManifestEntry struct {
	Path       string    `json:"path"`
	Kind       EntryKind `json:"kind"`
	Mode       int64     `json:"mode"`
	Size       int64     `json:"size,omitempty"`
	Digest     string    `json:"digest,omitempty"`
	Target     string    `json:"target,omitempty"`
	Protected  bool      `json:"protected,omitempty"`
	SourceMode uint32    `json:"sourceMode"`
}

func snapshotDigest(ctx context.Context, snapshot *Snapshot) (string, error) {
	paths := make([]string, 0, len(snapshot.entries))
	for entryPath := range snapshot.entries {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		paths = append(paths, entryPath)
	}
	sort.Strings(paths)
	entries := make([]snapshotManifestEntry, 0, len(paths))
	for _, entryPath := range paths {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		current := snapshot.entries[entryPath]
		entries = append(entries, snapshotManifestEntry{
			Path: current.path, Kind: current.kind, Mode: current.mode, Size: current.size,
			Digest: current.digest, Target: current.target, Protected: current.protected, SourceMode: current.sourceMode,
		})
	}
	manifest := struct {
		Schema       string                  `json:"schema"`
		PolicyDigest string                  `json:"policyDigest"`
		Entries      []snapshotManifestEntry `json:"entries"`
	}{Schema: ManifestSchema, PolicyDigest: snapshot.optionsDigest, Entries: entries}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode snapshot manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return DigestPrefix + hex.EncodeToString(sum[:]), nil
}
