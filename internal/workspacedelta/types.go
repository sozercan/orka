package workspacedelta

import "fmt"

const (
	ManifestSchema = "orka.workspace-delta.v1"
	DigestPrefix   = "sha256:"
)

// Intent determines whether a non-empty delta is an error classification or a
// publication artifact.
type Intent string

const (
	IntentRead  Intent = "read"
	IntentWrite Intent = "write"
)

func (i Intent) validate() error {
	switch i {
	case IntentRead, IntentWrite:
		return nil
	default:
		return fmt.Errorf("unsupported workspace intent %q", i)
	}
}

// Classification is the result of comparing a trusted baseline with the
// frozen final workspace tree.
type Classification string

const (
	ClassificationNoChange         Classification = "no_change"
	ClassificationReadOnlyModified Classification = "read_only_modified"
	ClassificationWriteDelta       Classification = "write_delta"
)

// EntryKind is a normalized workspace entry type.
type EntryKind string

const (
	EntryDirectory EntryKind = "directory"
	EntryFile      EntryKind = "file"
	EntrySymlink   EntryKind = "symlink"
)

// ChangeOperation describes how a final-tree entry relates to the baseline.
type ChangeOperation string

const (
	ChangeAdded    ChangeOperation = "added"
	ChangeModified ChangeOperation = "modified"
	ChangeReplaced ChangeOperation = "replaced"
)

// Change is deterministic metadata for one added or changed final-tree entry.
// File contents are stored separately under files/<Path> in a write artifact.
type Change struct {
	Path      string          `json:"path"`
	Operation ChangeOperation `json:"operation"`
	Kind      EntryKind       `json:"kind"`
	Mode      int64           `json:"mode"`
	Size      int64           `json:"size,omitempty"`
	Digest    string          `json:"digest,omitempty"`
	Target    string          `json:"target,omitempty"`
}

// Deletion explicitly identifies one baseline entry absent from, or replaced
// in, the final tree.
type Deletion struct {
	Path string    `json:"path"`
	Kind EntryKind `json:"kind"`
}

// Symlink contains validated symlink metadata. Artifacts never contain tar
// symlink headers, which avoids extraction-order and link-following hazards.
type Symlink struct {
	Path   string `json:"path"`
	Target string `json:"target"`
	Mode   int64  `json:"mode"`
}

// Manifest is the canonical metadata embedded in each non-empty write
// artifact. Entries, deletions, and symlinks are sorted deterministically.
type Manifest struct {
	Schema          string   `json:"schema"`
	Entries         []Change `json:"entries"`
	DeletionsDigest string   `json:"deletionsDigest"`
	SymlinksDigest  string   `json:"symlinksDigest"`
}

// Result is the complete comparison result. Artifact and manifest bytes are
// present only for ClassificationWriteDelta.
type Result struct {
	Classification Classification
	Changes        []Change
	Deletions      []Deletion
	Symlinks       []Symlink
	Manifest       []byte
	ManifestDigest string
	Artifact       []byte
	ArtifactDigest string
}

// BuildLimits narrows resource limits while constructing a delta from a trusted
// baseline. Zero fields preserve the limits captured in the baseline.
type BuildLimits struct {
	MaxArtifactBytes int64
}

// Snapshot is an immutable trusted baseline created by Capture. Its internal
// entries and policy are intentionally not exported so callers cannot forge a
// baseline by assembling public fields.
type Snapshot struct {
	version        uint32
	entries        map[string]entry
	options        normalizedOptions
	optionsDigest  string
	manifestDigest string
	entryCount     int
	totalBytes     int64
}

type entry struct {
	path       string
	kind       EntryKind
	mode       int64
	size       int64
	digest     string
	target     string
	protected  bool
	sourceMode uint32
	content    []byte
	// flagged records the Options.ContentFlagger verdict for the content this
	// entry was captured with. It lives only in the in-memory Snapshot and
	// never enters the manifest.
	flagged bool
	// fingerprints records the Options.ContentFingerprinter output for the
	// captured content; like flagged it never enters the manifest.
	fingerprints []string
}

// BaselineContentFlagged reports whether the file captured at path was
// flagged by the capture ContentFlagger. Paths use the same slash-separated
// workspace-relative form as delta change paths. A nil snapshot, an unknown
// path, or a capture without a ContentFlagger reports false.
func (s *Snapshot) BaselineContentFlagged(path string) bool {
	if s == nil {
		return false
	}
	e, ok := s.entries[path]
	return ok && e.flagged
}

// BaselineContentFingerprints returns the fingerprints the capture
// ContentFingerprinter recorded for the file at path (nil when unknown).
func (s *Snapshot) BaselineContentFingerprints(path string) []string {
	if s == nil {
		return nil
	}
	e, ok := s.entries[path]
	if !ok {
		return nil
	}
	return append([]string(nil), e.fingerprints...)
}
