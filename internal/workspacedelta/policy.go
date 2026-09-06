package workspacedelta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	defaultMaxEntries             = 100_000
	defaultMaxFileBytes     int64 = 256 << 20
	defaultMaxTotalBytes    int64 = 2 << 30
	defaultMaxArtifactBytes int64 = 3 << 30
	defaultMaxPathBytes           = 4 << 10
	defaultMaxSymlinkBytes        = 4 << 10

	// Repository tool conventions commonly expose the same checked-in skill
	// tree to multiple agents. This exact alias is read-only: both the link and
	// its protected target are fingerprinted and excluded from publication.
	readOnlySkillsAliasPath     = ".agents/skills"
	readOnlySkillsAliasTarget   = "../.claude/skills"
	readOnlySkillsAliasResolved = ".claude/skills"
)

var mandatoryExcludedNames = []string{
	".git",
	".orka-metadata",
	".orka-supervisor",
	".codex",
	".claude",
	".copilot",
	".config",
	".credentials",
	".secrets",
	".ssh",
	".gnupg",
	".aws",
	".azure",
	".docker",
	".kube",
}

var mandatoryReservedNames = []string{
	".orka-artifacts",
	".orka-credentials",
	".orka-infrastructure",
	".orka-publisher",
	".orka-runtime-control",
	".orka-workspace-delta",
}

// Limits bounds both snapshots and generated artifacts. Zero fields use the
// secure defaults; negative fields are invalid.
type Limits struct {
	MaxEntries       int
	MaxFileBytes     int64
	MaxTotalBytes    int64
	MaxArtifactBytes int64
	MaxPathBytes     int
	MaxSymlinkBytes  int
}

// Options adds deployment-specific protected and reserved path-component
// names. Built-in security names cannot be disabled.
type Options struct {
	Limits                  Limits
	AdditionalExcludedNames []string
	AdditionalReservedNames []string
	// ContentFlagger, when set, is evaluated against each regular file's
	// content during Capture. Flagged paths are queryable through
	// Snapshot.BaselineContentFlagged, letting callers distinguish content
	// that was already present in the trusted pre-prompt baseline from
	// content introduced afterwards. It never alters the manifest or the
	// options digest. Capture calls it from several goroutines at once, so
	// it must be safe for concurrent use.
	ContentFlagger func(content []byte) bool
	// ContentFingerprinter, when set, records opaque fingerprints of the
	// flagged fragments of each regular file (for example digests of its
	// secret-like lines) so a later delta can tell pre-existing flagged
	// content from content the agent introduced. Only fingerprints are kept.
	// Like ContentFlagger it must be safe for concurrent use.
	ContentFingerprinter func(content []byte) []string
}

type normalizedOptions struct {
	limits               Limits
	excludedNames        []string
	reservedNames        []string
	excludedSet          map[string]struct{}
	reservedSet          map[string]struct{}
	contentFlagger       func(content []byte) bool
	contentFingerprinter func(content []byte) []string
}

// withoutContentPolicy returns a copy of the options with the baseline-only
// content heuristics cleared.
func (o normalizedOptions) withoutContentPolicy() normalizedOptions {
	o.contentFlagger = nil
	o.contentFingerprinter = nil
	return o
}

// DefaultLimits returns the package's bounded first-release defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxEntries:       defaultMaxEntries,
		MaxFileBytes:     defaultMaxFileBytes,
		MaxTotalBytes:    defaultMaxTotalBytes,
		MaxArtifactBytes: defaultMaxArtifactBytes,
		MaxPathBytes:     defaultMaxPathBytes,
		MaxSymlinkBytes:  defaultMaxSymlinkBytes,
	}
}

func normalizeOptions(options Options) (normalizedOptions, error) {
	limits, err := normalizeLimits(options.Limits)
	if err != nil {
		return normalizedOptions{}, err
	}
	excluded, err := normalizeNames(mandatoryExcludedNames, options.AdditionalExcludedNames)
	if err != nil {
		return normalizedOptions{}, fmt.Errorf("excluded names: %w", err)
	}
	reserved, err := normalizeNames(mandatoryReservedNames, options.AdditionalReservedNames)
	if err != nil {
		return normalizedOptions{}, fmt.Errorf("reserved names: %w", err)
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		excludedSet[strings.ToLower(name)] = struct{}{}
	}
	reservedSet := make(map[string]struct{}, len(reserved))
	for _, name := range reserved {
		key := strings.ToLower(name)
		if _, exists := excludedSet[key]; exists {
			return normalizedOptions{}, fmt.Errorf("path component %q cannot be both excluded and reserved", name)
		}
		reservedSet[key] = struct{}{}
	}
	return normalizedOptions{
		limits: limits, excludedNames: excluded, reservedNames: reserved,
		excludedSet: excludedSet, reservedSet: reservedSet,
		contentFlagger:       options.ContentFlagger,
		contentFingerprinter: options.ContentFingerprinter,
	}, nil
}

func normalizeLimits(limits Limits) (Limits, error) {
	defaults := DefaultLimits()
	if limits.MaxEntries < 0 || limits.MaxFileBytes < 0 || limits.MaxTotalBytes < 0 ||
		limits.MaxArtifactBytes < 0 || limits.MaxPathBytes < 0 || limits.MaxSymlinkBytes < 0 {
		return Limits{}, fmt.Errorf("limits must not be negative")
	}
	if limits.MaxEntries == 0 {
		limits.MaxEntries = defaults.MaxEntries
	}
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaults.MaxFileBytes
	}
	if limits.MaxTotalBytes == 0 {
		limits.MaxTotalBytes = defaults.MaxTotalBytes
	}
	if limits.MaxArtifactBytes == 0 {
		limits.MaxArtifactBytes = defaults.MaxArtifactBytes
	}
	if limits.MaxPathBytes == 0 {
		limits.MaxPathBytes = defaults.MaxPathBytes
	}
	if limits.MaxSymlinkBytes == 0 {
		limits.MaxSymlinkBytes = defaults.MaxSymlinkBytes
	}
	if limits.MaxFileBytes > limits.MaxTotalBytes {
		return Limits{}, fmt.Errorf("max file bytes cannot exceed max total bytes")
	}
	return limits, nil
}

func normalizeNames(required, additional []string) ([]string, error) {
	set := make(map[string]string, len(required)+len(additional))
	for _, source := range [][]string{required, additional} {
		for _, raw := range source {
			name := strings.TrimSpace(raw)
			if err := validateComponentName(name); err != nil {
				return nil, err
			}
			key := strings.ToLower(name)
			if _, exists := set[key]; !exists {
				set[key] = name
			}
		}
	}
	out := make([]string, 0, len(set))
	for _, name := range set {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out, nil
}

func validateComponentName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") || !utf8.ValidString(name) {
		return fmt.Errorf("invalid path component name %q", name)
	}
	if hasControl(name) {
		return fmt.Errorf("path component name contains control characters")
	}
	return nil
}

func (o normalizedOptions) digest() (string, error) {
	value := struct {
		Schema        string   `json:"schema"`
		Limits        Limits   `json:"limits"`
		ExcludedNames []string `json:"excludedNames"`
		ReservedNames []string `json:"reservedNames"`
	}{
		Schema: ManifestSchema, Limits: o.limits,
		ExcludedNames: o.excludedNames, ReservedNames: o.reservedNames,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode workspace policy: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return DigestPrefix + hex.EncodeToString(sum[:]), nil
}

func (o normalizedOptions) classifyPath(path string) (protected bool, err error) {
	for component := range strings.SplitSeq(path, "/") {
		key := strings.ToLower(component)
		if _, found := o.reservedSet[key]; found {
			return false, pathError("validate", path, ErrReservedPath)
		}
		if _, found := o.excludedSet[key]; found {
			protected = true
		}
	}
	return protected, nil
}

func isReadOnlySkillsAlias(linkPath, target, resolved string) bool {
	return linkPath == readOnlySkillsAliasPath &&
		target == readOnlySkillsAliasTarget &&
		resolved == readOnlySkillsAliasResolved
}
