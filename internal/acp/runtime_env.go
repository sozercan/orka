package acp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/orka-agents/orka/internal/taskterminal"
)

const (
	DefaultChildPath = "/usr/local/bin:/usr/bin:/bin"

	// Linux process credentials are uint32 values, with all-bits-one reserved
	// as the no-change sentinel by ownership syscalls.
	maxRuntimeCredentialID int64 = (1 << 32) - 2
)

var (
	ErrUIDRangeExhausted = errors.New("runtime session UID range exhausted")
	ErrUIDReserveReached = errors.New("runtime session UID reserve reached")
)

var (
	sessionPathComponentRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	environmentNameRE      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// IsValidSessionPathComponent reports whether value is safe as one session
// directory component.
func IsValidSessionPathComponent(value string) bool {
	return sessionPathComponentRE.MatchString(value)
}

type SessionPaths struct {
	Root      string
	Home      string
	Temp      string
	Workspace string
	Config    string
	Cache     string
	Data      string
	State     string
}

func PrepareSessionPaths(baseDir, sessionID string) (SessionPaths, error) {
	baseDir = filepath.Clean(strings.TrimSpace(baseDir))
	if baseDir == "." || !filepath.IsAbs(baseDir) {
		return SessionPaths{}, fmt.Errorf("session base directory must be absolute")
	}
	if !IsValidSessionPathComponent(sessionID) {
		return SessionPaths{}, fmt.Errorf("invalid session path component %q", sessionID)
	}
	if err := ensureRealDirectory(baseDir, 0o711); err != nil {
		return SessionPaths{}, err
	}
	root, err := os.MkdirTemp(baseDir, sessionID+"-")
	if err != nil {
		return SessionPaths{}, fmt.Errorf("create session root: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()
	paths := SessionPaths{
		Root:      root,
		Home:      filepath.Join(root, "home"),
		Temp:      filepath.Join(root, "tmp"),
		Workspace: filepath.Join(root, "workspace"),
		Config:    filepath.Join(root, "xdg", "config"),
		Cache:     filepath.Join(root, "xdg", "cache"),
		Data:      filepath.Join(root, "xdg", "data"),
		State:     filepath.Join(root, "xdg", "state"),
	}
	for _, path := range []string{paths.Root, paths.Home, paths.Temp, paths.Workspace, paths.Config, paths.Cache, paths.Data, paths.State} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return SessionPaths{}, fmt.Errorf("create private session directory: %w", err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return SessionPaths{}, fmt.Errorf("chmod private session directory: %w", err)
		}
	}
	cleanup = false
	return paths, nil
}

func ensureRealDirectory(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, mode); err != nil {
			return fmt.Errorf("create session base directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect session base directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("session base directory must be a real directory")
	}
	return nil
}

type EnvironmentConfig struct {
	PATH       string
	Values     map[string]string
	UnsetNames []string
}

func BuildChildEnvironment(paths SessionPaths, cfg EnvironmentConfig) ([]string, error) {
	for name, value := range map[string]string{
		"HOME":            paths.Home,
		"TMPDIR":          paths.Temp,
		"XDG_CONFIG_HOME": paths.Config,
		"XDG_CACHE_HOME":  paths.Cache,
		"XDG_DATA_HOME":   paths.Data,
		"XDG_STATE_HOME":  paths.State,
		"ORKA_WORKSPACE":  paths.Workspace,
		"PWD":             paths.Workspace,
	} {
		if strings.TrimSpace(value) == "" || !filepath.IsAbs(value) {
			return nil, fmt.Errorf("%s must be an absolute session path", name)
		}
	}
	pathValue := strings.TrimSpace(cfg.PATH)
	if pathValue == "" {
		pathValue = DefaultChildPath
	}
	values := map[string]string{
		"HOME":            paths.Home,
		"TMPDIR":          paths.Temp,
		"XDG_CONFIG_HOME": paths.Config,
		"XDG_CACHE_HOME":  paths.Cache,
		"XDG_DATA_HOME":   paths.Data,
		"XDG_STATE_HOME":  paths.State,
		"ORKA_WORKSPACE":  paths.Workspace,
		"PWD":             paths.Workspace,
		"PATH":            pathValue,
	}
	for name, value := range cfg.Values {
		name = strings.TrimSpace(name)
		if !environmentNameRE.MatchString(name) {
			return nil, fmt.Errorf("invalid child environment name %q", name)
		}
		if strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("child environment %s contains NUL", name)
		}
		values[name] = value
	}
	for _, name := range cfg.UnsetNames {
		delete(values, strings.TrimSpace(name))
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	env := make([]string, 0, len(names))
	for _, name := range names {
		env = append(env, name+"="+values[name])
	}
	return env, nil
}

type UIDAllocator struct {
	mu        sync.Mutex
	firstUID  int
	firstGID  int
	allocated int
	capacity  int
	persist   func(int) error
}

// NewUIDAllocator creates a never-reusing paired UID/GID allocator. firstGID
// is the first primary GID in a range with the same capacity as the UID range.
func NewUIDAllocator(firstUID, lastUID, firstGID int) (*UIDAllocator, error) {
	if firstUID <= 0 || lastUID < firstUID || firstGID <= 0 {
		return nil, fmt.Errorf("invalid UID allocator range %d-%d gid %d", firstUID, lastUID, firstGID)
	}
	capacity := int64(lastUID) - int64(firstUID) + 1
	maxAllocatorID := maxRuntimeCredentialID
	if maxInt := int64(^uint(0) >> 1); maxInt < maxAllocatorID {
		maxAllocatorID = maxInt
	}
	if int64(lastUID) > maxAllocatorID ||
		int64(firstGID) > maxAllocatorID-(capacity-1) {
		return nil, fmt.Errorf("invalid UID allocator range %d-%d gid %d", firstUID, lastUID, firstGID)
	}
	return &UIDAllocator{
		firstUID: firstUID,
		firstGID: firstGID,
		capacity: int(capacity),
	}, nil
}

// AllocateAboveReserve returns the next never-reused UID/GID pair only when
// doing so leaves at least reserve pairs unused. The check and allocation use a
// single shared offset in one critical section, so UID and primary GID cannot be
// reused or paired with different sessions under concurrent creation.
func (a *UIDAllocator) AllocateAboveReserve(reserve int) (uid, gid int, err error) {
	if a == nil {
		return 0, 0, fmt.Errorf("UID allocator is required")
	}
	if reserve < 0 {
		return 0, 0, fmt.Errorf("UID allocator reserve must be non-negative")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	remaining := a.capacity - a.allocated
	if remaining == 0 {
		return 0, 0, ErrUIDRangeExhausted
	}
	if remaining <= reserve {
		return 0, 0, ErrUIDReserveReached
	}
	offset := a.allocated
	nextAllocated := a.allocated + 1
	if a.persist != nil {
		if err := a.persist(nextAllocated); err != nil {
			return 0, 0, fmt.Errorf("persist runtime session identity high-water mark: %w", err)
		}
	}
	a.allocated = nextAllocated
	return a.firstUID + offset, a.firstGID + offset, nil
}

// ConfigurePersistence restores a previously committed allocation count and
// installs the fail-closed callback that must durably commit each future count
// before an identity is returned to a session.
func (a *UIDAllocator) ConfigurePersistence(allocated int, persist func(int) error) error {
	if a == nil {
		return fmt.Errorf("UID allocator is required")
	}
	if persist == nil {
		return fmt.Errorf("UID allocator persistence callback is required")
	}
	if allocated < 0 || allocated > a.capacity {
		return fmt.Errorf("invalid persisted UID allocation count %d", allocated)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.allocated != 0 || a.persist != nil {
		return fmt.Errorf("UID allocator persistence can only be configured once on a fresh allocator")
	}
	a.allocated = allocated
	a.persist = persist
	return nil
}

func (a *UIDAllocator) Capacity() int {
	if a == nil {
		return 0
	}
	return a.capacity
}

// Range returns the immutable paired UID/GID range represented by the
// allocator. Callers use it to bind durable high-water state to one exact
// identity configuration.
func (a *UIDAllocator) Range() (firstUID, lastUID, firstGID, lastGID int) {
	if a == nil {
		return 0, 0, 0, 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.firstUID, a.firstUID + a.capacity - 1, a.firstGID, a.firstGID + a.capacity - 1
}

func (a *UIDAllocator) Remaining() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.capacity - a.allocated
}

// DurableWorkspaceBinding is the committed identity of a durable session
// workspace. It pins the repository binding of the first materialization so a
// later cold resume can never silently continue against different source
// content. It never carries credentials or provider-native identifiers.
type DurableWorkspaceBinding struct {
	RepositoryIdentity string `json:"repositoryIdentity"`
	Revision           string `json:"revision"`
	// SessionIdentityHighWater records the allocator count that was durable
	// before this checkpoint's child identity was admitted. A cold boot rejects
	// an allocator state below this floor so a partial restore cannot reuse a
	// UID/GID already represented by surviving workspace data.
	SessionIdentityHighWater int `json:"sessionIdentityHighWater"`
	// SessionGeneration records the monotonic RuntimeSession generation that
	// committed this checkpoint. A provider restoring an OLDER data snapshot
	// of the same repository presents a valid identity with an earlier
	// generation; the resume assertion compares it against the controller's
	// persisted floor so stale restores fail closed instead of silently
	// dropping checkpoint-only edits from the next publication.
	SessionGeneration uint64 `json:"sessionGeneration,omitempty"`
}

// ErrDurableWorkspaceCheckpointUnusable marks a committed durable checkpoint
// whose marker and workspace tree cannot form a valid resumable pair.
var ErrDurableWorkspaceCheckpointUnusable = errors.New("durable workspace checkpoint is unusable")

// StableDurableWorkspaceIdentity reduces a protocol workspace baseline to the
// session-stable repository identity durable continuity is judged on. Empty
// (no-repository) workspaces carry a Task-scoped protocol identity by design,
// so every such baseline reduces to the bare no-workspace revision; repository
// workspaces are identified by the repository identity alone because the
// controller verifies revision continuity before requesting the session.
func StableDurableWorkspaceIdentity(repositoryIdentity, revision string) string {
	if revision == taskterminal.NoWorkspaceRevision {
		return taskterminal.NoWorkspaceRevision
	}
	return repositoryIdentity
}

// SameDurableWorkspaceIdentity reports whether two session-stable workspace
// identities (StableDurableWorkspaceIdentity outputs) identify the same
// repository. It mirrors the controller's repository-identity equivalence:
// GitHub identities are case-insensitive, so a continuation that changes only
// capitalization must resume the preserved tree instead of wiping it.
func SameDurableWorkspaceIdentity(first, second string) bool {
	first = strings.TrimSpace(first)
	second = strings.TrimSpace(second)
	if first == second {
		return true
	}
	return strings.HasPrefix(strings.ToLower(first), "github.com/") &&
		strings.HasPrefix(strings.ToLower(second), "github.com/") &&
		strings.EqualFold(first, second)
}

// PrepareDurableSessionWorkspace prepares the durable workspace directory for
// one logical session under the provider-owned durable root. Committed content
// (a marker plus the workspace directory) reports resume=true with the
// recorded binding. Fresh content is covered by a pending marker carrying the
// allocator high-water before its tree is created, so a restart can verify the
// non-reuse fence and the next preparation can wipe the partial materialization.
func PrepareDurableSessionWorkspace(
	durableRoot, sessionUID string,
	sessionIdentityHighWater int,
) (string, *DurableWorkspaceBinding, error) {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) {
		return "", nil, fmt.Errorf("durable workspace root must be absolute")
	}
	if !IsValidSessionPathComponent(sessionUID) {
		return "", nil, fmt.Errorf("invalid durable workspace session component %q", sessionUID)
	}
	if sessionIdentityHighWater <= 0 {
		return "", nil, fmt.Errorf("session identity high-water must be positive")
	}
	if err := ensureRealDirectory(durableRoot, 0o711); err != nil {
		return "", nil, err
	}
	workspaceDir := filepath.Join(durableRoot, "ws-"+sessionUID)
	markerPath := durableWorkspaceMarkerPath(durableRoot, sessionUID)
	if _, pendingErr := os.Lstat(durableWorkspacePendingMarkerPath(durableRoot, sessionUID)); pendingErr == nil {
		if _, markerErr := os.Lstat(markerPath); markerErr == nil {
			// The committed marker is renamed into place BEFORE the pending
			// marker is removed, and a resume moves (never copies) the
			// committed marker aside — so their coexistence proves a commit
			// completed and only its pending-retirement was interrupted.
			// Retire the stale pending marker instead of wiping the
			// successfully committed tree.
			if err := os.Remove(durableWorkspacePendingMarkerPath(durableRoot, sessionUID)); err != nil && !os.IsNotExist(err) {
				return "", nil, fmt.Errorf("retire stale durable workspace pending marker: %w", err)
			}
		} else if os.IsNotExist(markerErr) {
			// A resume marked its tree pending and never recommitted: a later
			// creation stage failed after the provider process may have
			// modified the repository, so the tree is wiped instead of reused.
			if err := WipeDurableSessionWorkspace(durableRoot, sessionUID); err != nil {
				return "", nil, err
			}
		} else {
			return "", nil, fmt.Errorf("read durable workspace marker: %w", markerErr)
		}
	} else if !os.IsNotExist(pendingErr) {
		return "", nil, fmt.Errorf("read durable workspace pending marker: %w", pendingErr)
	}
	raw, err := os.ReadFile(markerPath)
	if err == nil {
		binding := &DurableWorkspaceBinding{}
		if jsonErr := json.Unmarshal(raw, binding); jsonErr != nil {
			return "", nil, fmt.Errorf("%w: marker is unreadable: %v", ErrDurableWorkspaceCheckpointUnusable, jsonErr)
		}
		info, statErr := os.Lstat(workspaceDir)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return "", nil, fmt.Errorf("%w: marker exists without its workspace directory", ErrDurableWorkspaceCheckpointUnusable)
			}
			return "", nil, fmt.Errorf("inspect durable workspace directory: %w", statErr)
		}
		if !info.IsDir() {
			return "", nil, fmt.Errorf("%w: marker workspace path is not a directory", ErrDurableWorkspaceCheckpointUnusable)
		}
		// A transition record can survive a successful commit when its
		// post-commit cleanup was interrupted. Retire it before starting a
		// new child; a failure here is safely retryable because the committed
		// checkpoint remains intact and no session has been initialized.
		if err := os.Remove(durableWorkspaceTransitionMarkerPath(durableRoot, sessionUID)); err != nil && !os.IsNotExist(err) {
			return "", nil, fmt.Errorf("retire stale durable workspace transition record: %w", err)
		}
		return workspaceDir, binding, nil
	}
	if !os.IsNotExist(err) {
		return "", nil, fmt.Errorf("read durable workspace marker: %w", err)
	}
	if err := reclaimDurableWorkspaceTree(workspaceDir); err != nil {
		return "", nil, err
	}
	if err := os.RemoveAll(workspaceDir); err != nil {
		return "", nil, fmt.Errorf("clear uncommitted durable workspace: %w", err)
	}
	if err := stageDurableWorkspaceFreshPending(
		durableRoot, sessionUID, sessionIdentityHighWater,
	); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(workspaceDir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create durable workspace directory: %w", err)
	}
	return workspaceDir, nil, nil
}

// stageDurableWorkspaceFreshPending publishes the cleanup record before a
// fresh tree exists. A crash can therefore leave either no tree or a tree whose
// identity floor is recoverable; it cannot create markerless durable history.
func stageDurableWorkspaceFreshPending(
	durableRoot, sessionUID string,
	sessionIdentityHighWater int,
) error {
	encoded, err := json.Marshal(DurableWorkspaceBinding{
		SessionIdentityHighWater: sessionIdentityHighWater,
	})
	if err != nil {
		return fmt.Errorf("encode durable workspace fresh pending marker: %w", err)
	}
	staged, err := os.CreateTemp(durableRoot, ".pending-*")
	if err != nil {
		return fmt.Errorf("stage durable workspace fresh pending marker: %w", err)
	}
	stagedName := staged.Name()
	defer os.Remove(stagedName) //nolint:errcheck // best-effort removal of the staged marker
	if _, err := staged.Write(encoded); err != nil {
		_ = staged.Close()
		return fmt.Errorf("write durable workspace fresh pending marker: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return fmt.Errorf("sync durable workspace fresh pending marker: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close durable workspace fresh pending marker: %w", err)
	}
	if err := os.Rename(stagedName, durableWorkspacePendingMarkerPath(durableRoot, sessionUID)); err != nil {
		return fmt.Errorf("publish durable workspace fresh pending marker: %w", err)
	}
	if err := syncDurableWorkspaceRoot(durableRoot); err != nil {
		return fmt.Errorf("sync durable workspace fresh pending marker: %w", err)
	}
	return nil
}

// CommitDurableSessionWorkspace records the repository binding of a freshly
// materialized durable workspace. The marker write is atomic: content covered
// only by the pending marker is treated as uncommitted and wiped by the next
// preparation.
func CommitDurableSessionWorkspace(durableRoot, sessionUID string, binding DurableWorkspaceBinding) error {
	return commitDurableSessionWorkspace(
		durableRoot, sessionUID, binding,
		syncDurableWorkspaceRoot, os.Remove,
	)
}

func commitDurableSessionWorkspace(
	durableRoot, sessionUID string,
	binding DurableWorkspaceBinding,
	syncRoot func(string) error,
	remove func(string) error,
) error {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) || !IsValidSessionPathComponent(sessionUID) {
		return fmt.Errorf("absolute durable root and a valid session component are required")
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("encode durable workspace marker: %w", err)
	}
	markerPath := durableWorkspaceMarkerPath(durableRoot, sessionUID)
	staged, err := os.CreateTemp(durableRoot, ".marker-*")
	if err != nil {
		return fmt.Errorf("stage durable workspace marker: %w", err)
	}
	stagedName := staged.Name()
	defer os.Remove(stagedName) //nolint:errcheck // best-effort removal of the staged marker
	if _, err := staged.Write(encoded); err != nil {
		_ = staged.Close()
		return fmt.Errorf("write durable workspace marker: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return fmt.Errorf("sync durable workspace marker: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close durable workspace marker: %w", err)
	}
	if err := os.Rename(stagedName, markerPath); err != nil {
		return fmt.Errorf("commit durable workspace marker: %w", err)
	}
	// Publish the committed marker durably before retiring the pending records.
	// Once this sync succeeds, the commit is complete. Cleanup failures cannot
	// be returned to the caller: doing so would tear down an initialized child
	// while leaving its tree durably resumable. Prepare and the next commit both
	// retry stale-record retirement.
	if err := syncRoot(durableRoot); err != nil {
		syncErr := fmt.Errorf("sync durable workspace marker commit: %w", err)
		// The rename is already visible even though its durability barrier
		// failed. Restore pending-only state before returning so a retry wipes
		// the initialized child's possibly modified tree instead of mistaking
		// both markers for interrupted post-commit cleanup.
		if rollbackErr := remove(markerPath); rollbackErr != nil && !os.IsNotExist(rollbackErr) {
			return errors.Join(syncErr, fmt.Errorf("roll back durable workspace marker commit: %w", rollbackErr))
		}
		if rollbackSyncErr := syncRoot(durableRoot); rollbackSyncErr != nil {
			return errors.Join(syncErr, fmt.Errorf("sync durable workspace marker rollback: %w", rollbackSyncErr))
		}
		return syncErr
	}
	_ = remove(durableWorkspacePendingMarkerPath(durableRoot, sessionUID))
	_ = remove(durableWorkspaceTransitionMarkerPath(durableRoot, sessionUID))
	return nil
}

// MarkDurableSessionWorkspaceResumePending invalidates the committed marker of
// a resumed tree before session initialization runs: the marker moves aside
// atomically, so a creation that fails after the provider process may have
// modified the repository leaves a pending record and the next preparation
// wipes the tree instead of reusing partial state. A successful creation
// recommits the marker, which retires the pending record.
func MarkDurableSessionWorkspaceResumePending(durableRoot, sessionUID string) error {
	return markDurableSessionWorkspaceResumePending(durableRoot, sessionUID, syncDurableWorkspaceRoot)
}

func markDurableSessionWorkspaceResumePending(
	durableRoot, sessionUID string,
	syncRoot func(string) error,
) error {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) || !IsValidSessionPathComponent(sessionUID) {
		return fmt.Errorf("absolute durable root and a valid session component are required")
	}
	markerPath := durableWorkspaceMarkerPath(durableRoot, sessionUID)
	pendingPath := durableWorkspacePendingMarkerPath(durableRoot, sessionUID)
	if err := os.Rename(
		markerPath,
		pendingPath,
	); err != nil {
		return fmt.Errorf("mark durable workspace resume pending: %w", err)
	}
	if err := syncRoot(durableRoot); err != nil {
		syncErr := fmt.Errorf("sync durable workspace resume pending marker: %w", err)
		if rollbackErr := os.Rename(pendingPath, markerPath); rollbackErr != nil {
			return errors.Join(syncErr, fmt.Errorf("restore committed durable workspace marker: %w", rollbackErr))
		}
		if rollbackSyncErr := syncRoot(durableRoot); rollbackSyncErr != nil {
			return errors.Join(syncErr, fmt.Errorf("sync restored durable workspace marker: %w", rollbackSyncErr))
		}
		return syncErr
	}
	return nil
}

// WipeDurableSessionWorkspace removes one logical session's durable tree and
// both of its markers, so the next preparation materializes fresh. A committed
// binding is moved to the pending path, or an existing pending binding is kept,
// until tree removal is durable so a crash always leaves a recognizable cleanup
// record beside any surviving workspace history.
func WipeDurableSessionWorkspace(durableRoot, sessionUID string) error {
	return wipeDurableSessionWorkspace(durableRoot, sessionUID, syncDurableWorkspaceRoot)
}

func wipeDurableSessionWorkspace(
	durableRoot, sessionUID string,
	syncRoot func(string) error,
) error {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) || !IsValidSessionPathComponent(sessionUID) {
		return fmt.Errorf("absolute durable root and a valid session component are required")
	}
	markerPath := durableWorkspaceMarkerPath(durableRoot, sessionUID)
	pendingPath := durableWorkspacePendingMarkerPath(durableRoot, sessionUID)
	if _, err := os.Lstat(markerPath); err == nil {
		// Prefer the newest committed binding as the cleanup record. Removing
		// an older pending record first is safe because the committed marker
		// still covers the tree until the atomic rename succeeds.
		if err := os.Remove(pendingPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("replace durable workspace pending marker: %w", err)
		}
		if err := os.Rename(markerPath, pendingPath); err != nil {
			return fmt.Errorf("stage durable workspace cleanup marker: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect durable workspace marker: %w", err)
	}
	// Make the committed-marker retirement durable before deleting workspace
	// data. The pending binding or an authorized transition record covers a
	// crash after this barrier, while a resurrected committed marker beside a
	// deleted tree cannot.
	if err := syncRoot(durableRoot); err != nil {
		return fmt.Errorf("sync durable workspace cleanup marker: %w", err)
	}
	workspaceDir := filepath.Join(durableRoot, "ws-"+sessionUID)
	// The tree may still carry the previous session child's ownership and
	// 0700 modes; the supervisor holds CHOWN but not DAC_OVERRIDE, so it must
	// reclaim the tree before it can traverse and remove it.
	if err := reclaimDurableWorkspaceTree(workspaceDir); err != nil {
		return err
	}
	if err := os.RemoveAll(workspaceDir); err != nil {
		return fmt.Errorf("remove durable workspace tree: %w", err)
	}
	if err := syncRoot(durableRoot); err != nil {
		return fmt.Errorf("sync durable workspace tree removal: %w", err)
	}
	if err := os.Remove(pendingPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retire durable workspace cleanup marker: %w", err)
	}
	if err := syncRoot(durableRoot); err != nil {
		return fmt.Errorf("sync durable workspace cleanup retirement: %w", err)
	}
	return nil
}

// reclaimDurableWorkspaceTree returns a durable workspace tree that may still
// be owned by a prior session child to the supervisor identity, tolerating an
// absent tree.
func reclaimDurableWorkspaceTree(path string) error {
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspect durable workspace tree: %w", err)
	}
	return ReclaimSessionOwnership(path)
}

func durableWorkspaceMarkerPath(durableRoot, sessionUID string) string {
	return filepath.Join(durableRoot, "ws-"+sessionUID+".binding.json")
}

func durableWorkspacePendingMarkerPath(durableRoot, sessionUID string) string {
	return filepath.Join(durableRoot, "ws-"+sessionUID+".binding.pending.json")
}

func durableWorkspaceTransitionMarkerPath(durableRoot, sessionUID string) string {
	return filepath.Join(durableRoot, "ws-"+sessionUID+".transition.json")
}

// MarkDurableWorkspaceTransitionAuthorized durably records, BEFORE the old
// checkpoint is wiped, that the controller authorized a repository-identity
// transition to the given target. A creation retry that finds no committed
// marker but a matching transition record may materialize fresh instead of
// failing closed - without it, a transient failure after the wipe would
// strand the lineage permanently. The record is retired by the next commit.
func MarkDurableWorkspaceTransitionAuthorized(durableRoot, sessionUID string, target DurableWorkspaceBinding) error {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) || !IsValidSessionPathComponent(sessionUID) {
		return fmt.Errorf("absolute durable root and a valid session component are required")
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("encode durable workspace transition record: %w", err)
	}
	staged, err := os.CreateTemp(durableRoot, ".transition-*")
	if err != nil {
		return fmt.Errorf("stage durable workspace transition record: %w", err)
	}
	stagedName := staged.Name()
	defer os.Remove(stagedName) //nolint:errcheck // best-effort removal of the staged record
	if _, err := staged.Write(encoded); err != nil {
		_ = staged.Close()
		return fmt.Errorf("write durable workspace transition record: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return fmt.Errorf("sync durable workspace transition record: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close durable workspace transition record: %w", err)
	}
	if err := os.Rename(stagedName, durableWorkspaceTransitionMarkerPath(durableRoot, sessionUID)); err != nil {
		return fmt.Errorf("commit durable workspace transition record: %w", err)
	}
	if err := syncDurableWorkspaceRoot(durableRoot); err != nil {
		return fmt.Errorf("sync durable workspace transition record: %w", err)
	}
	return nil
}

func syncDurableWorkspaceRoot(durableRoot string) error {
	directory, err := os.Open(durableRoot)
	if err != nil {
		return fmt.Errorf("open durable workspace root: %w", err)
	}
	defer directory.Close() //nolint:errcheck
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync durable workspace root: %w", err)
	}
	return nil
}

// DurableWorkspaceTransitionTarget reads a previously authorized transition
// record, or nil when none exists.
func DurableWorkspaceTransitionTarget(durableRoot, sessionUID string) (*DurableWorkspaceBinding, error) {
	durableRoot = filepath.Clean(strings.TrimSpace(durableRoot))
	if durableRoot == "." || !filepath.IsAbs(durableRoot) || !IsValidSessionPathComponent(sessionUID) {
		return nil, fmt.Errorf("absolute durable root and a valid session component are required")
	}
	raw, err := os.ReadFile(durableWorkspaceTransitionMarkerPath(durableRoot, sessionUID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read durable workspace transition record: %w", err)
	}
	target := &DurableWorkspaceBinding{}
	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("durable workspace transition record is unreadable: %w", err)
	}
	return target, nil
}

// FinalizeSessionOwnership assigns the prepared session tree to its unique child
// identity without following symlinks. It must run before the provider process is
// started and after the trusted workspace artifact has been materialized.
func FinalizeSessionOwnership(root string, uid, gid int) error {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || !filepath.IsAbs(root) || uid <= 0 || gid <= 0 {
		return fmt.Errorf("absolute session root and non-root UID/GID are required")
	}
	if os.Geteuid() != 0 {
		if uid == os.Getuid() && gid == os.Getgid() {
			return nil
		}
		return fmt.Errorf("non-root supervisor cannot assign session ownership")
	}
	entries, err := sessionOwnershipEntries(root)
	if err != nil {
		return err
	}
	return assignSessionOwnership(entries, uid, gid, os.Chmod, os.Lchown)
}

// ReclaimSessionOwnership returns a frozen session tree to the supervisor so it
// can validate workspace state without requiring DAC_OVERRIDE. Parent
// directories are reclaimed before WalkDir descends into their children.
func ReclaimSessionOwnership(root string) error {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || !filepath.IsAbs(root) {
		return fmt.Errorf("absolute session root is required")
	}
	if os.Geteuid() != 0 {
		return nil
	}
	return reclaimSessionOwnership(root, os.Geteuid(), os.Getegid(), os.Lchown, os.Chmod)
}

func reclaimSessionOwnership(
	root string,
	uid, gid int,
	lchown func(string, int, int) error,
	chmod func(string, os.FileMode) error,
) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := lchown(path, uid, gid); err != nil {
			return fmt.Errorf("reclaim session path: %w", err)
		}
		if entry.IsDir() {
			if err := chmod(path, 0o700); err != nil {
				return fmt.Errorf("chmod reclaimed session directory: %w", err)
			}
		}
		return nil
	})
}

type sessionOwnershipEntry struct {
	path      string
	directory bool
}

func sessionOwnershipEntries(root string) ([]sessionOwnershipEntry, error) {
	entries := make([]sessionOwnershipEntry, 0, 16)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries = append(entries, sessionOwnershipEntry{path: path, directory: entry.IsDir()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inspect session tree: %w", err)
	}
	for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
		entries[left], entries[right] = entries[right], entries[left]
	}
	return entries, nil
}

func assignSessionOwnership(
	entries []sessionOwnershipEntry,
	uid, gid int,
	chmod func(string, os.FileMode) error,
	lchown func(string, int, int) error,
) error {
	for _, entry := range entries {
		if entry.directory {
			if err := chmod(entry.path, 0o700); err != nil {
				return fmt.Errorf("chmod session directory: %w", err)
			}
		}
		if err := lchown(entry.path, uid, gid); err != nil {
			return fmt.Errorf("chown session path: %w", err)
		}
	}
	return nil
}
