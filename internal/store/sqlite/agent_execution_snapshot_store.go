/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

// AgentExecutionSnapshotCipher encrypts immutable execution snapshot bodies at
// rest. Snapshot bodies are sensitive (resolved prompts, Skill content,
// repository identities, endpoint metadata, policy configuration) and are
// never stored or returned in plaintext outside this seam.
type AgentExecutionSnapshotCipher struct {
	aead cipher.AEAD
}

// AgentExecutionSnapshotKeyBytes is the required AES-256 key length.
const AgentExecutionSnapshotKeyBytes = 32

// NewAgentExecutionSnapshotCipher builds an AES-256-GCM snapshot cipher.
func NewAgentExecutionSnapshotCipher(key []byte) (*AgentExecutionSnapshotCipher, error) {
	if len(key) != AgentExecutionSnapshotKeyBytes {
		return nil, fmt.Errorf("agent execution snapshot key must be exactly %d bytes", AgentExecutionSnapshotKeyBytes)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("initialize snapshot cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("initialize snapshot AEAD: %w", err)
	}
	return &AgentExecutionSnapshotCipher{aead: aead}, nil
}

func snapshotAdditionalData(taskUID, digest string, schemaVersion int32) []byte {
	return fmt.Appendf(nil, "orka.agent-execution-snapshot\x00%s\x00%s\x00%d", taskUID, digest, schemaVersion)
}

func (c *AgentExecutionSnapshotCipher) seal(taskUID, digest string, schemaVersion int32, body []byte) (nonce, ciphertext []byte, err error) {
	nonce = make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("generate snapshot nonce: %w", err)
	}
	ciphertext = c.aead.Seal(nil, nonce, body, snapshotAdditionalData(taskUID, digest, schemaVersion))
	return nonce, ciphertext, nil
}

func (c *AgentExecutionSnapshotCipher) open(taskUID, digest string, schemaVersion int32, nonce, ciphertext []byte) ([]byte, error) {
	body, err := c.aead.Open(nil, nonce, ciphertext, snapshotAdditionalData(taskUID, digest, schemaVersion))
	if err != nil {
		return nil, fmt.Errorf("decrypt agent execution snapshot %s/%s: %w", taskUID, digest, err)
	}
	return body, nil
}

// SetAgentExecutionSnapshotCipher installs the required at-rest encryption
// cipher after proving that it can authenticate every retained snapshot.
// Callers must configure the cipher before snapshot readers or writers start.
// This rejects key rotation while any snapshot encrypted by the previous key
// remains, preventing one database from silently accumulating mixed-key rows.
func (s *Store) SetAgentExecutionSnapshotCipher(snapshotCipher *AgentExecutionSnapshotCipher) error {
	if snapshotCipher == nil {
		return errors.New("agent execution snapshot cipher is required")
	}
	rows, err := s.db.Query(`SELECT task_uid, digest, schema_version, nonce, ciphertext
		FROM agent_execution_snapshots ORDER BY task_uid ASC, digest ASC`)
	if err != nil {
		return fmt.Errorf("verify agent execution snapshot key: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			taskUID       string
			digest        string
			schemaVersion int32
			nonce         []byte
			ciphertext    []byte
		)
		if err := rows.Scan(&taskUID, &digest, &schemaVersion, &nonce, &ciphertext); err != nil {
			return fmt.Errorf("scan retained agent execution snapshot while verifying key: %w", err)
		}
		body, err := snapshotCipher.open(taskUID, digest, schemaVersion, nonce, ciphertext)
		if err != nil {
			return fmt.Errorf(
				"candidate agent execution snapshot key cannot authenticate retained snapshot %s/%s; restore the previous key or wait for reference-aware retention before rotating: %w",
				taskUID, digest, err,
			)
		}
		if computed := store.CanonicalAgentExecutionSnapshotDigest(body); computed != digest {
			return fmt.Errorf("retained agent execution snapshot %s/%s failed integrity verification while activating key", taskUID, digest)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate retained agent execution snapshots while verifying key: %w", err)
	}
	s.snapshotCipher = snapshotCipher
	return nil
}

var errSnapshotCipherRequired = errors.New("agent execution snapshot encryption is not configured; snapshot persistence fails closed")

// PersistAgentExecutionSnapshot implements store.AgentExecutionSnapshotStore.
func (s *Store) PersistAgentExecutionSnapshot(ctx context.Context, snapshot store.AgentExecutionSnapshot) error {
	if s.snapshotCipher == nil {
		return errSnapshotCipherRequired
	}
	key := store.AgentExecutionSnapshotKey{TaskUID: snapshot.TaskUID, Digest: snapshot.Digest}
	if err := key.Validate(); err != nil {
		return err
	}
	if snapshot.SchemaVersion < 1 {
		return store.ValidationErrorf("snapshot schema version must be positive")
	}
	if len(snapshot.Body) == 0 {
		return store.ValidationErrorf("snapshot body is required")
	}
	if computed := store.CanonicalAgentExecutionSnapshotDigest(snapshot.Body); computed != snapshot.Digest {
		return store.ValidationErrorf("snapshot digest %s does not match body digest %s", snapshot.Digest, computed)
	}

	existing, err := s.GetAgentExecutionSnapshot(ctx, key)
	switch {
	case err == nil:
		if existing.SchemaVersion == snapshot.SchemaVersion && subtle.ConstantTimeCompare(existing.Body, snapshot.Body) == 1 {
			return nil
		}
		return fmt.Errorf("%w: agent execution snapshot %s already exists with different content", store.ErrDuplicateMismatch, key.ID())
	case errors.Is(err, store.ErrNotFound):
	default:
		return err
	}

	nonce, ciphertext, err := s.snapshotCipher.seal(snapshot.TaskUID, snapshot.Digest, snapshot.SchemaVersion, snapshot.Body)
	if err != nil {
		return err
	}
	createdAt := snapshot.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO agent_execution_snapshots
		(task_uid, digest, schema_version, nonce, ciphertext, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_uid, digest) DO NOTHING`,
		snapshot.TaskUID, snapshot.Digest, snapshot.SchemaVersion, nonce, ciphertext, createdAt)
	if err != nil {
		return fmt.Errorf("persist agent execution snapshot: %w", err)
	}
	return nil
}

// GetAgentExecutionSnapshot implements store.AgentExecutionSnapshotStore.
func (s *Store) GetAgentExecutionSnapshot(ctx context.Context, key store.AgentExecutionSnapshotKey) (*store.AgentExecutionSnapshot, error) {
	if s.snapshotCipher == nil {
		return nil, errSnapshotCipherRequired
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `SELECT schema_version, nonce, ciphertext, created_at
		FROM agent_execution_snapshots WHERE task_uid = ? AND digest = ?`, key.TaskUID, key.Digest)
	var (
		schemaVersion int32
		nonce         []byte
		ciphertext    []byte
		createdAt     time.Time
	)
	if err := row.Scan(&schemaVersion, &nonce, &ciphertext, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get agent execution snapshot: %w", err)
	}
	body, err := s.snapshotCipher.open(key.TaskUID, key.Digest, schemaVersion, nonce, ciphertext)
	if err != nil {
		return nil, err
	}
	if computed := store.CanonicalAgentExecutionSnapshotDigest(body); computed != key.Digest {
		return nil, fmt.Errorf("agent execution snapshot %s failed integrity verification", key.ID())
	}
	return &store.AgentExecutionSnapshot{
		TaskUID:       key.TaskUID,
		Digest:        key.Digest,
		SchemaVersion: schemaVersion,
		Body:          body,
		CreatedAt:     createdAt.UTC(),
	}, nil
}

// ListAgentExecutionSnapshotMetadataBefore implements
// store.AgentExecutionSnapshotLifecycleStore. The cutoff is strict so a
// retention pass can use one stable timestamp without collecting records
// created exactly at that boundary.
func (s *Store) ListAgentExecutionSnapshotMetadataBefore(
	ctx context.Context,
	cutoff time.Time,
) ([]store.AgentExecutionSnapshotMetadata, error) {
	if cutoff.IsZero() {
		return nil, store.ValidationErrorf("snapshot metadata cutoff is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT task_uid, digest, schema_version, created_at
		FROM agent_execution_snapshots
		WHERE created_at < ?
		ORDER BY created_at ASC, task_uid ASC, digest ASC`, cutoff.UTC())
	if err != nil {
		return nil, fmt.Errorf("list agent execution snapshot metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	metadata := make([]store.AgentExecutionSnapshotMetadata, 0)
	for rows.Next() {
		var item store.AgentExecutionSnapshotMetadata
		if err := rows.Scan(&item.Key.TaskUID, &item.Key.Digest, &item.SchemaVersion, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan agent execution snapshot metadata: %w", err)
		}
		item.CreatedAt = item.CreatedAt.UTC()
		if err := item.Validate(); err != nil {
			return nil, fmt.Errorf("validate stored agent execution snapshot metadata: %w", err)
		}
		metadata = append(metadata, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent execution snapshot metadata: %w", err)
	}
	return metadata, nil
}

// CountAgentExecutionSnapshotReferences implements
// store.AgentExecutionSnapshotLifecycleStore. All counts are read by one SQL
// statement so the result is one consistent SQLite snapshot.
func (s *Store) CountAgentExecutionSnapshotReferences(
	ctx context.Context,
	key store.AgentExecutionSnapshotKey,
) (store.AgentExecutionSnapshotReferenceCounts, error) {
	if err := key.Validate(); err != nil {
		return store.AgentExecutionSnapshotReferenceCounts{}, err
	}
	var counts store.AgentExecutionSnapshotReferenceCounts
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM harness_v1_attempts
			WHERE task_uid = ? AND snapshot_digest = ?),
		(SELECT COUNT(*) FROM prompt_attempts
			WHERE task_uid = ? AND snapshot_digest = ?),
		(SELECT COUNT(*) FROM session_turns
			WHERE task_uid = ?)`,
		key.TaskUID, key.Digest,
		key.TaskUID, key.Digest,
		key.TaskUID,
	).Scan(
		&counts.HarnessV1Attempts,
		&counts.PromptAttempts,
		&counts.SessionTurns,
	)
	if err != nil {
		return store.AgentExecutionSnapshotReferenceCounts{}, fmt.Errorf("count agent execution snapshot references: %w", err)
	}
	return counts, nil
}

// DeleteAgentExecutionSnapshot implements
// store.AgentExecutionSnapshotLifecycleStore. It deliberately does not alter
// the Task-scoped broad deletion method above or perform lifecycle policy.
func (s *Store) DeleteAgentExecutionSnapshot(ctx context.Context, key store.AgentExecutionSnapshotKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM agent_execution_snapshots
		WHERE task_uid = ? AND digest = ?`, key.TaskUID, key.Digest); err != nil {
		return fmt.Errorf("delete agent execution snapshot: %w", err)
	}
	return nil
}

// DeleteAgentExecutionSnapshots implements store.AgentExecutionSnapshotStore.
func (s *Store) DeleteAgentExecutionSnapshots(ctx context.Context, taskUID string) error {
	if strings.TrimSpace(taskUID) == "" {
		return store.ValidationErrorf("snapshot task UID is required")
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM agent_execution_snapshots WHERE task_uid = ?`, taskUID); err != nil {
		return fmt.Errorf("delete agent execution snapshots: %w", err)
	}
	return nil
}
