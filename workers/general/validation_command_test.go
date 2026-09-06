/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orka-agents/orka/internal/workerenv"
)

func validationCommandDigest(command []byte) string {
	digest := sha256.Sum256(command)
	return validationCommandDigestPrefix + hex.EncodeToString(digest[:])
}

func TestMaterializeValidationCommandCopiesDigestMatchedCommand(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	command := []byte("go test ./... && golangci-lint run")
	if err := os.WriteFile(source, command, 0o400); err != nil {
		t.Fatal(err)
	}

	digest := validationCommandDigest(command)
	if err := materializeValidationCommand([]string{source, destination, digest}); err != nil {
		t.Fatalf("materializeValidationCommand() error = %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(command) {
		t.Fatalf("materialized command = %q, want %q", got, command)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Fatalf("materialized command mode = %o, want 400", info.Mode().Perm())
	}
}

func TestMaterializeValidationCommandRejectsReplacedSecret(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	if err := os.WriteFile(source, []byte("exit 0"), 0o400); err != nil {
		t.Fatal(err)
	}

	err := materializeValidationCommand([]string{source, destination, validationCommandDigest([]byte("go test ./..."))})
	if !errors.Is(err, errValidationCommandUnavailable) {
		t.Fatalf("materializeValidationCommand() error = %v, want unavailable", err)
	}
	if got := generalWorkerExitCode(err); got != workerenv.RepositoryValidationUnavailableExitCode {
		t.Fatalf("generalWorkerExitCode() = %d, want %d", got, workerenv.RepositoryValidationUnavailableExitCode)
	}
	if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
		t.Fatalf("destination exists after digest mismatch: %v", statErr)
	}
}

func TestMaterializeValidationCommandDoesNotOverwriteDestination(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	command := []byte("go test ./...")
	if err := os.WriteFile(source, command, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("existing"), 0o400); err != nil {
		t.Fatal(err)
	}

	err := materializeValidationCommand([]string{source, destination, validationCommandDigest(command)})
	if !errors.Is(err, errValidationCommandUnavailable) {
		t.Fatalf("materializeValidationCommand() error = %v, want unavailable", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != "existing" {
		t.Fatalf("existing destination was overwritten with %q", got)
	}
}
