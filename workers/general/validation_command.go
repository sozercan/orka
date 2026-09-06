/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/workerenv"
)

const validationCommandDigestPrefix = "sha256:"

func materializeValidationCommand(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf(
			"%w: command materializer requires source, destination, and digest",
			errValidationCommandUnavailable,
		)
	}
	sourcePath, destinationPath, expectedDigest := args[0], args[1], strings.TrimSpace(args[2])
	expectedDigestLength := len(validationCommandDigestPrefix) + sha256.Size*2
	if sourcePath == "" || destinationPath == "" || len(expectedDigest) != expectedDigestLength ||
		!strings.HasPrefix(expectedDigest, validationCommandDigestPrefix) {
		return fmt.Errorf("%w: command materializer arguments are invalid", errValidationCommandUnavailable)
	}
	expected, err := hex.DecodeString(strings.TrimPrefix(expectedDigest, validationCommandDigestPrefix))
	if err != nil || len(expected) != sha256.Size {
		return fmt.Errorf("%w: command digest is invalid", errValidationCommandUnavailable)
	}

	command, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("%w: read validation command: %w", errValidationCommandUnavailable, err)
	}
	commandText := string(command)
	if len(command) == 0 || len(command) > workerenv.RepositoryValidationMaxCommandBytes ||
		!utf8.Valid(command) || strings.IndexByte(commandText, 0) >= 0 || commandText != strings.TrimSpace(commandText) {
		return fmt.Errorf("%w: validation command content is invalid", errValidationCommandUnavailable)
	}
	digest := sha256.Sum256(command)
	if subtle.ConstantTimeCompare(digest[:], expected) != 1 {
		return fmt.Errorf("%w: validation command digest does not match", errValidationCommandUnavailable)
	}

	destination, err := os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return fmt.Errorf("%w: create verified validation command: %w", errValidationCommandUnavailable, err)
	}
	removeDestination := true
	defer func() {
		if removeDestination {
			_ = os.Remove(destinationPath)
		}
	}()
	if _, err := destination.Write(command); err != nil {
		_ = destination.Close()
		return fmt.Errorf("%w: write verified validation command: %w", errValidationCommandUnavailable, err)
	}
	if err := destination.Close(); err != nil {
		return fmt.Errorf("%w: close verified validation command: %w", errValidationCommandUnavailable, err)
	}
	removeDestination = false
	return nil
}
