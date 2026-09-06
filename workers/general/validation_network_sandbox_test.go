/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"errors"
	"slices"
	"testing"

	"github.com/orka-agents/orka/internal/workerenv"
)

func TestRunValidationNetworkSandboxInstallsBeforeExec(t *testing.T) {
	originalProcessLimit := validationProcessLimitInstall
	originalInstall := validationNetworkSandboxInstall
	originalExec := validationNetworkSandboxExec
	t.Cleanup(func() {
		validationProcessLimitInstall = originalProcessLimit
		validationNetworkSandboxInstall = originalInstall
		validationNetworkSandboxExec = originalExec
	})

	processLimited := false
	installed := false
	validationProcessLimitInstall = func() error {
		processLimited = true
		return nil
	}
	validationNetworkSandboxInstall = func() error {
		if !processLimited {
			t.Fatal("network sandbox installed before the process limit")
		}
		installed = true
		return nil
	}
	wantCommand := []string{"/bin/sh", "-c", "go test ./..."}
	validationNetworkSandboxExec = func(path string, args, _ []string) error {
		if !installed {
			t.Fatal("validation command executed before the network sandbox was installed")
		}
		if path != wantCommand[0] || !slices.Equal(args, wantCommand) {
			t.Fatalf("exec path/args = %q/%#v, want %q/%#v", path, args, wantCommand[0], wantCommand)
		}
		return nil
	}

	if err := runValidationNetworkSandbox(wantCommand); err != nil {
		t.Fatalf("runValidationNetworkSandbox() error = %v", err)
	}
}

func TestRunValidationNetworkSandboxFailsClosed(t *testing.T) {
	originalProcessLimit := validationProcessLimitInstall
	originalInstall := validationNetworkSandboxInstall
	originalExec := validationNetworkSandboxExec
	t.Cleanup(func() {
		validationProcessLimitInstall = originalProcessLimit
		validationNetworkSandboxInstall = originalInstall
		validationNetworkSandboxExec = originalExec
	})

	validationProcessLimitInstall = func() error { return nil }
	installErr := errors.New("seccomp unavailable")
	validationNetworkSandboxInstall = func() error { return installErr }
	validationNetworkSandboxExec = func(string, []string, []string) error {
		t.Fatal("validation command executed after sandbox installation failed")
		return nil
	}

	err := runValidationNetworkSandbox([]string{"/bin/true"})
	if !errors.Is(err, installErr) || !errors.Is(err, errValidationCommandUnavailable) {
		t.Fatalf("runValidationNetworkSandbox() error = %v, want sandbox installation failure", err)
	}
	if got := generalWorkerExitCode(err); got != workerenv.RepositoryValidationUnavailableExitCode {
		t.Fatalf("generalWorkerExitCode() = %d, want %d", got, workerenv.RepositoryValidationUnavailableExitCode)
	}
}

func TestRunValidationNetworkSandboxExecFailureIsUnavailable(t *testing.T) {
	originalProcessLimit := validationProcessLimitInstall
	originalInstall := validationNetworkSandboxInstall
	originalExec := validationNetworkSandboxExec
	t.Cleanup(func() {
		validationProcessLimitInstall = originalProcessLimit
		validationNetworkSandboxInstall = originalInstall
		validationNetworkSandboxExec = originalExec
	})

	validationProcessLimitInstall = func() error { return nil }
	execErr := errors.New("shell missing")
	validationNetworkSandboxInstall = func() error { return nil }
	validationNetworkSandboxExec = func(string, []string, []string) error { return execErr }

	err := runValidationNetworkSandbox([]string{"/bin/sh"})
	if !errors.Is(err, execErr) || !errors.Is(err, errValidationCommandUnavailable) {
		t.Fatalf("runValidationNetworkSandbox() error = %v, want command exec failure", err)
	}
}

func TestRunValidationNetworkSandboxFailsClosedWhenProcessLimitCannotBeInstalled(t *testing.T) {
	originalProcessLimit := validationProcessLimitInstall
	originalInstall := validationNetworkSandboxInstall
	originalExec := validationNetworkSandboxExec
	t.Cleanup(func() {
		validationProcessLimitInstall = originalProcessLimit
		validationNetworkSandboxInstall = originalInstall
		validationNetworkSandboxExec = originalExec
	})

	limitErr := errors.New("process limit unavailable")
	validationProcessLimitInstall = func() error { return limitErr }
	validationNetworkSandboxInstall = func() error {
		t.Fatal("network sandbox installed after process-limit failure")
		return nil
	}
	validationNetworkSandboxExec = func(string, []string, []string) error {
		t.Fatal("validation command executed after process-limit failure")
		return nil
	}

	err := runValidationNetworkSandbox([]string{"/bin/true"})
	if !errors.Is(err, limitErr) || !errors.Is(err, errValidationCommandUnavailable) {
		t.Fatalf("runValidationNetworkSandbox() error = %v, want process-limit failure", err)
	}
}
