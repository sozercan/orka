/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

var (
	validationProcessLimitInstall   = installValidationProcessLimit
	validationNetworkSandboxInstall = installValidationNetworkSandbox
	validationNetworkSandboxExec    = execValidationNetworkSandbox
	errValidationCommandUnavailable = errors.New("validation command unavailable")
)

func runValidationNetworkSandbox(command []string) error {
	if len(command) == 0 || command[0] == "" {
		return fmt.Errorf("%w: validation network sandbox requires a command", errValidationCommandUnavailable)
	}

	// Seccomp filters apply to the calling thread. Keep this goroutine on the
	// filtered thread until exec replaces the process; descendants inherit the
	// filter and can create only local Unix-domain sockets.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := validationProcessLimitInstall(); err != nil {
		return fmt.Errorf("%w: install validation process limit: %w", errValidationCommandUnavailable, err)
	}
	if err := validationNetworkSandboxInstall(); err != nil {
		return fmt.Errorf("%w: install validation network sandbox: %w", errValidationCommandUnavailable, err)
	}
	if err := validationNetworkSandboxExec(command[0], command, os.Environ()); err != nil {
		return fmt.Errorf("%w: execute validation command in network sandbox: %w", errValidationCommandUnavailable, err)
	}
	return nil
}
