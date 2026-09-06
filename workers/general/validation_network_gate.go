/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	validationNetworkProbeInterval = time.Second
	validationNetworkReadGate      = os.ReadFile
	validationNetworkDial          = func(ctx context.Context, address string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: time.Second}
		return dialer.DialContext(ctx, "tcp", address)
	}
	validationNetworkExecutable     = os.Executable
	validationNetworkInstallSandbox = installValidationNetworkSandboxBinary
)

// waitForValidationNetworkAccess establishes that the repository endpoint is
// reachable from the Pod before the controller creates the deny-all policy.
func waitForValidationNetworkAccess(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("validation network access probe requires a probe address")
	}
	probeAddress := strings.TrimSpace(args[0])
	if probeAddress == "" {
		return fmt.Errorf("validation network access probe address must not be empty")
	}

	for {
		connection, dialErr := validationNetworkDial(ctx, probeAddress)
		if connection != nil {
			_ = connection.Close()
		}
		if dialErr == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := waitForValidationNetworkProbe(ctx); err != nil {
			return err
		}
	}
}

// waitForValidationNetworkPolicy blocks the application container until the
// controller has created its deny-all NetworkPolicy. The installed seccomp
// wrapper is the absolute offline boundary; the NetworkPolicy remains a second
// layer and may overlap with additive namespace policies.
func waitForValidationNetworkPolicy(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("validation network gate requires a gate file and sandbox destination")
	}
	gateFile := strings.TrimSpace(args[0])
	sandboxDestination := strings.TrimSpace(args[1])
	if gateFile == "" || sandboxDestination == "" {
		return fmt.Errorf("validation network gate inputs must not be empty")
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := validationNetworkReadGate(gateFile)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("read validation network gate: %w", err)
		}
		if strings.TrimSpace(string(state)) != "true" {
			if err := waitForValidationNetworkProbe(ctx); err != nil {
				return err
			}
			continue
		}
		return validationNetworkInstallSandbox(sandboxDestination)
	}
}

func installValidationNetworkSandboxBinary(destination string) error {
	source, err := validationNetworkExecutable()
	if err != nil {
		return fmt.Errorf("resolve validation network sandbox executable: %w", err)
	}
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open validation network sandbox executable: %w", err)
	}
	defer func() { _ = input.Close() }()

	destinationDir := filepath.Dir(destination)
	temporary, err := os.CreateTemp(destinationDir, ".validation-network-sandbox-*")
	if err != nil {
		return fmt.Errorf("create validation network sandbox executable: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()

	if _, err := io.Copy(temporary, input); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("copy validation network sandbox executable: %w", err)
	}
	if err := temporary.Chmod(0o555); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("make validation network sandbox executable: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close validation network sandbox executable: %w", err)
	}
	if err := os.Rename(temporaryName, destination); err != nil {
		return fmt.Errorf("install validation network sandbox executable: %w", err)
	}
	return nil
}

func waitForValidationNetworkProbe(ctx context.Context) error {
	timer := time.NewTimer(validationNetworkProbeInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
