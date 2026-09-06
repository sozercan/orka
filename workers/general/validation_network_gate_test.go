/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWaitForValidationNetworkAccessRequiresSuccessfulProbe(t *testing.T) {
	originalDial := validationNetworkDial
	originalInterval := validationNetworkProbeInterval
	t.Cleanup(func() {
		validationNetworkDial = originalDial
		validationNetworkProbeInterval = originalInterval
	})

	validationNetworkProbeInterval = time.Millisecond
	calls := 0
	validationNetworkDial = func(context.Context, string) (net.Conn, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("unreachable")
		}
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	if err := waitForValidationNetworkAccess(context.Background(), []string{"github.com:443"}); err != nil {
		t.Fatalf("waitForValidationNetworkAccess() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("probe calls = %d, want 3", calls)
	}
}

func TestWaitForValidationNetworkPolicyWaitsForControllerGate(t *testing.T) {
	originalRead := validationNetworkReadGate
	originalInterval := validationNetworkProbeInterval
	t.Cleanup(func() {
		validationNetworkReadGate = originalRead
		validationNetworkProbeInterval = originalInterval
	})

	reads := 0
	validationNetworkReadGate = func(string) ([]byte, error) {
		reads++
		if reads < 3 {
			return []byte("false"), nil
		}
		return []byte("true"), nil
	}
	validationNetworkProbeInterval = time.Millisecond
	installed := ""
	stubValidationNetworkSandboxInstall(t, func(destination string) error {
		installed = destination
		return nil
	})

	err := waitForValidationNetworkPolicy(
		context.Background(),
		[]string{"/gate/ready", "/sandbox/worker"},
	)
	if err != nil {
		t.Fatalf("waitForValidationNetworkPolicy() error = %v", err)
	}
	if reads != 3 || installed != "/sandbox/worker" {
		t.Fatalf("gate reads/installed destination = %d/%q, want 3 and sandbox path", reads, installed)
	}
}

func TestWaitForValidationNetworkPolicyFailsClosedWhenSandboxInstallFails(t *testing.T) {
	originalRead := validationNetworkReadGate
	originalInterval := validationNetworkProbeInterval
	t.Cleanup(func() {
		validationNetworkReadGate = originalRead
		validationNetworkProbeInterval = originalInterval
	})

	validationNetworkReadGate = func(string) ([]byte, error) { return []byte("true"), nil }
	validationNetworkProbeInterval = time.Millisecond
	installErr := errors.New("network sandbox unavailable")
	stubValidationNetworkSandboxInstall(t, func(string) error { return installErr })

	err := waitForValidationNetworkPolicy(
		context.Background(),
		[]string{"/gate/ready", "/sandbox/worker"},
	)
	if !errors.Is(err, installErr) {
		t.Fatalf("waitForValidationNetworkPolicy() error = %v, want sandbox install failure", err)
	}
}

func TestInstallValidationNetworkSandboxBinaryCopiesExecutable(t *testing.T) {
	originalExecutable := validationNetworkExecutable
	t.Cleanup(func() { validationNetworkExecutable = originalExecutable })

	source := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(source, []byte("sandbox-binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	validationNetworkExecutable = func() (string, error) { return source, nil }
	destination := filepath.Join(t.TempDir(), "sandbox", "worker")
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := installValidationNetworkSandboxBinary(destination); err != nil {
		t.Fatalf("installValidationNetworkSandboxBinary() error = %v", err)
	}
	contents, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "sandbox-binary" || info.Mode().Perm() != 0o555 {
		t.Fatalf("installed sandbox = %q mode %#o, want copied executable mode 0555", contents, info.Mode().Perm())
	}
}

func stubValidationNetworkSandboxInstall(t *testing.T, install func(string) error) {
	t.Helper()
	original := validationNetworkInstallSandbox
	validationNetworkInstallSandbox = install
	t.Cleanup(func() { validationNetworkInstallSandbox = original })
}
