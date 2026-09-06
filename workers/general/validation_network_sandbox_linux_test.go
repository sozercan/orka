//go:build linux

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

const validationNetworkSandboxSocketProbeEnv = "ORKA_VALIDATION_SOCKET_PROBE"

func TestValidationNetworkSandboxAllowsOnlyUnixSockets(t *testing.T) {
	if os.Getenv(validationNetworkSandboxSocketProbeEnv) == "1" {
		runtime.LockOSThread()
		if err := runValidationNetworkSandboxSocketProbe(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve validation socket sandbox probe executable: %v", err)
	}
	command := exec.Command(executable, "-test.run=^TestValidationNetworkSandboxAllowsOnlyUnixSockets$")
	command.Env = append(os.Environ(), validationNetworkSandboxSocketProbeEnv+"=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("validation socket sandbox probe failed: %v\n%s", err, output)
	}
}

func runValidationNetworkSandboxSocketProbe() error {
	if err := installValidationNetworkSandbox(); err != nil {
		return err
	}
	unixSocket, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("create allowed Unix-domain socket: %w", err)
	}
	if err := unix.Close(unixSocket); err != nil {
		return fmt.Errorf("close Unix-domain socket: %w", err)
	}
	unixSocketPair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("create allowed Unix-domain socket pair: %w", err)
	}
	for _, socket := range unixSocketPair {
		if err := unix.Close(socket); err != nil {
			return fmt.Errorf("close Unix-domain socket pair: %w", err)
		}
	}
	for _, family := range []int{unix.AF_INET, unix.AF_INET6, unix.AF_NETLINK, unix.AF_VSOCK, unix.AF_TIPC} {
		socket, err := unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err == nil {
			_ = unix.Close(socket)
			return fmt.Errorf("network-capable socket family %d was allowed", family)
		}
		if !errors.Is(err, unix.ENETUNREACH) {
			return fmt.Errorf("network-capable socket family %d error = %w, want ENETUNREACH", family, err)
		}
		socketPair, err := unix.Socketpair(family, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err == nil {
			for _, socket := range socketPair {
				_ = unix.Close(socket)
			}
			return fmt.Errorf("network-capable socketpair family %d was allowed", family)
		}
		if !errors.Is(err, unix.ENETUNREACH) {
			return fmt.Errorf("network-capable socketpair family %d error = %w, want ENETUNREACH", family, err)
		}
	}
	return nil
}
