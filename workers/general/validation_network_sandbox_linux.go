//go:build linux

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/orka-agents/orka/internal/workerenv"
	"golang.org/x/sys/unix"
)

const (
	seccompDataSyscallOffset = 0
	seccompDataArchOffset    = 4
	seccompDataArg0Offset    = 16
	x32SyscallBit            = 0x40000000
)

func installValidationProcessLimit() error {
	limit := uint64(workerenv.RepositoryValidationMaxProcesses)
	if err := unix.Setrlimit(unix.RLIMIT_NPROC, &unix.Rlimit{Cur: limit, Max: limit}); err != nil {
		return fmt.Errorf("set max processes to %d: %w", limit, err)
	}
	return nil
}

func installValidationNetworkSandbox() error {
	auditArch, err := validationNetworkSandboxAuditArch()
	if err != nil {
		return err
	}
	deny := uint32(unix.SECCOMP_RET_ERRNO) | uint32(unix.ENETUNREACH)
	filters := []unix.SockFilter{
		validationSandboxStatement(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, seccompDataArchOffset),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, auditArch, 1, 0),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_KILL_PROCESS),
		validationSandboxStatement(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, seccompDataSyscallOffset),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JSET|unix.BPF_K, x32SyscallBit, 0, 1),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_KILL_PROCESS),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, uint32(unix.SYS_SOCKET), 0, 3),
		validationSandboxStatement(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, seccompDataArg0Offset),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, unix.AF_UNIX, 1, 0),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, deny),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, uint32(unix.SYS_SOCKETPAIR), 0, 3),
		validationSandboxStatement(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, seccompDataArg0Offset),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, unix.AF_UNIX, 1, 0),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, deny),
		validationSandboxJump(unix.BPF_JMP|unix.BPF_JEQ|unix.BPF_K, uint32(unix.SYS_IO_URING_SETUP), 0, 1),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, deny),
		validationSandboxStatement(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ALLOW),
	}
	program := unix.SockFprog{Len: uint16(len(filters)), Filter: &filters[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("set no-new-privileges: %w", err)
	}
	if err := unix.Prctl(
		unix.PR_SET_SECCOMP,
		unix.SECCOMP_MODE_FILTER,
		uintptr(unsafe.Pointer(&program)),
		0,
		0,
	); err != nil {
		return fmt.Errorf("install seccomp network filter: %w", err)
	}
	return nil
}

func execValidationNetworkSandbox(path string, args, env []string) error {
	return unix.Exec(path, args, env)
}

func validationNetworkSandboxAuditArch() (uint32, error) {
	switch runtime.GOARCH {
	case "amd64":
		return unix.AUDIT_ARCH_X86_64, nil
	case "arm64":
		return unix.AUDIT_ARCH_AARCH64, nil
	default:
		return 0, fmt.Errorf("validation network sandbox does not support Linux architecture %s", runtime.GOARCH)
	}
}

func validationSandboxStatement(code uint16, value uint32) unix.SockFilter {
	return unix.SockFilter{Code: code, K: value}
}

func validationSandboxJump(code uint16, value uint32, onTrue, onFalse uint8) unix.SockFilter {
	return unix.SockFilter{Code: code, Jt: onTrue, Jf: onFalse, K: value}
}
