//go:build !linux

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"fmt"
	"runtime"
)

func installValidationProcessLimit() error {
	return fmt.Errorf("validation process limit requires Linux, current OS is %s", runtime.GOOS)
}

func installValidationNetworkSandbox() error {
	return fmt.Errorf("validation network sandbox requires Linux, current OS is %s", runtime.GOOS)
}

func execValidationNetworkSandbox(string, []string, []string) error {
	return fmt.Errorf("validation network sandbox requires Linux, current OS is %s", runtime.GOOS)
}
