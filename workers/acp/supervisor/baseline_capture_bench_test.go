package supervisor

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/orka-agents/orka/internal/workspacedelta"
)

// BenchmarkBaselineCapture measures the trusted baseline capture — walk,
// hashing, and the secret-like content flagger and fingerprinter — over a
// staged copy of the regular files under ORKA_SECRET_SCAN_CORPUS_DIR (a
// repository checkout), as session creation runs it.
func BenchmarkBaselineCapture(b *testing.B) {
	source := os.Getenv("ORKA_SECRET_SCAN_CORPUS_DIR")
	if source == "" {
		b.Skip("ORKA_SECRET_SCAN_CORPUS_DIR unset")
	}
	root := b.TempDir()
	var files int
	var bytes int64
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "bin":
				if relative != "." {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		files++
		bytes += int64(len(content))
		return os.WriteFile(target, content, 0o644)
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("staged %d files, %d bytes", files, bytes)
	for name, options := range map[string]workspacedelta.Options{
		"WalkOnly":      {},
		"ContentPolicy": (&Server{}).baselineCaptureOptions(),
	} {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(bytes)
			for i := 0; i < b.N; i++ {
				if _, err := workspacedelta.Capture(root, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
