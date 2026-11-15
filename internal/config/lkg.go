package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// lkgFileMode is the permission of the last-known-good file: owner read and
// write only. The config names secret env vars, key files and API-key hashes,
// so other local users have no reason to read it.
const lkgFileMode fs.FileMode = 0o600

// readLKG loads the last-known-good file. A missing file returns (nil, nil):
// that is the normal first start. Anything else that prevents use of the file
// (unreadable, unparseable, invalid, Version <= 0) is an error for the caller
// to log; the file is never deleted or rewritten here.
func readLKG(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cfg, err := ParseJSON(b) // strict decode, Normalize, Validate
	if err != nil {
		return nil, err
	}
	if cfg.Version <= 0 {
		return nil, fmt.Errorf("config: last-known-good file has version %d, want > 0", cfg.Version)
	}
	return cfg, nil
}

// writeLKG replaces the file at path with cfg atomically: a temp file in the
// same directory (rename is only atomic within one file system), fsync, then
// rename. A concurrent reader or a crash therefore sees the old file or the
// new one, never a torn write. On error the temp file is removed.
func writeLKG(path string, cfg *Config) (err error) {
	b, err := json.Marshal(cfg) // read-only use of a delivered, immutable config
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*") // created with mode 0600
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close() // may already be closed; the error is irrelevant then
			_ = os.Remove(tmp.Name())
		}
	}()
	if err = tmp.Chmod(lkgFileMode); err != nil {
		return err
	}
	if _, err = tmp.Write(b); err != nil {
		return err
	}
	// Sync before rename: otherwise a crash can leave the new name pointing
	// at a file whose data never reached the disk.
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	// Best effort: persist the rename itself. Some platforms cannot fsync a
	// directory, and the file content is already safe, so errors are ignored.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
