package config

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// FileSource is a ConfigSource backed by a YAML file. It polls and compares
// a content hash rather than using mtime or fsnotify, which also copes with
// Kubernetes ConfigMap symlink swaps.
type FileSource struct {
	path     string
	interval time.Duration
	logger   *slog.Logger

	mu      sync.Mutex
	started bool

	// Only touched by Watch (before the goroutine starts) and then by the
	// single poll goroutine.
	lastHash    [sha256.Size]byte // last content acted on (applied or rejected)
	pendingHash [sha256.Size]byte // new content seen once, awaiting confirmation
	lastVersion int64             // last version delivered
}

// NewFileSource returns a source polling path every pollInterval. A nil
// logger discards logs.
func NewFileSource(path string, pollInterval time.Duration, logger *slog.Logger) *FileSource {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &FileSource{path: path, interval: pollInterval, logger: logger}
}

// load reads and parses the file, returning the config and its content hash.
// The hash is returned even when parsing fails so bad content is logged once.
func (s *FileSource) load() (*Config, [sha256.Size]byte, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("read %s: %w", s.path, err)
	}
	h := sha256.Sum256(data)
	c, err := ParseYAML(data)
	if err != nil {
		return nil, h, fmt.Errorf("parse %s: %w", s.path, err)
	}
	return c, h, nil
}

// errNoRoutes is FileSource's own rule: a gateway with no routes can only be
// the result of a truncated or blank file. (*Config).Validate deliberately
// accepts an empty config because the control plane starts empty.
var errNoRoutes = errors.New("config file has no routes")

// stamp makes the delivered Version strictly increasing: the file's version
// if it is higher than the last delivered one, else last+1. Later phases drop
// any config whose version is not higher than the applied one, so a removed,
// unchanged or lowered `version:` must still produce a higher number.
func (s *FileSource) stamp(c *Config) {
	switch {
	case c.Version > s.lastVersion:
	default:
		c.Version = s.lastVersion + 1
	}
	s.lastVersion = c.Version
}

// Watch implements ConfigSource.
func (s *FileSource) Watch(ctx context.Context) (<-chan *Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil, ErrAlreadyWatching
	}
	cfg, hash, err := s.load()
	if err != nil {
		return nil, fmt.Errorf("config: file source cannot start: %w", err)
	}
	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("config: file source cannot start: %s: %w", s.path, errNoRoutes)
	}
	s.started = true
	s.lastHash = hash
	s.stamp(cfg)

	out := make(chan *Config, 1)
	out <- cfg // buffer is empty, so this cannot block
	go s.run(ctx, out)
	return out, nil
}

// run polls until ctx is done, then closes out (it is the only sender).
func (s *FileSource) run(ctx context.Context, out chan *Config) {
	defer close(out) // close only after the loop ends: closed means shut down
	t := time.NewTicker(s.interval)
	defer t.Stop()
	readFailing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cfg, hash, err := s.load()
		if err != nil && hash == ([sha256.Size]byte{}) { // read failure: transient, retry
			if !readFailing {
				s.logger.Warn("config file unreadable, keeping last good config", "path", s.path, "error", err)
			}
			readFailing = true
			continue
		}
		readFailing = false
		if hash == s.lastHash {
			s.pendingHash = [sha256.Size]byte{} // content reverted; forget the blip
			continue
		}
		// Debounce: an editor or ConfigMap update may be mid-write. Act only
		// when two consecutive polls see identical content.
		if hash != s.pendingHash {
			s.pendingHash = hash
			continue
		}
		s.pendingHash = [sha256.Size]byte{}
		s.lastHash = hash // remember bad content so it is logged once
		if err == nil && len(cfg.Routes) == 0 {
			err = fmt.Errorf("parse %s: %w", s.path, errNoRoutes)
		}
		if err != nil {
			s.logger.Error("config file invalid, keeping last good config", "path", s.path, "error", err)
			continue
		}
		s.stamp(cfg)
		replaceLatest(out, cfg)
	}
}

// replaceLatest sends cfg on a capacity-1 channel, dropping a stale unread
// value if needed. It is safe only because the caller is the sole sender:
// after draining, the buffer cannot be refilled by anyone else, so the next
// send cannot block and the newest value is never lost. The caller must not
// touch cfg afterwards.
func replaceLatest(out chan *Config, cfg *Config) {
	for {
		select {
		case out <- cfg:
			return
		default:
		}
		select {
		case <-out: // drop the stale value (the consumer may have just taken it)
		default:
		}
	}
}
