package config

import (
	"context"
	"crypto/sha256"
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
	lastHash [sha256.Size]byte
	counter  int64
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

// stamp gives a version-less config a local, strictly increasing version. It
// is called only for new content so the counter tracks real changes.
func (s *FileSource) stamp(c *Config) {
	if c.Version <= 0 {
		s.counter++
		c.Version = s.counter
	}
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
		if err != nil {
			if hash == ([sha256.Size]byte{}) { // read failure: transient, retry
				if !readFailing {
					s.logger.Warn("config file unreadable, keeping last good config", "path", s.path, "error", err)
				}
				readFailing = true
				continue
			}
			readFailing = false
			if hash != s.lastHash {
				s.lastHash = hash // remember bad content so it is logged once
				s.logger.Error("config file invalid, keeping last good config", "path", s.path, "error", err)
			}
			continue
		}
		readFailing = false
		if hash == s.lastHash {
			continue
		}
		s.lastHash = hash
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
