package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// MemStore is an in-memory Store with the same semantics as PostgresStore.
// It exists so the API can be tested without a database, and passes the same
// conformance suite. All state is guarded by one mutex; writers therefore
// run one at a time, which gives the linearizable, gapless behaviour the
// Store contract requires.
type MemStore struct {
	mu       sync.RWMutex
	versions []memVersion // versions[i] holds version i+1
}

type memVersion struct {
	cfg  *config.Config // private copy, never handed out
	info VersionInfo
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{} }

// cloneConfig deep-copies c through JSON, the same representation the
// Postgres store persists, so both stores behave alike.
func cloneConfig(c *config.Config) (*config.Config, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("clone config: encode: %w", err)
	}
	out := new(config.Config)
	if err := json.Unmarshal(b, out); err != nil {
		return nil, fmt.Errorf("clone config: decode: %w", err)
	}
	return out, nil
}

// headLocked returns the head config (not a copy) or nil and version 0.
// Caller holds mu.
func (s *MemStore) headLocked() (*config.Config, int64) {
	if len(s.versions) == 0 {
		return nil, 0
	}
	return s.versions[len(s.versions)-1].cfg, int64(len(s.versions))
}

// Head implements Store.
func (s *MemStore) Head(ctx context.Context) (*config.Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, _ := s.headLocked()
	if h == nil {
		return &config.Config{}, nil
	}
	return cloneConfig(h)
}

// Get implements Store.
func (s *MemStore) Get(ctx context.Context, version int64) (*config.Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version < 1 || version > int64(len(s.versions)) {
		return nil, fmt.Errorf("%w: version %d", ErrNotFound, version)
	}
	return cloneConfig(s.versions[version-1].cfg)
}

// clampLimit applies the List page-size rules from store.go.
func clampLimit(limit int) int {
	if limit < 1 {
		return DefaultListLimit
	}
	return min(limit, MaxListLimit)
}

// List implements Store.
func (s *MemStore) List(ctx context.Context, limit int, before int64) ([]VersionInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit = clampLimit(limit)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []VersionInfo{} // non-nil even when empty
	for v := int64(len(s.versions)); v >= 1 && len(out) < limit; v-- {
		if before > 0 && v >= before {
			continue
		}
		info := s.versions[v-1].info
		if info.RolledBackFrom != nil {
			rb := *info.RolledBackFrom // do not share the pointer with the store
			info.RolledBackFrom = &rb
		}
		out = append(out, info)
	}
	return out, nil
}

// Update implements Store.
func (s *MemStore) Update(ctx context.Context, req UpdateRequest, mutate func(*config.Config) error) (*config.Config, error) {
	return s.write(ctx, req, nil, func(head *config.Config, _ int64) (*config.Config, *int64, error) {
		cp, err := cloneConfig(head)
		if err != nil {
			return nil, nil, err
		}
		if err := mutate(cp); err != nil {
			return nil, nil, err // returned unchanged, per the Store contract
		}
		return cp, nil, nil
	})
}

// Rollback implements Store.
func (s *MemStore) Rollback(ctx context.Context, to int64, req UpdateRequest) (*config.Config, error) {
	return s.write(ctx, req, &to, nil)
}

// write is the single write path: lock, read head, check IfMatch, build the
// new document, stamp the version, normalize, validate, append.
// Exactly one of rollbackTo / build is set.
func (s *MemStore) write(ctx context.Context, req UpdateRequest, rollbackTo *int64,
	build func(head *config.Config, headVersion int64) (*config.Config, *int64, error)) (*config.Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil { // the wait for the lock may have outlived ctx
		return nil, err
	}

	head, headVersion := s.headLocked()
	if head == nil {
		head = &config.Config{}
	}
	if req.IfMatch != nil && *req.IfMatch != headVersion {
		return nil, fmt.Errorf("%w: if-match %d, head is %d", ErrVersionConflict, *req.IfMatch, headVersion)
	}

	var (
		next       *config.Config
		rolledFrom *int64
		err        error
	)
	if rollbackTo != nil {
		target := *rollbackTo
		if target == 0 {
			target = headVersion - 1
		}
		if target < 1 || target > headVersion {
			return nil, fmt.Errorf("%w: rollback target %d (head is %d)", ErrNotFound, *rollbackTo, headVersion)
		}
		if next, err = cloneConfig(s.versions[target-1].cfg); err != nil {
			return nil, err
		}
		rolledFrom = &target
	} else if next, rolledFrom, err = build(head, headVersion); err != nil {
		return nil, err
	}

	next.Version = headVersion + 1 // overwrites whatever mutate set
	next.Normalize()
	if err := next.Validate(); err != nil {
		return nil, err
	}
	b, err := marshalStored(next) // same encoding and limits as PostgresStore
	if err != nil {
		return nil, err
	}
	stored := new(config.Config) // the store keeps its own copy, decoded like a stored body
	if err := json.Unmarshal(b, stored); err != nil {
		return nil, fmt.Errorf("clone config: decode: %w", err)
	}
	s.versions = append(s.versions, memVersion{
		cfg: stored,
		info: VersionInfo{
			Version:        next.Version,
			CreatedAt:      time.Now().UTC(),
			Author:         req.Author,
			Comment:        req.Comment,
			RolledBackFrom: rolledFrom,
		},
	})
	return next, nil
}

// Ping implements Store; memory is always reachable.
func (s *MemStore) Ping(ctx context.Context) error { return nil }
