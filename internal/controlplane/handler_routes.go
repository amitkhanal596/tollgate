package controlplane

import (
	"cmp"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// errNameMismatch is the 400 for a PUT body whose name differs from the path.
const errNameMismatch = "name in body does not match path"

// routes mounts every admin endpoint (architecture.md §6.4 rows 1-13, 16).
func (a *api) routes(mux *http.ServeMux, admin func(http.Handler) http.Handler) {
	handle := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, admin(h)) }

	handle("GET /v1/config", a.getConfig)
	handle("PUT /v1/config", a.putConfig)
	handle("GET /v1/config/versions", a.listVersions)
	handle("GET /v1/config/versions/{version}", a.getVersion)
	handle("POST /v1/config/rollback", a.rollback)

	handle("GET /v1/routes", a.listRoutes)
	handle("GET /v1/routes/{name}", a.getRoute)
	handle("PUT /v1/routes/{name}", a.putRoute)
	handle("DELETE /v1/routes/{name}", a.deleteRoute)

	handle("GET /v1/upstreams", a.listUpstreams)
	handle("GET /v1/upstreams/{name}", a.getUpstream)
	handle("PUT /v1/upstreams/{name}", a.putUpstream)
	handle("DELETE /v1/upstreams/{name}", a.deleteUpstream)

	handle("GET /v1/dataplanes", a.listDataPlanes)
}

// head reads the head config under the store timeout; on failure it has
// already answered.
func (a *api) head(w http.ResponseWriter, r *http.Request) (*config.Config, bool) {
	ctx, cancel := a.storeCtx(r)
	defer cancel()
	cfg, err := a.store.Head(ctx)
	if err != nil {
		a.writeStoreError(w, r, err)
		return nil, false
	}
	return cfg, true
}

// publish hands a freshly stored config to the hub BEFORE the response is
// written (rule 10), so a client that sees the 2xx can rely on streams being
// notified. cfg must not be modified afterwards.
func (a *api) publish(cfg *config.Config) { a.hub.Publish(cfg) }

// ---- /v1/config -------------------------------------------------------------

func (a *api) getConfig(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.head(w, r)
	if !ok {
		return
	}
	setETag(w, cfg.Version)
	writeJSON(w, http.StatusOK, cfg)
}

func (a *api) putConfig(w http.ResponseWriter, r *http.Request) {
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	var doc config.Config
	if !readJSON(w, r, &doc) {
		return
	}
	a.update(w, r, req, func(c *config.Config) error {
		*c = doc // the whole document is replaced; the store overwrites Version
		return nil
	}, func(w http.ResponseWriter, cfg *config.Config) {
		writeJSON(w, http.StatusOK, cfg)
	})
}

// update runs one Store.Update, publishes, sets the new ETag and lets respond
// write the body. It is the common tail of every write handler.
func (a *api) update(w http.ResponseWriter, r *http.Request, req UpdateRequest,
	mutate func(*config.Config) error, respond func(http.ResponseWriter, *config.Config)) {
	ctx, cancel := a.storeCtx(r)
	defer cancel()
	cfg, err := a.store.Update(ctx, req, mutate)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	a.finishWrite(w, cfg, respond)
}

func (a *api) finishWrite(w http.ResponseWriter, cfg *config.Config, respond func(http.ResponseWriter, *config.Config)) {
	a.publish(cfg)
	setETag(w, cfg.Version)
	respond(w, cfg)
}

// ---- versions ---------------------------------------------------------------

func (a *api) listVersions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := DefaultListLimit
	if q.Has(QueryLimit) {
		v, ok := parseDecimal(q.Get(QueryLimit))
		if !ok || v < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive decimal integer")
			return
		}
		limit = int(min(v, MaxListLimit)) // clamp, never an error
	}
	var before int64
	if q.Has(QueryBefore) {
		v, ok := parseDecimal(q.Get(QueryBefore))
		if !ok || v < 1 {
			writeError(w, http.StatusBadRequest, "before must be a positive decimal integer")
			return
		}
		before = v
	}

	ctx, cancel := a.storeCtx(r)
	defer cancel()
	infos, err := a.store.List(ctx, limit, before)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	list := VersionList{Versions: make([]VersionItem, 0, len(infos))} // never null
	for _, vi := range infos {
		list.Versions = append(list.Versions, VersionItem{
			Version: vi.Version, CreatedAt: vi.CreatedAt, Author: vi.Author,
			Comment: vi.Comment, RolledBackFrom: vi.RolledBackFrom,
		})
	}
	if n := len(infos); n > 0 && infos[n-1].Version > 1 {
		list.NextBefore = infos[n-1].Version // older versions remain
	}
	writeJSON(w, http.StatusOK, list)
}

func (a *api) getVersion(w http.ResponseWriter, r *http.Request) {
	v, ok := parseDecimal(r.PathValue("version"))
	if !ok {
		writeError(w, http.StatusBadRequest, "version must be a non-negative decimal integer")
		return
	}
	ctx, cancel := a.storeCtx(r)
	defer cancel()
	cfg, err := a.store.Get(ctx, v)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	setETag(w, v)
	writeJSON(w, http.StatusOK, cfg)
}

func (a *api) rollback(w http.ResponseWriter, r *http.Request) {
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	var body RollbackRequest
	// An empty body means "head - 1", the same as {}.
	if written, err := decodeBody(w, r, &body); err != nil && !errors.Is(err, errEmptyBody) {
		if !written {
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	if body.ToVersion < 0 {
		writeError(w, http.StatusBadRequest, "to_version must not be negative")
		return
	}
	ctx, cancel := a.storeCtx(r)
	defer cancel()
	cfg, err := a.store.Rollback(ctx, body.ToVersion, req)
	if err != nil {
		a.writeStoreError(w, r, err)
		return
	}
	a.finishWrite(w, cfg, func(w http.ResponseWriter, cfg *config.Config) {
		writeJSON(w, http.StatusOK, cfg)
	})
}

// ---- routes -------------------------------------------------------------------

func (a *api) listRoutes(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.head(w, r)
	if !ok {
		return
	}
	routes := slices.Clone(cfg.Routes)
	if routes == nil {
		routes = []config.Route{} // never null
	}
	slices.SortFunc(routes, func(x, y config.Route) int { return cmp.Compare(x.Name, y.Name) })
	setETag(w, cfg.Version)
	writeJSON(w, http.StatusOK, RouteList{Routes: routes})
}

func (a *api) getRoute(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := a.head(w, r)
	if !ok {
		return
	}
	i := slices.IndexFunc(cfg.Routes, func(x config.Route) bool { return x.Name == name })
	if i < 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("route %q not found", name))
		return
	}
	setETag(w, cfg.Version)
	writeJSON(w, http.StatusOK, cfg.Routes[i])
}

func (a *api) putRoute(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	var rt config.Route
	if !readJSON(w, r, &rt) {
		return
	}
	if rt.Name == "" {
		rt.Name = name
	} else if rt.Name != name {
		writeError(w, http.StatusBadRequest, errNameMismatch)
		return
	}

	created := false
	a.update(w, r, req, func(c *config.Config) error {
		if i := slices.IndexFunc(c.Routes, func(x config.Route) bool { return x.Name == name }); i >= 0 {
			c.Routes[i] = rt // replace in place: keep position
			created = false
		} else {
			c.Routes = append(c.Routes, rt)
			created = true
		}
		return nil
	}, func(w http.ResponseWriter, cfg *config.Config) {
		i := slices.IndexFunc(cfg.Routes, func(x config.Route) bool { return x.Name == name })
		if created {
			w.Header().Set("Location", "/v1/routes/"+name)
		}
		writeJSON(w, putStatus(created), cfg.Routes[i])
	})
}

func (a *api) deleteRoute(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	a.update(w, r, req, func(c *config.Config) error {
		i := slices.IndexFunc(c.Routes, func(x config.Route) bool { return x.Name == name })
		if i < 0 {
			return fmt.Errorf("%w: route %q", ErrNotFound, name)
		}
		c.Routes = slices.Delete(c.Routes, i, i+1)
		return nil
	}, func(w http.ResponseWriter, _ *config.Config) { w.WriteHeader(http.StatusNoContent) })
}

// ---- upstreams ----------------------------------------------------------------

func (a *api) listUpstreams(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.head(w, r)
	if !ok {
		return
	}
	ups := slices.Clone(cfg.Upstreams)
	if ups == nil {
		ups = []config.Upstream{} // never null
	}
	slices.SortFunc(ups, func(x, y config.Upstream) int { return cmp.Compare(x.Name, y.Name) })
	setETag(w, cfg.Version)
	writeJSON(w, http.StatusOK, UpstreamList{Upstreams: ups})
}

func (a *api) getUpstream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := a.head(w, r)
	if !ok {
		return
	}
	i := slices.IndexFunc(cfg.Upstreams, func(x config.Upstream) bool { return x.Name == name })
	if i < 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("upstream %q not found", name))
		return
	}
	setETag(w, cfg.Version)
	writeJSON(w, http.StatusOK, cfg.Upstreams[i])
}

func (a *api) putUpstream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	var up config.Upstream
	if !readJSON(w, r, &up) {
		return
	}
	if up.Name == "" {
		up.Name = name
	} else if up.Name != name {
		writeError(w, http.StatusBadRequest, errNameMismatch)
		return
	}

	created := false
	a.update(w, r, req, func(c *config.Config) error {
		if i := slices.IndexFunc(c.Upstreams, func(x config.Upstream) bool { return x.Name == name }); i >= 0 {
			c.Upstreams[i] = up
			created = false
		} else {
			c.Upstreams = append(c.Upstreams, up)
			created = true
		}
		return nil
	}, func(w http.ResponseWriter, cfg *config.Config) {
		i := slices.IndexFunc(cfg.Upstreams, func(x config.Upstream) bool { return x.Name == name })
		if created {
			w.Header().Set("Location", "/v1/upstreams/"+name)
		}
		writeJSON(w, putStatus(created), cfg.Upstreams[i])
	})
}

func (a *api) deleteUpstream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := writeRequest(w, r)
	if !ok {
		return
	}
	// A route that still references the upstream makes Validate fail (400);
	// there is no cascade.
	a.update(w, r, req, func(c *config.Config) error {
		i := slices.IndexFunc(c.Upstreams, func(x config.Upstream) bool { return x.Name == name })
		if i < 0 {
			return fmt.Errorf("%w: upstream %q", ErrNotFound, name)
		}
		c.Upstreams = slices.Delete(c.Upstreams, i, i+1)
		return nil
	}, func(w http.ResponseWriter, _ *config.Config) { w.WriteHeader(http.StatusNoContent) })
}

// putStatus is 201 for a new object and 200 for a replacement.
func putStatus(created bool) int {
	if created {
		return http.StatusCreated
	}
	return http.StatusOK
}

// ---- data planes --------------------------------------------------------------

func (a *api) listDataPlanes(w http.ResponseWriter, _ *http.Request) {
	list := a.nodes.List()
	if list == nil {
		list = []NodeStatus{} // never null
	}
	writeJSON(w, http.StatusOK, DataPlaneList{DataPlanes: list})
}
