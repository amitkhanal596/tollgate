package controlplane

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

const versionConflictFmt = "controlplane: version conflict: if-match %d, head is %d"

// oversizeBody is valid JSON preceded by more than MaxBodyBytes of whitespace,
// so the 1 MiB limit trips before any decoding verdict.
func oversizeBody(json string) string {
	return strings.Repeat(" ", MaxBodyBytes+1) + json
}

// ---- GET/PUT /v1/config -----------------------------------------------------

func TestConfig_GetOnEmptyStore(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	rec := e.admin(t, "GET", "/v1/config", "")
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if got := rec.Header().Get("ETag"); got != `"0"` {
		t.Fatalf(`ETag = %q, want "0"`, got)
	}
	c := decodeConfig(t, rec)
	if c.Version != 0 || len(c.Routes) != 0 || len(c.Upstreams) != 0 {
		t.Fatalf("body = %+v", c)
	}
}

func TestConfig_PutSuccess(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name       string
		body       string
		wantRoutes int
		wantUps    int
	}{
		{"seed document", seedJSON, 1, 1},
		{"version in body is ignored", strings.Replace(seedJSON, `"version":0`, `"version":99`, 1), 1, 1},
		{"zero routes and upstreams are accepted", `{"routes":[],"upstreams":[]}`, 0, 0},
		{"null routes accepted", `{"routes":null,"upstreams":null}`, 0, 0},
		{"empty object accepted", `{}`, 0, 0},
		{"upstream without routes", `{"upstreams":[{"name":"web","backends":["` + seedBackend + `"]}]}`, 0, 1},
		{"trailing whitespace allowed", seedJSON + " \n\t ", 1, 1},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			rec := e.admin(t, "PUT", "/v1/config", tc.body)
			if rec.Code != 200 {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if got := rec.Header().Get("ETag"); got != `"1"` {
				t.Fatalf(`ETag = %q, want "1"`, got)
			}
			c := decodeConfig(t, rec)
			if c.Version != 1 || len(c.Routes) != tc.wantRoutes || len(c.Upstreams) != tc.wantUps {
				t.Fatalf("body = %+v", c)
			}
			// GET returns exactly what PUT returned.
			get := e.admin(t, "GET", "/v1/config", "")
			if get.Header().Get("ETag") != `"1"` || canonJSON(decodeConfig(t, get)) != canonJSON(c) {
				t.Fatalf("GET after PUT differs: %s vs %s", get.Body, rec.Body)
			}
			// Published to the hub with the same content (rule 10).
			pub, ok := tryRecv(sub)
			if !ok || canonJSON(pub) != canonJSON(c) {
				t.Fatalf("hub got %v, want %s", pub, canonJSON(c))
			}
		})
	}
}

func TestConfig_PutNormalizes(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	body := `{"routes":[{"name":"r","host":"API.Example.COM","path_prefix":"/r","upstream":"web"}],"upstreams":[{"name":"web","backends":["` + seedBackend + `"]}]}`
	c := decodeConfig(t, e.admin(t, "PUT", "/v1/config", body))
	if c.Routes[0].Host != "api.example.com" || c.Routes[0].Auth != config.AuthNone || c.Upstreams[0].Algorithm != config.AlgorithmRoundRobin {
		t.Fatalf("not normalized: %+v", c)
	}
}

func TestConfig_EveryWriteCreatesAVersion(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	for want := int64(1); want <= 3; want++ {
		rec := e.admin(t, "PUT", "/v1/config", seedJSON) // identical document each time
		if rec.Code != 200 || etagVersion(t, rec) != want || decodeConfig(t, rec).Version != want {
			t.Fatalf("write #%d: %d etag %q", want, rec.Code, rec.Header().Get("ETag"))
		}
	}
	var vl VersionList
	mustJSON(t, e.admin(t, "GET", "/v1/config/versions", ""), &vl)
	if len(vl.Versions) != 3 {
		t.Fatalf("%d versions, want 3", len(vl.Versions))
	}
}

func TestConfig_PutErrors(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantInMsg  string
	}{
		{"malformed JSON", `{"routes":`, 400, ""},
		{"not JSON", `hello`, 400, ""},
		{"empty body", ``, 400, ""},
		{"unknown top-level field", `{"bogus":1}`, 400, "bogus"},
		{"unknown nested field", `{"routes":[{"name":"r","path_prefix":"/","upstream":"u","bogus":1}]}`, 400, "bogus"},
		{"wrong type", `{"routes":"nope"}`, 400, ""},
		{"trailing garbage", `{} x`, 400, ""},
		{"two documents", `{} {}`, 400, ""},
		{"route to unknown upstream", `{"routes":[{"name":"r","path_prefix":"/r","upstream":"ghost"}]}`, 400, "routes[0].upstream"},
		{"duplicate route names", `{"routes":[{"name":"r","path_prefix":"/a","upstream":"web"},{"name":"r","path_prefix":"/b","upstream":"web"}],"upstreams":[{"name":"web","backends":["` + seedBackend + `"]}]}`, 400, "duplicated"},
		{"bad path prefix", `{"routes":[{"name":"r","path_prefix":"nope","upstream":"web"}],"upstreams":[{"name":"web","backends":["` + seedBackend + `"]}]}`, 400, "path_prefix"},
		{"body over 1 MiB", oversizeBody(seedJSON), 413, "request body too large"},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			rec := e.admin(t, "PUT", "/v1/config", tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; body %s", rec.Code, tc.wantStatus, rec.Body)
			}
			msg := errBody(t, rec)
			if !strings.Contains(msg, tc.wantInMsg) {
				t.Fatalf("error %q does not mention %q", msg, tc.wantInMsg)
			}
			if tc.wantStatus == 413 && msg != "request body too large" {
				t.Fatalf("413 message = %q", msg)
			}
			if v := e.headVersion(t); v != 0 {
				t.Fatalf("failed write created version %d", v)
			}
			if c, ok := tryRecv(sub); ok {
				t.Fatalf("failed write published v%d", c.Version)
			}
		})
	}
}

func TestConfig_ValidationMessageIsOnePerLine(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	body := `{"routes":[{"name":"a","path_prefix":"/a","upstream":"x"},{"name":"b","path_prefix":"/b","upstream":"y"}]}`
	rec := e.admin(t, "PUT", "/v1/config", body)
	msg := errBody(t, rec)
	if rec.Code != 400 || !strings.Contains(msg, "routes[0].upstream") || !strings.Contains(msg, "routes[1].upstream") {
		t.Fatalf("status %d message %q; want both problems listed (err.Error() of the joined error)", rec.Code, msg)
	}
}

// ---- If-Match ------------------------------------------------------------------

func TestIfMatch(t *testing.T) {
	defer failOnPanic(t)
	// All write endpoints honour the same rules; PUT /v1/config is the probe,
	// then each other write endpoint gets a conflict and a success case.
	cases := []struct {
		name       string
		header     string
		wantStatus int
	}{
		{"quoted match", `"1"`, 200},
		{"bare match", `1`, 200},
		{"quoted stale", `"2"`, 412},
		{"bare stale", `2`, 412},
		{"quoted zero on non-empty", `"0"`, 412},
		{"bare zero on non-empty", `0`, 412},
		{"star", `*`, 400},
		{"weak validator", `W/"1"`, 400},
		{"list of tags", `"1", "2"`, 400},
		{"garbage", `abc`, 400},
		{"negative", `-1`, 400},
		{"empty quotes", `""`, 400},
		{"unterminated quote", `"1`, 400},
		{"fraction", `1.5`, 400},
		{"hex", `0x1`, 400},
		{"overflow", `99999999999999999999`, 400},
		{"plus sign", `+1`, 400},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			rec := e.admin(t, "PUT", "/v1/config", seedJSON, "If-Match", tc.header)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; body %s", rec.Code, tc.wantStatus, rec.Body)
			}
			switch tc.wantStatus {
			case 200:
				if etagVersion(t, rec) != 2 {
					t.Fatalf("ETag %q, want \"2\"", rec.Header().Get("ETag"))
				}
			default:
				errBody(t, rec)
				if v := e.headVersion(t); v != 1 {
					t.Fatalf("rejected write moved head to %d", v)
				}
			}
		})
	}

	run(t, "412 message is the documented one", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "PUT", "/v1/config", seedJSON, "If-Match", `"7"`)
		if rec.Code != 412 {
			t.Fatalf("status %d", rec.Code)
		}
		if got, want := errBody(t, rec), fmt.Sprintf(versionConflictFmt, 7, 1); got != want {
			t.Fatalf("message %q, want %q", got, want)
		}
	})
	run(t, `"0" succeeds only on an empty store`, func(t *testing.T) {
		e := newEnv(t)
		if rec := e.admin(t, "PUT", "/v1/config", seedJSON, "If-Match", `"0"`); rec.Code != 200 {
			t.Fatalf("seed with If-Match \"0\": %d %s", rec.Code, rec.Body)
		}
		if rec := e.admin(t, "PUT", "/v1/config", seedJSON, "If-Match", `0`); rec.Code != 412 {
			t.Fatalf("second seed with If-Match 0: %d", rec.Code)
		}
	})
	run(t, "malformed If-Match wins over a body that would also be rejected, nothing written", func(t *testing.T) {
		e := newEnv(t)
		if rec := e.admin(t, "PUT", "/v1/config", seedJSON, "If-Match", "*"); rec.Code != 400 {
			t.Fatalf("status %d", rec.Code)
		}
		if v := e.headVersion(t); v != 0 {
			t.Fatalf("wrote version %d", v)
		}
	})

	// Every write endpoint: stale -> 412 without writing; malformed -> 400;
	// matching -> success.
	writes := []struct {
		name         string
		method, path string
		body         string
		okStatus     int
	}{
		{"PUT /v1/config", "PUT", "/v1/config", seedJSON, 200},
		{"PUT route", "PUT", "/v1/routes/extra", routeJSON("extra", "/extra", "web"), 201},
		{"DELETE route", "DELETE", "/v1/routes/api", "", 204},
		{"PUT upstream", "PUT", "/v1/upstreams/extra", upstreamJSON("extra"), 201},
		{"DELETE upstream (unreferenced)", "DELETE", "/v1/upstreams/spare", "", 204},
		{"POST rollback", "POST", "/v1/config/rollback", `{"to_version":1}`, 200},
	}
	for _, w := range writes {
		run(t, w.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			e.putUpstream(t, "spare") // head is now 2
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			recvWithin(t, sub, 0+1e9) // drain the primed max

			rec := e.admin(t, w.method, w.path, w.body, "If-Match", `"1"`)
			if rec.Code != 412 {
				t.Fatalf("stale: status %d, want 412; %s", rec.Code, rec.Body)
			}
			if msg := errBody(t, rec); msg != fmt.Sprintf(versionConflictFmt, 1, 2) {
				t.Fatalf("stale message %q", msg)
			}
			rec = e.admin(t, w.method, w.path, w.body, "If-Match", `W/"2"`)
			if rec.Code != 400 {
				t.Fatalf("weak tag: status %d, want 400", rec.Code)
			}
			if v := e.headVersion(t); v != 2 {
				t.Fatalf("rejected writes moved head to %d", v)
			}
			if c, ok := tryRecv(sub); ok {
				t.Fatalf("rejected write published v%d", c.Version)
			}
			rec = e.admin(t, w.method, w.path, w.body, "If-Match", `"2"`)
			if rec.Code != w.okStatus {
				t.Fatalf("matching: status %d, want %d; %s", rec.Code, w.okStatus, rec.Body)
			}
			if etagVersion(t, rec) != 3 {
				t.Fatalf("ETag %q, want \"3\"", rec.Header().Get("ETag"))
			}
		})
	}
}

// ---- audit headers -------------------------------------------------------------

func TestAuditHeaders(t *testing.T) {
	defer failOnPanic(t)
	author := func(n int) string { return strings.Repeat("a", n) }
	comment := func(n int) string { return strings.Repeat("c", n) }
	cases := []struct {
		name            string
		author, comment string
		wantStatus      int
	}{
		{"both set", "alice", "first seed", 200},
		{"neither set", "", "", 200},
		{"author at limit", author(MaxAuthorBytes), "", 200},
		{"comment at limit", "", comment(MaxCommentBytes), 200},
		{"author over limit", author(MaxAuthorBytes + 1), "", 400},
		{"comment over limit", "", comment(MaxCommentBytes + 1), 400},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			var kv []string
			if tc.author != "" {
				kv = append(kv, "X-Tollgate-Author", tc.author)
			}
			if tc.comment != "" {
				kv = append(kv, "X-Tollgate-Comment", tc.comment)
			}
			rec := e.admin(t, "PUT", "/v1/config", seedJSON, kv...)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus != 200 {
				errBody(t, rec)
				if v := e.headVersion(t); v != 0 {
					t.Fatalf("rejected write created version %d", v)
				}
				return
			}
			var vl VersionList
			mustJSON(t, e.admin(t, "GET", "/v1/config/versions", ""), &vl)
			if len(vl.Versions) != 1 || vl.Versions[0].Author != tc.author || vl.Versions[0].Comment != tc.comment {
				t.Fatalf("versions = %+v, want stored verbatim", vl.Versions)
			}
		})
	}
	run(t, "recorded by every kind of write", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		h := []string{"X-Tollgate-Author", "bob", "X-Tollgate-Comment", "edit"}
		e.admin(t, "PUT", "/v1/routes/b", routeJSON("b", "/b", "web"), h...)
		e.admin(t, "DELETE", "/v1/routes/b", "", h...)
		e.admin(t, "PUT", "/v1/upstreams/z", upstreamJSON("z"), h...)
		e.admin(t, "POST", "/v1/config/rollback", `{}`, h...)
		var vl VersionList
		mustJSON(t, e.admin(t, "GET", "/v1/config/versions", ""), &vl)
		if len(vl.Versions) != 5 {
			t.Fatalf("%d versions", len(vl.Versions))
		}
		for _, v := range vl.Versions[:4] {
			if v.Author != "bob" || v.Comment != "edit" {
				t.Errorf("v%d = %+v", v.Version, v)
			}
		}
		if v := vl.Versions[4]; v.Author != "" || v.Comment != "" {
			t.Errorf("v1 should have no audit labels: %+v", v)
		}
	})
	run(t, "audit headers are checked on every write endpoint", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		long := strings.Repeat("a", MaxAuthorBytes+1)
		for _, w := range []endpoint{
			{"PUT", "/v1/routes/b", routeJSON("b", "/b", "web")},
			{"DELETE", "/v1/routes/api", ""},
			{"PUT", "/v1/upstreams/z", upstreamJSON("z")},
			{"POST", "/v1/config/rollback", `{}`},
		} {
			if rec := e.admin(t, w.method, w.path, w.body, "X-Tollgate-Author", long); rec.Code != 400 {
				t.Errorf("%s %s: status %d, want 400", w.method, w.path, rec.Code)
			}
		}
		if v := e.headVersion(t); v != 1 {
			t.Fatalf("rejected writes moved head to %d", v)
		}
	})
}

// ---- versions -------------------------------------------------------------------

// versionedEnv has versions 1..n; v1 is the seed, later ones add upstreams.
func versionedEnv(t *testing.T, n int) *env {
	t.Helper()
	e := newEnv(t)
	e.seed(t)
	for i := 2; i <= n; i++ {
		rec := e.admin(t, "PUT", fmt.Sprintf("/v1/upstreams/up%d", i), upstreamJSON(fmt.Sprintf("up%d", i)),
			"X-Tollgate-Author", fmt.Sprintf("author-%d", i), "X-Tollgate-Comment", fmt.Sprintf("comment %d", i))
		if rec.Code != 201 {
			t.Fatalf("setup v%d: %d %s", i, rec.Code, rec.Body)
		}
	}
	return e
}

func TestVersions_List(t *testing.T) {
	defer failOnPanic(t)
	e := versionedEnv(t, 5)
	cases := []struct {
		name           string
		query          string
		want           []int64
		wantNextBefore int64
	}{
		{"default", "", []int64{5, 4, 3, 2, 1}, 0},
		{"limit 2", "?limit=2", []int64{5, 4}, 4},
		{"limit 2 before 4", "?limit=2&before=4", []int64{3, 2}, 2},
		{"last page", "?limit=2&before=2", []int64{1}, 0},
		{"limit 3 before 3", "?limit=3&before=3", []int64{2, 1}, 0},
		{"exact fit has no next_before", "?limit=5", []int64{5, 4, 3, 2, 1}, 0},
		{"limit 1", "?limit=1", []int64{5}, 5},
		{"before 1 is empty", "?before=1", []int64{}, 0},
		{"before beyond head", "?before=100&limit=1", []int64{5}, 5},
		{"limit above 500 is clamped, not an error", "?limit=600", []int64{5, 4, 3, 2, 1}, 0},
		{"unknown params ignored", "?foo=bar", []int64{5, 4, 3, 2, 1}, 0},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			rec := e.admin(t, "GET", "/v1/config/versions"+tc.query, "")
			if rec.Code != 200 {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			var vl VersionList
			mustJSON(t, rec, &vl)
			if vl.Versions == nil {
				t.Fatal("versions is null, want []")
			}
			var got []int64
			for _, v := range vl.Versions {
				got = append(got, v.Version)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) && !(len(got) == 0 && len(tc.want) == 0) {
				t.Fatalf("versions %v, want %v", got, tc.want)
			}
			if vl.NextBefore != tc.wantNextBefore {
				t.Fatalf("next_before = %d, want %d", vl.NextBefore, tc.wantNextBefore)
			}
		})
	}
}

func TestVersions_ListMetadata(t *testing.T) {
	defer failOnPanic(t)
	e := versionedEnv(t, 3)
	e.admin(t, "POST", "/v1/config/rollback", `{"to_version":1}`, "X-Tollgate-Author", "rb") // v4
	var vl VersionList
	mustJSON(t, e.admin(t, "GET", "/v1/config/versions", ""), &vl)
	if len(vl.Versions) != 4 {
		t.Fatalf("%d versions", len(vl.Versions))
	}
	for _, v := range vl.Versions {
		if v.CreatedAt.IsZero() {
			t.Errorf("v%d has no created_at", v.Version)
		}
	}
	if v := vl.Versions[0]; v.Version != 4 || v.Author != "rb" || v.RolledBackFrom == nil || *v.RolledBackFrom != 1 {
		t.Errorf("v4 = %+v", v)
	}
	if v := vl.Versions[2]; v.Version != 2 || v.Author != "author-2" || v.Comment != "comment 2" || v.RolledBackFrom != nil {
		t.Errorf("v2 = %+v", v)
	}
	// rolled_back_from is omitted (not null) when absent.
	raw := e.admin(t, "GET", "/v1/config/versions?limit=1&before=3", "").Body.String()
	if strings.Contains(raw, "rolled_back_from") {
		t.Errorf("rolled_back_from present for a non-rollback version: %s", raw)
	}
}

func TestVersions_ListEmptyStoreIsEmptyArray(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	rec := e.admin(t, "GET", "/v1/config/versions", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"versions":[]`) {
		t.Fatalf("%d %s, want versions as []", rec.Code, rec.Body)
	}
}

func TestVersions_ListBadQuery(t *testing.T) {
	defer failOnPanic(t)
	e := versionedEnv(t, 2)
	for _, q := range []string{
		"?limit=abc", "?limit=0", "?limit=-1", "?limit=1.5", "?limit=", "?limit=99999999999999999999",
		"?before=abc", "?before=0", "?before=-3", "?before=1.5", "?before=99999999999999999999",
	} {
		rec := e.admin(t, "GET", "/v1/config/versions"+q, "")
		if rec.Code != 400 {
			t.Errorf("%s: status %d, want 400", q, rec.Code)
			continue
		}
		errBody(t, rec)
	}
}

func TestVersions_Get(t *testing.T) {
	defer failOnPanic(t)
	e := versionedEnv(t, 3)
	for v := int64(1); v <= 3; v++ {
		rec := e.admin(t, "GET", fmt.Sprintf("/v1/config/versions/%d", v), "")
		if rec.Code != 200 || rec.Header().Get("ETag") != fmt.Sprintf(`"%d"`, v) {
			t.Fatalf("v%d: status %d etag %q", v, rec.Code, rec.Header().Get("ETag"))
		}
		c := decodeConfig(t, rec)
		if c.Version != v || len(c.Upstreams) != int(v) {
			t.Fatalf("v%d body: version %d with %d upstreams", v, c.Version, len(c.Upstreams))
		}
	}
	cases := []struct {
		path string
		want int
	}{
		{"/v1/config/versions/0", 404},
		{"/v1/config/versions/4", 404},
		{"/v1/config/versions/999", 404},
		{"/v1/config/versions/abc", 400},
		{"/v1/config/versions/-1", 400},
		{"/v1/config/versions/1.5", 400},
		{"/v1/config/versions/99999999999999999999", 400},
	}
	for _, tc := range cases {
		rec := e.admin(t, "GET", tc.path, "")
		if rec.Code != tc.want {
			t.Errorf("GET %s: status %d, want %d", tc.path, rec.Code, tc.want)
			continue
		}
		errBody(t, rec)
	}
}

// ---- rollback -------------------------------------------------------------------

func TestRollback(t *testing.T) {
	defer failOnPanic(t)
	// Setup: v1 = seed (1 upstream), v2 = +up2, v3 = +up3 (3 upstreams: web, up2, up3... see versionedEnv).
	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantTarget int64 // version whose body the new head must equal
	}{
		{"empty body means head-1", ``, 200, 2},
		{"empty object means head-1", `{}`, 200, 2},
		{"to_version 0 means head-1", `{"to_version":0}`, 200, 2},
		{"explicit older version", `{"to_version":1}`, 200, 1},
		{"to head is allowed", `{"to_version":3}`, 200, 3},
		{"missing version", `{"to_version":99}`, 404, 0},
		{"one past head", `{"to_version":4}`, 404, 0},
		{"negative", `{"to_version":-1}`, 400, 0},
		{"unknown field", `{"to":1}`, 400, 0},
		{"wrong type", `{"to_version":"1"}`, 400, 0},
		{"malformed", `{"to_version":`, 400, 0},
		{"trailing garbage", `{} {}`, 400, 0},
		{"body over 1 MiB", oversizeBody(`{}`), 413, 0},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := versionedEnv(t, 3)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			recvWithin(t, sub, 1e9) // primed with v3
			target, _ := e.store.Get(testCtx(t), max(tc.wantTarget, 1))

			rec := e.admin(t, "POST", "/v1/config/rollback", tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus != 200 {
				errBody(t, rec)
				if v := e.headVersion(t); v != 3 {
					t.Fatalf("failed rollback moved head to %d", v)
				}
				if c, ok := tryRecv(sub); ok {
					t.Fatalf("failed rollback published v%d", c.Version)
				}
				return
			}
			if etagVersion(t, rec) != 4 {
				t.Fatalf("ETag %q, want \"4\"", rec.Header().Get("ETag"))
			}
			c := decodeConfig(t, rec)
			if c.Version != 4 || canonBody(c) != canonBody(target) {
				t.Fatalf("rolled-back config = %s, want v4 with body of v%d (%s)", canonJSON(c), tc.wantTarget, canonJSON(target))
			}
			if h := e.headVersion(t); h != 4 {
				t.Fatalf("head = %d", h)
			}
			pub, ok := tryRecv(sub)
			if !ok || pub.Version != 4 {
				t.Fatalf("hub got %v, want v4", pub)
			}
			var vl VersionList
			mustJSON(t, e.admin(t, "GET", "/v1/config/versions?limit=1", ""), &vl)
			if r := vl.Versions[0].RolledBackFrom; r == nil || *r != tc.wantTarget {
				t.Fatalf("rolled_back_from = %v, want %d", r, tc.wantTarget)
			}
		})
	}
}

func TestRollback_HeadAtMostOne(t *testing.T) {
	defer failOnPanic(t)
	run(t, "empty store", func(t *testing.T) {
		e := newEnv(t)
		rec := e.admin(t, "POST", "/v1/config/rollback", `{}`)
		if rec.Code != 404 {
			t.Fatalf("status %d, want 404", rec.Code)
		}
		errBody(t, rec)
	})
	run(t, "head 1, default target", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		for _, body := range []string{``, `{}`, `{"to_version":0}`} {
			if rec := e.admin(t, "POST", "/v1/config/rollback", body); rec.Code != 404 {
				t.Fatalf("body %q: status %d, want 404", body, rec.Code)
			}
		}
		if v := e.headVersion(t); v != 1 {
			t.Fatalf("head moved to %d", v)
		}
	})
	run(t, "head 1, explicit to_version 1 is allowed", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "POST", "/v1/config/rollback", `{"to_version":1}`)
		if rec.Code != 200 || etagVersion(t, rec) != 2 {
			t.Fatalf("status %d etag %q", rec.Code, rec.Header().Get("ETag"))
		}
	})
}

// ---- routes ---------------------------------------------------------------------

func TestRoutes_ListAndGet(t *testing.T) {
	defer failOnPanic(t)
	run(t, "empty store lists []", func(t *testing.T) {
		e := newEnv(t)
		rec := e.admin(t, "GET", "/v1/routes", "")
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"routes":[]`) || rec.Header().Get("ETag") != `"0"` {
			t.Fatalf("%d etag %q body %s", rec.Code, rec.Header().Get("ETag"), rec.Body)
		}
	})
	run(t, "sorted by name, ETag is head", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		e.admin(t, "PUT", "/v1/routes/zeta", routeJSON("zeta", "/z", "web"))
		e.admin(t, "PUT", "/v1/routes/beta", routeJSON("beta", "/b", "web"))
		rec := e.admin(t, "GET", "/v1/routes", "")
		var rl RouteList
		mustJSON(t, rec, &rl)
		var names []string
		for _, r := range rl.Routes {
			names = append(names, r.Name)
		}
		if fmt.Sprint(names) != "[api beta zeta]" || rec.Header().Get("ETag") != `"3"` {
			t.Fatalf("names %v etag %q", names, rec.Header().Get("ETag"))
		}
	})
	run(t, "get one is normalized", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "GET", "/v1/routes/api", "")
		var r config.Route
		mustJSON(t, rec, &r)
		if rec.Code != 200 || r.Name != "api" || r.Auth != config.AuthNone || rec.Header().Get("ETag") != `"1"` {
			t.Fatalf("%d %+v etag %q", rec.Code, r, rec.Header().Get("ETag"))
		}
	})
	run(t, "get missing is 404", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "GET", "/v1/routes/ghost", "")
		if rec.Code != 404 {
			t.Fatalf("status %d", rec.Code)
		}
		errBody(t, rec)
	})
}

func TestRoutes_Put(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name       string
		path       string
		body       string
		wantStatus int
		wantName   string // name in the response when successful
		wantMsg    string // substring of the error message
	}{
		{"create", "/v1/routes/beta", routeJSON("beta", "/b", "web"), 201, "beta", ""},
		{"replace existing", "/v1/routes/api", routeJSON("api", "/api2", "web"), 200, "api", ""},
		{"empty body name takes the path name", "/v1/routes/beta", `{"path_prefix":"/b","upstream":"web"}`, 201, "beta", ""},
		{"explicit empty name takes the path name", "/v1/routes/beta", `{"name":"","path_prefix":"/b","upstream":"web"}`, 201, "beta", ""},
		{"name mismatch", "/v1/routes/beta", routeJSON("other", "/b", "web"), 400, "", "name in body does not match path"},
		{"unknown upstream", "/v1/routes/beta", routeJSON("beta", "/b", "ghost"), 400, "", "upstream"},
		{"duplicate host+path_prefix with another route", "/v1/routes/beta", routeJSON("beta", "/api", "web"), 400, "", "duplicated"},
		{"bad path_prefix", "/v1/routes/beta", routeJSON("beta", "b", "web"), 400, "", "path_prefix"},
		{"unknown field", "/v1/routes/beta", `{"path_prefix":"/b","upstream":"web","bogus":1}`, 400, "", "bogus"},
		{"malformed", "/v1/routes/beta", `{`, 400, "", ""},
		{"empty body", "/v1/routes/beta", ``, 400, "", ""},
		{"too large", "/v1/routes/beta", oversizeBody(routeJSON("beta", "/b", "web")), 413, "", "request body too large"},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			recvWithin(t, sub, 1e9)

			rec := e.admin(t, "PUT", tc.path, tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus >= 400 {
				if msg := errBody(t, rec); !strings.Contains(msg, tc.wantMsg) {
					t.Fatalf("error %q, want it to mention %q", msg, tc.wantMsg)
				}
				if tc.wantMsg == "name in body does not match path" {
					if msg := errBody(t, rec); msg != tc.wantMsg {
						t.Fatalf("error %q, want exactly %q", msg, tc.wantMsg)
					}
				}
				if v := e.headVersion(t); v != 1 {
					t.Fatalf("failed write moved head to %d", v)
				}
				if c, ok := tryRecv(sub); ok {
					t.Fatalf("failed write published v%d", c.Version)
				}
				return
			}
			var r config.Route
			mustJSON(t, rec, &r)
			if r.Name != tc.wantName || r.Auth != config.AuthNone {
				t.Fatalf("response route %+v, want name %q and normalized auth", r, tc.wantName)
			}
			if etagVersion(t, rec) != 2 {
				t.Fatalf("ETag %q, want \"2\"", rec.Header().Get("ETag"))
			}
			loc := rec.Header().Get("Location")
			if tc.wantStatus == 201 && loc != "/v1/routes/"+tc.wantName {
				t.Fatalf("Location = %q", loc)
			}
			if tc.wantStatus == 200 && loc != "" {
				t.Fatalf("Location = %q on a replace", loc)
			}
			if pub, ok := tryRecv(sub); !ok || pub.Version != 2 {
				t.Fatalf("hub got %v, want v2", pub)
			}
			// What was stored is what the GET returns.
			var got config.Route
			mustJSON(t, e.admin(t, "GET", "/v1/routes/"+tc.wantName, ""), &got)
			if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", r) {
				t.Fatalf("GET %+v != PUT response %+v", got, r)
			}
		})
	}
}

func TestRoutes_PutNormalizesResponse(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	e.seed(t)
	rec := e.admin(t, "PUT", "/v1/routes/hosty", `{"host":"API.Example.COM","path_prefix":"/h","upstream":"web"}`)
	var r config.Route
	mustJSON(t, rec, &r)
	if rec.Code != 201 || r.Host != "api.example.com" {
		t.Fatalf("%d %+v", rec.Code, r)
	}
}

func TestRoutes_ReplaceKeepsPosition(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	e.seed(t) // api
	e.admin(t, "PUT", "/v1/routes/zeta", routeJSON("zeta", "/z", "web"))
	e.admin(t, "PUT", "/v1/routes/beta", routeJSON("beta", "/b", "web"))
	e.admin(t, "PUT", "/v1/routes/zeta", routeJSON("zeta", "/z2", "web")) // replace in place
	c := decodeConfig(t, e.admin(t, "GET", "/v1/config", ""))
	var names []string
	for _, r := range c.Routes {
		names = append(names, r.Name)
	}
	// Document order = insertion order (api, zeta, beta), not sorted: only the
	// list endpoints sort (rule 12).
	if fmt.Sprint(names) != "[api zeta beta]" || c.Routes[1].PathPrefix != "/z2" {
		t.Fatalf("routes %v (zeta prefix %q)", names, c.Routes[1].PathPrefix)
	}
}

func TestRoutes_Delete(t *testing.T) {
	defer failOnPanic(t)
	run(t, "success", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		sub, cancel := e.hub.Subscribe()
		defer cancel()
		recvWithin(t, sub, 1e9)
		rec := e.admin(t, "DELETE", "/v1/routes/api", "")
		if rec.Code != 204 || rec.Body.Len() != 0 || rec.Header().Get("ETag") != `"2"` {
			t.Fatalf("status %d body %q etag %q", rec.Code, rec.Body, rec.Header().Get("ETag"))
		}
		if rec := e.admin(t, "GET", "/v1/routes/api", ""); rec.Code != 404 {
			t.Fatalf("route still there: %d", rec.Code)
		}
		if c := decodeConfig(t, e.admin(t, "GET", "/v1/config", "")); len(c.Routes) != 0 || c.Version != 2 {
			t.Fatalf("config after delete: %+v", c)
		}
		if pub, ok := tryRecv(sub); !ok || pub.Version != 2 {
			t.Fatalf("hub got %v, want v2", pub)
		}
	})
	run(t, "missing is 404 and writes nothing", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		sub, cancel := e.hub.Subscribe()
		defer cancel()
		recvWithin(t, sub, 1e9)
		rec := e.admin(t, "DELETE", "/v1/routes/ghost", "")
		if rec.Code != 404 {
			t.Fatalf("status %d", rec.Code)
		}
		errBody(t, rec)
		if v := e.headVersion(t); v != 1 {
			t.Fatalf("head %d", v)
		}
		if c, ok := tryRecv(sub); ok {
			t.Fatalf("published v%d", c.Version)
		}
	})
	run(t, "missing on empty store is 404", func(t *testing.T) {
		e := newEnv(t)
		if rec := e.admin(t, "DELETE", "/v1/routes/ghost", ""); rec.Code != 404 {
			t.Fatalf("status %d", rec.Code)
		}
	})
	run(t, "stale If-Match on a missing name is 412, matching is 404 (rule 7)", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		if rec := e.admin(t, "DELETE", "/v1/routes/ghost", "", "If-Match", `"9"`); rec.Code != 412 {
			t.Fatalf("stale: status %d, want 412", rec.Code)
		}
		if rec := e.admin(t, "DELETE", "/v1/routes/ghost", "", "If-Match", `"1"`); rec.Code != 404 {
			t.Fatalf("matching: status %d, want 404", rec.Code)
		}
	})
	run(t, "request body is ignored", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		if rec := e.admin(t, "DELETE", "/v1/routes/api", `garbage {`); rec.Code != 204 {
			t.Fatalf("status %d", rec.Code)
		}
	})
}

// ---- upstreams ------------------------------------------------------------------

func TestUpstreams_ListAndGet(t *testing.T) {
	defer failOnPanic(t)
	run(t, "empty store lists []", func(t *testing.T) {
		e := newEnv(t)
		rec := e.admin(t, "GET", "/v1/upstreams", "")
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"upstreams":[]`) || rec.Header().Get("ETag") != `"0"` {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
	})
	run(t, "sorted by name", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		e.putUpstream(t, "zz")
		e.putUpstream(t, "aa")
		rec := e.admin(t, "GET", "/v1/upstreams", "")
		var ul UpstreamList
		mustJSON(t, rec, &ul)
		var names []string
		for _, u := range ul.Upstreams {
			names = append(names, u.Name)
		}
		if fmt.Sprint(names) != "[aa web zz]" || rec.Header().Get("ETag") != `"3"` {
			t.Fatalf("names %v etag %q", names, rec.Header().Get("ETag"))
		}
	})
	run(t, "get one is normalized", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "GET", "/v1/upstreams/web", "")
		var u config.Upstream
		mustJSON(t, rec, &u)
		if rec.Code != 200 || u.Name != "web" || u.Algorithm != config.AlgorithmRoundRobin || rec.Header().Get("ETag") != `"1"` {
			t.Fatalf("%d %+v", rec.Code, u)
		}
	})
	run(t, "get missing is 404", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "GET", "/v1/upstreams/ghost", "")
		if rec.Code != 404 {
			t.Fatalf("status %d", rec.Code)
		}
		errBody(t, rec)
	})
}

func TestUpstreams_Put(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name       string
		path       string
		body       string
		wantStatus int
		wantName   string
		wantMsg    string
	}{
		{"create", "/v1/upstreams/api2", upstreamJSON("api2"), 201, "api2", ""},
		{"replace existing", "/v1/upstreams/web", `{"name":"web","algorithm":"least_conn","backends":["http://127.0.0.1:9002"]}`, 200, "web", ""},
		{"empty body name takes the path name", "/v1/upstreams/api2", `{"backends":["` + seedBackend + `"]}`, 201, "api2", ""},
		{"name mismatch", "/v1/upstreams/api2", upstreamJSON("other"), 400, "", "name in body does not match path"},
		{"neither backends nor consul", "/v1/upstreams/api2", `{"name":"api2"}`, 400, "", "upstreams["},
		{"both backends and consul", "/v1/upstreams/api2", `{"name":"api2","backends":["` + seedBackend + `"],"consul":{"service":"x"}}`, 400, "", "upstreams["},
		{"bad backend url", "/v1/upstreams/api2", `{"name":"api2","backends":["not a url"]}`, 400, "", "upstreams["},
		{"unknown field", "/v1/upstreams/api2", `{"backends":["` + seedBackend + `"],"bogus":true}`, 400, "", "bogus"},
		{"malformed", "/v1/upstreams/api2", `[`, 400, "", ""},
		{"empty body", "/v1/upstreams/api2", ``, 400, "", ""},
		{"too large", "/v1/upstreams/api2", oversizeBody(upstreamJSON("api2")), 413, "", "request body too large"},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			recvWithin(t, sub, 1e9)

			rec := e.admin(t, "PUT", tc.path, tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus >= 400 {
				msg := errBody(t, rec)
				if !strings.Contains(msg, tc.wantMsg) {
					t.Fatalf("error %q, want it to mention %q", msg, tc.wantMsg)
				}
				if tc.wantMsg == "name in body does not match path" && msg != tc.wantMsg {
					t.Fatalf("error %q, want exactly %q", msg, tc.wantMsg)
				}
				if v := e.headVersion(t); v != 1 {
					t.Fatalf("failed write moved head to %d", v)
				}
				if c, ok := tryRecv(sub); ok {
					t.Fatalf("failed write published v%d", c.Version)
				}
				return
			}
			var u config.Upstream
			mustJSON(t, rec, &u)
			if u.Name != tc.wantName || u.Algorithm == "" {
				t.Fatalf("response %+v, want name %q and normalized algorithm", u, tc.wantName)
			}
			if etagVersion(t, rec) != 2 {
				t.Fatalf("ETag %q", rec.Header().Get("ETag"))
			}
			loc := rec.Header().Get("Location")
			if tc.wantStatus == 201 && loc != "/v1/upstreams/"+tc.wantName {
				t.Fatalf("Location = %q", loc)
			}
			if tc.wantStatus == 200 && loc != "" {
				t.Fatalf("Location = %q on a replace", loc)
			}
			if pub, ok := tryRecv(sub); !ok || pub.Version != 2 {
				t.Fatalf("hub got %v, want v2", pub)
			}
		})
	}
}

func TestUpstreams_ReplaceKeepsPosition(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	e.seed(t) // web
	e.putUpstream(t, "zz")
	e.putUpstream(t, "aa")
	e.admin(t, "PUT", "/v1/upstreams/zz", `{"backends":["http://127.0.0.1:9003"]}`)
	c := decodeConfig(t, e.admin(t, "GET", "/v1/config", ""))
	var names []string
	for _, u := range c.Upstreams {
		names = append(names, u.Name)
	}
	if fmt.Sprint(names) != "[web zz aa]" || c.Upstreams[1].Backends[0] != "http://127.0.0.1:9003" {
		t.Fatalf("upstreams %v", names)
	}
}

func TestUpstreams_Delete(t *testing.T) {
	defer failOnPanic(t)
	run(t, "referenced by a route is 400 and writes nothing", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		sub, cancel := e.hub.Subscribe()
		defer cancel()
		recvWithin(t, sub, 1e9)
		rec := e.admin(t, "DELETE", "/v1/upstreams/web", "")
		if rec.Code != 400 {
			t.Fatalf("status %d, want 400; %s", rec.Code, rec.Body)
		}
		msg := errBody(t, rec)
		// The validation error points at the referencing route and the upstream.
		if !strings.Contains(msg, "routes[0].upstream") || !strings.Contains(msg, "web") {
			t.Fatalf("error %q does not name the dangling reference", msg)
		}
		if v := e.headVersion(t); v != 1 {
			t.Fatalf("head %d", v)
		}
		if c, ok := tryRecv(sub); ok {
			t.Fatalf("published v%d", c.Version)
		}
		if rec := e.admin(t, "GET", "/v1/upstreams/web", ""); rec.Code != 200 {
			t.Fatalf("upstream gone: %d", rec.Code)
		}
	})
	run(t, "route first, then upstream", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		if rec := e.admin(t, "DELETE", "/v1/routes/api", ""); rec.Code != 204 {
			t.Fatalf("delete route: %d", rec.Code)
		}
		rec := e.admin(t, "DELETE", "/v1/upstreams/web", "")
		if rec.Code != 204 || rec.Body.Len() != 0 || rec.Header().Get("ETag") != `"3"` {
			t.Fatalf("status %d body %q etag %q", rec.Code, rec.Body, rec.Header().Get("ETag"))
		}
	})
	run(t, "unreferenced is 204 and published", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		e.putUpstream(t, "spare")
		sub, cancel := e.hub.Subscribe()
		defer cancel()
		recvWithin(t, sub, 1e9)
		rec := e.admin(t, "DELETE", "/v1/upstreams/spare", "")
		if rec.Code != 204 || rec.Header().Get("ETag") != `"3"` {
			t.Fatalf("status %d etag %q", rec.Code, rec.Header().Get("ETag"))
		}
		if pub, ok := tryRecv(sub); !ok || pub.Version != 3 {
			t.Fatalf("hub got %v", pub)
		}
	})
	run(t, "missing is 404", func(t *testing.T) {
		e := newEnv(t)
		e.seed(t)
		rec := e.admin(t, "DELETE", "/v1/upstreams/ghost", "")
		if rec.Code != 404 {
			t.Fatalf("status %d", rec.Code)
		}
		errBody(t, rec)
	})
}

// ---- version bookkeeping across a mixed session ------------------------------------

func TestEveryWriteBumpsVersionByExactlyOne(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	steps := []struct {
		name         string
		method, path string
		body         string
		wantStatus   int
	}{
		{"seed", "PUT", "/v1/config", seedJSON, 200},
		{"create upstream", "PUT", "/v1/upstreams/b", upstreamJSON("b"), 201},
		{"create route", "PUT", "/v1/routes/rb", routeJSON("rb", "/b", "b"), 201},
		{"replace route unchanged", "PUT", "/v1/routes/rb", routeJSON("rb", "/b", "b"), 200},
		{"replace upstream unchanged", "PUT", "/v1/upstreams/b", upstreamJSON("b"), 200},
		{"delete route", "DELETE", "/v1/routes/rb", "", 204},
		{"delete upstream", "DELETE", "/v1/upstreams/b", "", 204},
		{"rollback", "POST", "/v1/config/rollback", `{"to_version":3}`, 200},
		{"put config", "PUT", "/v1/config", seedJSON, 200},
	}
	for i, s := range steps {
		rec := e.admin(t, s.method, s.path, s.body)
		if rec.Code != s.wantStatus {
			t.Fatalf("step %q: status %d, want %d; %s", s.name, rec.Code, s.wantStatus, rec.Body)
		}
		if got := etagVersion(t, rec); got != int64(i+1) {
			t.Fatalf("step %q: ETag version %d, want %d", s.name, got, i+1)
		}
		if got := e.headVersion(t); got != int64(i+1) {
			t.Fatalf("step %q: head %d, want %d", s.name, got, i+1)
		}
	}
	// A failed write in the middle does not consume a number.
	if rec := e.admin(t, "PUT", "/v1/routes/bad", routeJSON("bad", "/x", "ghost")); rec.Code != 400 {
		t.Fatalf("status %d", rec.Code)
	}
	if rec := e.admin(t, "PUT", "/v1/upstreams/c", upstreamJSON("c")); etagVersion(t, rec) != int64(len(steps)+1) {
		t.Fatalf("version after a failed write: %q", rec.Header().Get("ETag"))
	}
}

// ---- hub publication -----------------------------------------------------------------

func TestWritesPublishToHub(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name         string
		method, path string
		body         string
		hdr          []string
		wantStatus   int
		wantPublish  bool
	}{
		{"PUT config", "PUT", "/v1/config", seedJSON, nil, 200, true},
		{"PUT route create", "PUT", "/v1/routes/b", routeJSON("b", "/b", "web"), nil, 201, true},
		{"PUT route replace", "PUT", "/v1/routes/api", routeJSON("api", "/api", "web"), nil, 200, true},
		{"PUT upstream", "PUT", "/v1/upstreams/z", upstreamJSON("z"), nil, 201, true},
		{"DELETE route", "DELETE", "/v1/routes/api", "", nil, 204, true},
		{"DELETE upstream", "DELETE", "/v1/upstreams/spare", "", nil, 204, true},
		{"rollback", "POST", "/v1/config/rollback", `{"to_version":1}`, nil, 200, true},
		{"failed: invalid config", "PUT", "/v1/config", `{"routes":[{"name":"r","path_prefix":"/r","upstream":"x"}]}`, nil, 400, false},
		{"failed: stale If-Match", "PUT", "/v1/config", seedJSON, []string{"If-Match", `"1"`}, 412, false},
		{"failed: malformed If-Match", "PUT", "/v1/config", seedJSON, []string{"If-Match", `*`}, 400, false},
		{"failed: delete missing route", "DELETE", "/v1/routes/ghost", "", nil, 404, false},
		{"failed: delete referenced upstream", "DELETE", "/v1/upstreams/web", "", nil, 400, false},
		{"failed: rollback missing version", "POST", "/v1/config/rollback", `{"to_version":50}`, nil, 404, false},
		{"failed: unauthorized", "PUT", "/v1/config", seedJSON, []string{"Authorization", "Bearer nope"}, 401, false},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			e.putUpstream(t, "spare") // head 2
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			if got := recvWithin(t, sub, 1e9); got.Version != 2 {
				t.Fatalf("primed with v%d", got.Version)
			}
			var rec *httptest.ResponseRecorder
			if tc.wantStatus == 401 {
				rec = do(t, e.h, tc.method, tc.path, "", tc.body, tc.hdr...)
			} else {
				rec = e.admin(t, tc.method, tc.path, tc.body, tc.hdr...)
			}
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			// Publish happens BEFORE the response is written (rule 10), so the
			// value must already be in the slot when the handler has returned.
			pub, ok := tryRecv(sub)
			if tc.wantPublish {
				if !ok || pub.Version != 3 || pub.Version != etagVersion(t, rec) {
					t.Fatalf("hub got %v, want v3 matching the response ETag", pub)
				}
				if h := e.headVersion(t); h != 3 {
					t.Fatalf("head %d", h)
				}
				return
			}
			if ok {
				t.Fatalf("failed write published v%d", pub.Version)
			}
		})
	}
}

// ---- store failures ----------------------------------------------------------------------

func TestStoreFailuresAre500WithoutDetails(t *testing.T) {
	defer failOnPanic(t)
	const secret = "pq: password authentication failed for user secretuser at 10.1.2.3"
	eps := []endpoint{
		{"GET", "/v1/config", ""},
		{"PUT", "/v1/config", seedJSON},
		{"GET", "/v1/config/versions", ""},
		{"GET", "/v1/config/versions/1", ""},
		{"POST", "/v1/config/rollback", `{}`},
		{"GET", "/v1/routes", ""},
		{"GET", "/v1/routes/api", ""},
		{"PUT", "/v1/routes/api", routeJSON("api", "/api", "web")},
		{"DELETE", "/v1/routes/api", ""},
		{"GET", "/v1/upstreams", ""},
		{"GET", "/v1/upstreams/web", ""},
		{"PUT", "/v1/upstreams/web", upstreamJSON("web")},
		{"DELETE", "/v1/upstreams/web", ""},
	}
	for _, ep := range eps {
		run(t, ep.method+" "+ep.path, func(t *testing.T) {
			fs := &faultyStore{Store: NewMemStore(), err: fmt.Errorf("%s", secret)}
			e := newEnvWithStore(t, fs)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			rec := e.admin(t, ep.method, ep.path, ep.body)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status %d, want 500; %s", rec.Code, rec.Body)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"internal error"}` {
				t.Fatalf("body = %s", got)
			}
			if c, ok := tryRecv(sub); ok {
				t.Fatalf("published v%d after a store failure", c.Version)
			}
		})
	}
}

func TestStoreTimeoutIsEnforced(t *testing.T) {
	defer failOnPanic(t)
	fs := &faultyStore{Store: NewMemStore(), block: true}
	e := newEnvWithStore(t, fs, func(o *Options) { o.StoreTimeout = 50e6 }) // 50ms
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- e.admin(t, "GET", "/v1/config", "") }()
	select {
	case rec := <-done:
		if rec.Code != 500 {
			t.Fatalf("status %d, want 500 (anything but a typed error is 500)", rec.Code)
		}
	case <-after(3):
		t.Fatal("handler did not give up on a hung store within StoreTimeout")
	}
}
