package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

var accessKeys = []string{
	"request_id", "method", "host", "path", "route", "upstream", "backend",
	"client_id", "remote_ip", "status", "duration_ms", "bytes_out", "config_version",
}

func newJSONLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func sampleRecord() AccessRecord {
	return AccessRecord{
		RequestID: "req-123", Method: "GET", Host: "api.example.com", Path: "/v1/items",
		Route: "items", Upstream: "svc", Backend: "http://10.0.0.1:8080", ClientID: "acme",
		RemoteIP: "203.0.113.7", Status: 200, Duration: 1500 * time.Microsecond,
		BytesOut: 2048, ConfigVersion: 17,
	}
}

func decodeOne(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("want exactly one log line, got %d: %q", len(lines), buf.String())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("not JSON: %v: %q", err, lines[0])
	}
	return m
}

func TestLogAccessFields(t *testing.T) {
	defer guard(t)
	var buf bytes.Buffer
	LogAccess(context.Background(), newJSONLogger(&buf), sampleRecord())
	m := decodeOne(t, &buf)

	if m["msg"] != "request" {
		t.Errorf("msg = %v", m["msg"])
	}
	var got []string
	for k := range m {
		switch k {
		case "time", "level", "msg":
		default:
			got = append(got, k)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), accessKeys...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v\nwant   %v", got, want)
	}

	strs := map[string]string{
		"request_id": "req-123", "method": "GET", "host": "api.example.com", "path": "/v1/items",
		"route": "items", "upstream": "svc", "backend": "http://10.0.0.1:8080",
		"client_id": "acme", "remote_ip": "203.0.113.7",
	}
	for k, v := range strs {
		if m[k] != v {
			t.Errorf("%s = %v, want %q", k, m[k], v)
		}
	}
	nums := map[string]float64{"status": 200, "bytes_out": 2048, "config_version": 17, "duration_ms": 1.5}
	for k, v := range nums {
		f, ok := m[k].(float64)
		if !ok {
			t.Errorf("%s is %T, want JSON number", k, m[k])
			continue
		}
		if f != v {
			t.Errorf("%s = %v, want %v", k, f, v)
		}
	}
}

func TestLogAccessDurationMs(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want float64
	}{
		{0, 0},
		{250 * time.Microsecond, 0.25},
		{time.Millisecond, 1},
		{2*time.Second + 500*time.Millisecond, 2500},
	}
	for _, tc := range tests {
		t.Run(tc.d.String(), func(t *testing.T) {
			defer guard(t)
			var buf bytes.Buffer
			rec := sampleRecord()
			rec.Duration = tc.d
			LogAccess(context.Background(), newJSONLogger(&buf), rec)
			got, _ := decodeOne(t, &buf)["duration_ms"].(float64)
			if got < tc.want-1e-9 || got > tc.want+1e-9 {
				t.Errorf("duration_ms = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLogAccessLevel(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{200, "INFO"}, {302, "INFO"}, {404, "INFO"}, {429, "INFO"},
		{499, "INFO"}, {500, "WARN"}, {502, "WARN"}, {503, "WARN"}, {504, "WARN"}, {599, "WARN"},
	}
	for _, tc := range tests {
		t.Run(string(rune('0'+tc.status/100))+"xx_"+itoa(tc.status), func(t *testing.T) {
			defer guard(t)
			var buf bytes.Buffer
			rec := sampleRecord()
			rec.Status = tc.status
			LogAccess(context.Background(), newJSONLogger(&buf), rec)
			if lvl := decodeOne(t, &buf)["level"]; lvl != tc.want {
				t.Errorf("status %d level = %v, want %s", tc.status, lvl, tc.want)
			}
		})
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestLogAccessNeverAddsQueryString(t *testing.T) {
	defer guard(t)
	var buf bytes.Buffer
	rec := sampleRecord()
	rec.Path = "/search"
	LogAccess(context.Background(), newJSONLogger(&buf), rec)
	m := decodeOne(t, &buf)
	if m["path"] != "/search" {
		t.Errorf("path = %v", m["path"])
	}
	for k, v := range m {
		if s, ok := v.(string); ok && strings.Contains(s, "?") {
			t.Errorf("%s contains '?': %q", k, s)
		}
	}
}

func TestLogAccessOneLinePerCall(t *testing.T) {
	defer guard(t)
	var buf bytes.Buffer
	l := newJSONLogger(&buf)
	LogAccess(context.Background(), l, sampleRecord())
	LogAccess(context.Background(), l, sampleRecord())
	if n := len(strings.Split(strings.TrimSpace(buf.String()), "\n")); n != 2 {
		t.Errorf("lines = %d, want 2", n)
	}
}

func TestLogAccessNilLogger(t *testing.T) {
	defer guard(t)
	LogAccess(context.Background(), nil, sampleRecord()) // must not panic
}

// The record type has no field that could carry a credential.
func TestAccessRecordHasNoSecretFields(t *testing.T) {
	typ := reflect.TypeOf(AccessRecord{})
	allowed := map[string]bool{
		"RequestID": true, "Method": true, "Host": true, "Path": true, "Route": true,
		"Upstream": true, "Backend": true, "ClientID": true, "RemoteIP": true,
		"Status": true, "Duration": true, "BytesOut": true, "ConfigVersion": true,
	}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !allowed[name] {
			t.Errorf("unexpected AccessRecord field %q", name)
		}
		l := strings.ToLower(name)
		for _, bad := range []string{"key", "auth", "token", "secret", "header", "query", "cookie"} {
			if strings.Contains(l, bad) {
				t.Errorf("field %q looks credential-bearing", name)
			}
		}
	}
	if typ.NumField() != len(allowed) {
		t.Errorf("AccessRecord has %d fields, want %d", typ.NumField(), len(allowed))
	}
}
