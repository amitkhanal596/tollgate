package metrics

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestHardening_AccessLogTruncatesClientFields(t *testing.T) {
	// "é" is 2 bytes: an odd limit lands in the middle of a rune for these inputs.
	tests := []struct {
		name  string
		set   func(r *AccessRecord, v string)
		key   string
		limit int
	}{
		{"path", func(r *AccessRecord, v string) { r.Path = v }, "path", MaxLogPathBytes},
		{"host", func(r *AccessRecord, v string) { r.Host = v }, "host", MaxLogHostBytes},
		{"client_id", func(r *AccessRecord, v string) { r.ClientID = v }, "client_id", MaxLogClientIDBytes},
		{"request_id", func(r *AccessRecord, v string) { r.RequestID = v }, "request_id", MaxLogRequestIDBytes},
		{"method", func(r *AccessRecord, v string) { r.Method = v }, "method", MaxLogMethodBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inputs := map[string]string{
				"ascii_over":     strings.Repeat("a", tc.limit+500),
				"multibyte_over": strings.Repeat("é", tc.limit),                  // 2*limit bytes, cut point inside a rune boundary or not
				"split_rune":     strings.Repeat("a", tc.limit-1) + "é" + "tail", // 2-byte rune straddles the limit
			}
			for iname, in := range inputs {
				t.Run(iname, func(t *testing.T) {
					rec := sampleRecord()
					tc.set(&rec, in)
					var buf bytes.Buffer
					LogAccess(context.Background(), newJSONLogger(&buf), rec)
					m := decodeOne(t, &buf)
					got, _ := m[tc.key].(string)
					if len(got) > tc.limit {
						t.Errorf("%s logged %d bytes, want <= %d", tc.key, len(got), tc.limit)
					}
					if !utf8.ValidString(got) {
						t.Errorf("%s truncated mid-rune: invalid UTF-8", tc.key)
					}
					if !strings.HasPrefix(in, got) || got == "" {
						t.Errorf("%s must be a non-empty prefix of the input", tc.key)
					}
					if m["truncated"] != true {
						t.Errorf("truncated attr = %v, want true", m["truncated"])
					}
				})
			}
		})
	}
}

func TestHardening_AccessLogShortValuesUnchanged(t *testing.T) {
	tests := []struct {
		name string
		rec  AccessRecord
	}{
		{"sample", sampleRecord()},
		{"path_exactly_at_limit", func() AccessRecord { r := sampleRecord(); r.Path = strings.Repeat("p", MaxLogPathBytes); return r }()},
		{"host_exactly_at_limit", func() AccessRecord { r := sampleRecord(); r.Host = strings.Repeat("h", MaxLogHostBytes); return r }()},
		{"multibyte_exactly_at_limit", func() AccessRecord {
			r := sampleRecord()
			r.Path = strings.Repeat("é", MaxLogPathBytes/2)
			return r
		}()},
		{"empty_fields", AccessRecord{Status: 200}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			LogAccess(context.Background(), newJSONLogger(&buf), tc.rec)
			m := decodeOne(t, &buf)
			if m["path"] != tc.rec.Path || m["host"] != tc.rec.Host {
				t.Errorf("short values changed: path len %d->%d", len(tc.rec.Path), len(m["path"].(string)))
			}
			if _, ok := m["truncated"]; ok {
				t.Errorf("truncated attr present for a value within limits")
			}
		})
	}
}

func TestHardening_AccessLogHugePathLineSmall(t *testing.T) {
	const maxLine = 8 << 10
	tests := []struct {
		name string
		path string
	}{
		{"1MiB_ascii", strings.Repeat("a", 1<<20)},
		{"1MiB_multibyte", strings.Repeat("é", 1<<19)},
		{"1MiB_needs_json_escaping", strings.Repeat("\"", 1<<20)}, // escaping doubles size; cap must apply before encoding
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := sampleRecord()
			rec.Path, rec.Host, rec.ClientID = tc.path, tc.path, tc.path
			var buf bytes.Buffer
			LogAccess(context.Background(), newJSONLogger(&buf), rec)
			if buf.Len() >= maxLine {
				t.Errorf("log line is %d bytes, want < %d", buf.Len(), maxLine)
			}
		})
	}
}
