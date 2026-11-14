package config

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseYAMLFull(t *testing.T) {
	got, err := ParseYAML([]byte(fullYAML))
	if err != nil {
		t.Fatal(err)
	}
	want := fullConfigNormalized()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func TestParseJSONFull(t *testing.T) {
	js, err := json.Marshal(fullConfigNormalized())
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseJSON(js)
	if err != nil {
		t.Fatalf("%v\n%s", err, js)
	}
	if want := fullConfigNormalized(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func TestParseJSONNormalizes(t *testing.T) {
	in := `{"routes":[{"name":"r","host":"WWW.X.COM","path_prefix":"/","upstream":"u"}],
	        "upstreams":[{"name":"u","backends":["http://h:1"],"health_check":{"path":"/h"}}]}`
	c, err := ParseJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if c.Routes[0].Host != "www.x.com" || c.Routes[0].Auth != AuthNone ||
		c.Upstreams[0].Algorithm != AlgorithmRoundRobin ||
		c.Upstreams[0].HealthCheck.HealthyThreshold != DefaultHealthyThreshold {
		t.Fatalf("not normalized: %+v", c)
	}
}

func TestParseErrors(t *testing.T) {
	const okRoute = `{"name":"r","path_prefix":"/","upstream":"u"}`
	const okUp = `{"name":"u","backends":["http://h:1"]}`
	tests := []struct {
		name        string
		yaml, json  string
		wantInvalid bool // error must wrap ErrInvalid (as opposed to a syntax/decoding error)
	}{
		{"empty document", "", "", false},
		{"whitespace only", " \n\t\n", " \n", false},
		{"comment only", "# nothing here\n", "", false},
		{
			"unknown top-level field",
			"bogus: 1\nroutes: [{name: r, path_prefix: /, upstream: u}]\nupstreams: [{name: u, backends: ['http://h:1']}]\n",
			`{"bogus":1,"routes":[` + okRoute + `],"upstreams":[` + okUp + `]}`, false,
		},
		{
			"unknown nested field",
			"routes: [{name: r, path_prefix: /, upstream: u, ratelimit: {}}]\nupstreams: [{name: u, backends: ['http://h:1']}]\n",
			`{"routes":[{"name":"r","path_prefix":"/","upstream":"u","ratelimit":{}}],"upstreams":[` + okUp + `]}`, false,
		},
		{"malformed syntax", "routes: [unclosed\n", `{"routes":[`, false},
		{"wrong type", "version: abc\n", `{"version":"abc"}`, false},
		{"bad duration", "defaults: {timeouts: {dial: soon}}\n", `{"defaults":{"timeouts":{"dial":"soon"}}}`, false},
		{
			"semantically invalid (unknown upstream)",
			"routes: [{name: r, path_prefix: /, upstream: ghost}]\nupstreams: [{name: u, backends: ['http://h:1']}]\n",
			`{"routes":[{"name":"r","path_prefix":"/","upstream":"ghost"}],"upstreams":[` + okUp + `]}`, true,
		},
	}
	for _, tc := range tests {
		t.Run("yaml/"+tc.name, func(t *testing.T) {
			c, err := ParseYAML([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected error, got %+v", c)
			}
			if tc.wantInvalid && !errors.Is(err, ErrInvalid) {
				t.Errorf("want ErrInvalid, got %v", err)
			}
		})
		t.Run("json/"+tc.name, func(t *testing.T) {
			c, err := ParseJSON([]byte(tc.json))
			if err == nil {
				t.Fatalf("expected error, got %+v", c)
			}
			if tc.wantInvalid && !errors.Is(err, ErrInvalid) {
				t.Errorf("want ErrInvalid, got %v", err)
			}
		})
	}
}

func TestParseUnknownFieldNamedInError(t *testing.T) {
	_, err := ParseYAML([]byte("bogus_field: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "bogus_field") {
		t.Fatalf("error should name the unknown field: %v", err)
	}
}
