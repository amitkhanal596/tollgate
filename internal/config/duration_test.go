package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		in      string // the scalar text, e.g. 2s
		want    time.Duration
		wantErr bool
	}{
		{"seconds", "2s", 2 * time.Second, false},
		{"millis", "150ms", 150 * time.Millisecond, false},
		{"compound", "1m30s", 90 * time.Second, false},
		{"zero", "0s", 0, false},
		{"garbage", "abc", 0, true},
		{"no unit", "5", 0, true},
		{"negative", "-1s", 0, true},
		{"negative millis", "-150ms", 0, true},
		{"empty", "", 0, true},
	}
	for _, tc := range tests {
		t.Run("json/"+tc.name, func(t *testing.T) {
			var d Duration
			err := json.Unmarshal([]byte(`"`+tc.in+`"`), &d)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && d.Std() != tc.want {
				t.Fatalf("got %v want %v", d.Std(), tc.want)
			}
		})
		t.Run("yaml/"+tc.name, func(t *testing.T) {
			var d Duration
			err := yaml.Unmarshal([]byte(`"`+tc.in+`"`), &d)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && d.Std() != tc.want {
				t.Fatalf("got %v want %v", d.Std(), tc.want)
			}
		})
	}
}

func TestDurationJSONRejectsNonString(t *testing.T) {
	for _, in := range []string{`5`, `true`, `{}`, `[]`} {
		var d Duration
		if err := json.Unmarshal([]byte(in), &d); err == nil {
			t.Errorf("json %s: expected error, got %v", in, d.Std())
		}
	}
}

func TestDurationMarshalIsHumanReadable(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{2 * time.Second, "2s"},
		{150 * time.Millisecond, "150ms"},
		{90 * time.Second, "1m30s"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			b, err := json.Marshal(Duration(tc.d))
			if err != nil || string(b) != `"`+tc.want+`"` {
				t.Fatalf("json = %s, %v", b, err)
			}
			y, err := yaml.Marshal(Duration(tc.d))
			if err != nil || strings.TrimSpace(string(y)) != tc.want {
				t.Fatalf("yaml = %q, %v", y, err)
			}
		})
	}
}

func TestDurationRoundTrip(t *testing.T) {
	for _, d := range []time.Duration{0, time.Nanosecond, 150 * time.Millisecond, 2 * time.Second, 90 * time.Second, 36 * time.Hour} {
		t.Run(d.String(), func(t *testing.T) {
			b, err := json.Marshal(Duration(d))
			if err != nil {
				t.Fatal(err)
			}
			var j Duration
			if err := json.Unmarshal(b, &j); err != nil || j.Std() != d {
				t.Fatalf("json round trip: %v, %v", j.Std(), err)
			}
			y, err := yaml.Marshal(Duration(d))
			if err != nil {
				t.Fatal(err)
			}
			var yd Duration
			if err := yaml.Unmarshal(y, &yd); err != nil || yd.Std() != d {
				t.Fatalf("yaml round trip: %v, %v", yd.Std(), err)
			}
		})
	}
}

func TestDurationZeroOmitted(t *testing.T) {
	b, err := json.Marshal(Timeouts{})
	if err != nil || string(b) != `{}` {
		t.Fatalf("json zero Timeouts = %s, %v", b, err)
	}
	y, err := yaml.Marshal(Timeouts{})
	if err != nil || strings.TrimSpace(string(y)) != `{}` {
		t.Fatalf("yaml zero Timeouts = %q, %v", y, err)
	}
	b, _ = json.Marshal(Timeouts{Request: Duration(time.Second)})
	if string(b) != `{"request":"1s"}` {
		t.Fatalf("json = %s", b)
	}
}

func TestConfigRoundTripYAMLToJSON(t *testing.T) {
	var fromYAML Config
	if err := yaml.Unmarshal([]byte(fullYAML), &fromYAML); err != nil {
		t.Fatal(err)
	}
	if fromYAML.Defaults.Timeouts.Request.Std() != 20*time.Second {
		t.Fatalf("yaml decode lost durations: %+v", fromYAML.Defaults)
	}
	js, err := json.Marshal(&fromYAML)
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON Config
	if err := json.Unmarshal(js, &fromJSON); err != nil {
		t.Fatalf("%v\n%s", err, js)
	}
	if !reflect.DeepEqual(fromYAML, fromJSON) {
		t.Fatalf("YAML->JSON round trip differs:\nyaml: %+v\njson: %+v", fromYAML, fromJSON)
	}

	// And back out through YAML.
	y, err := yaml.Marshal(&fromJSON)
	if err != nil {
		t.Fatal(err)
	}
	var again Config
	if err := yaml.Unmarshal(y, &again); err != nil {
		t.Fatalf("%v\n%s", err, y)
	}
	if !reflect.DeepEqual(fromYAML, again) {
		t.Fatalf("JSON->YAML round trip differs:\n%+v\n%+v", fromYAML, again)
	}
}
