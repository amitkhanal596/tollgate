package auth

import "testing"

func TestClientIPKey(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"ipv4", "203.0.113.7:54321", "203.0.113.7"},
		{"ipv4 other port", "203.0.113.7:1", "203.0.113.7"},
		{"ipv6 to /64", "[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:443", "2001:db8:1:2::/64"},
		{"ipv6 same /64 different host", "[2001:db8:1:2:1:2:3:4]:80", "2001:db8:1:2::/64"},
		{"ipv6 different /64", "[2001:db8:1:3:aaaa:bbbb:cccc:dddd]:443", "2001:db8:1:3::/64"},
		{"ipv4-mapped unmapped", "[::ffff:203.0.113.7]:80", "203.0.113.7"},
		{"loopback v6", "[::1]:1234", "::/64"},
		{"unparseable returned unchanged", "not-an-address", "not-an-address"},
		{"empty returned unchanged", "", ""},
		{"bad ip with port unchanged", "999.1.1.1:80", "999.1.1.1:80"},
		{"garbage with colons unchanged", "a:b:c", "a:b:c"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClientIPKey(tc.in); got != tc.want {
				t.Fatalf("ClientIPKey(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestClientIPKeyGrouping(t *testing.T) {
	a := ClientIPKey("[2001:db8:1:2::1]:1")
	b := ClientIPKey("[2001:db8:1:2:ffff:ffff:ffff:ffff]:2")
	c := ClientIPKey("[2001:db8:1:3::1]:1")
	if a != b {
		t.Fatalf("same /64 differ: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("different /64 equal: %q", a)
	}
	if ClientIPKey("203.0.113.7:1") == ClientIPKey("203.0.113.8:1") {
		t.Fatal("distinct IPv4 addresses must differ")
	}
}
