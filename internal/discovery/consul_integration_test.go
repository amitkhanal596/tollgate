package discovery

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// Runs only when TOLLGATE_TEST_CONSUL_ADDR (e.g. 127.0.0.1:18500) is set.
// It never contacts any Consul otherwise.
func TestConsulIntegration_RealAgent(t *testing.T) {
	addr := os.Getenv("TOLLGATE_TEST_CONSUL_ADDR")
	if addr == "" {
		t.Skip("TOLLGATE_TEST_CONSUL_ADDR not set")
	}
	safely(t, func() {
		client, err := consulapi.NewClient(&consulapi.Config{Address: addr})
		if err != nil {
			t.Fatal(err)
		}
		svc := fmt.Sprintf("tollgate-it-%d", time.Now().UnixNano())
		agent := client.Agent()
		register := func(id string, port int) {
			err := agent.ServiceRegister(&consulapi.AgentServiceRegistration{
				ID: id, Name: svc, Address: "127.0.0.1", Port: port,
				Check: &consulapi.AgentServiceCheck{CheckID: "chk-" + id, TTL: "5m", Status: consulapi.HealthPassing},
			})
			if err != nil {
				t.Fatalf("register %s: %v", id, err)
			}
			t.Cleanup(func() { _ = agent.ServiceDeregister(id) })
		}
		register(svc+"-1", 9001)
		register(svc+"-2", 9002)

		c := NewConsul(client, ConsulOptions{WaitTime: 2 * time.Second})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := c.Watch(ctx, Query{Service: svc})
		if err != nil {
			t.Fatal(err)
		}
		next := func(wantLen int) []Endpoint {
			t.Helper()
			deadline := time.After(15 * time.Second)
			for {
				select {
				case v, ok := <-ch:
					if !ok {
						t.Fatal("channel closed")
					}
					if len(v) == wantLen {
						return v
					}
				case <-deadline:
					t.Fatalf("no set of len %d within deadline", wantLen)
				}
			}
		}

		got := next(2)
		if !strings.HasSuffix(got[0].ID, "/"+svc+"-1") || got[0].Address != "127.0.0.1:9001" {
			t.Fatalf("unexpected endpoint %+v", got[0])
		}
		if err := agent.ServiceDeregister(svc + "-1"); err != nil {
			t.Fatal(err)
		}
		got = next(1)
		if got[0].Address != "127.0.0.1:9002" {
			t.Fatalf("unexpected survivor %+v", got[0])
		}
		if err := agent.UpdateTTL("chk-"+svc+"-2", "forced critical", consulapi.HealthCritical); err != nil {
			t.Fatal(err)
		}
		next(0)
	})
}
