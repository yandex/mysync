package dcs

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRandomHostProviderNextEmptyResolved(t *testing.T) {
	logger := zerolog.Nop()
	rhp := NewRandomHostProvider(context.Background(), &RandomHostProviderConfig{
		LookupTTL:                time.Minute,
		ConnectivityCheckTimeout: time.Second,
		LookupTimeout:            time.Second,
		LookupTickInterval:       time.Minute,
		RetryJitter:              0,
	}, true, &logger)

	rhp.hostsKeys = []string{"a:2181", "b:2181"}
	rhp.hosts.Store("a:2181", zkhost{})
	rhp.hosts.Store("b:2181", zkhost{})

	done := make(chan struct{})
	var server string
	var retry bool
	go func() {
		server, retry = rhp.Next()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Next hung when no host resolves")
	}

	if server != "a:2181" && server != "b:2181" {
		t.Fatalf("got server %q, want one of the configured hosts", server)
	}
	if !retry {
		t.Fatalf("expected retryStart true after exhausting unresolved hosts")
	}
}
