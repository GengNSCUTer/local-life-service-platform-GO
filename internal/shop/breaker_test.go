package shop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestShopBreakerSingleProbe(t *testing.T) {
	s := &Service{}
	s.EnableBreaker(prometheus.NewRegistry())
	b := s.breaker
	now := time.Now()
	failure := errors.New("MySQL unavailable")
	for range shopBreakerThreshold {
		generation, allowed := b.allow(now)
		if !allowed {
			t.Fatal("opened before reaching the threshold")
		}
		b.finish(generation, failure, nil, now)
	}
	if _, allowed := b.allow(now); allowed {
		t.Fatal("open circuit accepted a request")
	}
	now = now.Add(5 * time.Second)
	generation, allowed := b.allow(now)
	if !allowed {
		t.Fatal("half-open circuit refused the probe")
	}
	if _, allowed := b.allow(now); allowed {
		t.Fatal("half-open circuit accepted a second probe")
	}
	b.finish(generation, context.Canceled, context.Canceled, now)
	generation, allowed = b.allow(now)
	if !allowed {
		t.Fatal("canceled probe blocked recovery")
	}
	b.finish(generation, nil, nil, now)
	if b.probing || !b.openUntil.IsZero() || b.failures != 0 {
		t.Fatal("successful probe did not close the circuit")
	}
}
