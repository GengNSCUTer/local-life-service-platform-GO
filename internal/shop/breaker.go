package shop

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

var errShopCircuitOpen = errors.New("shop database circuit is open")

const shopBreakerThreshold = 5

// The generation keeps older in-flight requests from changing a newer state.
type shopBreaker struct {
	mu         sync.Mutex
	state      prometheus.Gauge
	failures   int
	openUntil  time.Time
	probing    bool
	generation uint64
}

func (s *Service) EnableBreaker(reg prometheus.Registerer) {
	state := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hmdp_shop_mysql_circuit_state",
		Help: "Shop fallback circuit: 0 closed, 1 open, 2 half open.",
	})
	reg.MustRegister(state)
	s.breaker = &shopBreaker{state: state}
}

func (b *shopBreaker) find(ctx context.Context, repo repository, id uint64) (shopRecord, error) {
	if b == nil {
		return repo.find(ctx, id)
	}
	generation, allowed := b.allow(time.Now())
	if !allowed {
		return shopRecord{}, errShopCircuitOpen
	}
	operation, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	item, err := repo.find(operation, id)
	cancel()
	b.finish(generation, err, ctx.Err(), time.Now())
	return item, err
}

func (b *shopBreaker) allow(now time.Time) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.IsZero() {
		return b.generation, true
	}
	if now.Before(b.openUntil) || b.probing {
		return b.generation, false
	}
	b.probing = true
	b.state.Set(2)
	return b.generation, true
}

func (b *shopBreaker) finish(generation uint64, err, parentErr error, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.generation != generation {
		return
	}
	if parentErr != nil || errors.Is(err, context.Canceled) {
		if b.probing {
			b.generation++
			b.probing = false
			b.state.Set(1)
		}
		return
	}
	if err == nil || errors.Is(err, gorm.ErrRecordNotFound) {
		b.failures = 0
		if b.probing {
			b.generation++
			b.probing = false
			b.openUntil = time.Time{}
			b.state.Set(0)
		}
		return
	}
	b.failures++
	if b.probing || b.failures >= shopBreakerThreshold {
		b.generation++
		b.probing = false
		b.openUntil = now.Add(5 * time.Second)
		b.state.Set(1)
	}
}
