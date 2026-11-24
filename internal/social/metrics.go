package social

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	cache         *prometheus.CounterVec
	fanout        *prometheus.CounterVec
	counterLag    prometheus.Histogram
	writeRejected prometheus.Counter
}

func (s *Service) RegisterMetrics(reg prometheus.Registerer, worker bool) {
	s.metrics = &metrics{
		cache:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hmdp_social_detail_cache_total", Help: "Blog detail cache outcomes."}, []string{"outcome"}),
		fanout:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hmdp_social_fanout_batches_total", Help: "Social fanout batch outcomes."}, []string{"outcome"}),
		counterLag:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "hmdp_social_counter_projection_delay_seconds", Help: "Age of the oldest pending delta when the blog projection commits.", Buckets: []float64{.1, .25, .5, 1, 2, 3, 5, 10, 30, 60}}),
		writeRejected: prometheus.NewCounter(prometheus.CounterOpts{Name: "hmdp_social_write_overload_total", Help: "Social writes refused by the API concurrency limit."}),
	}
	reg.MustRegister(s.metrics.cache, s.metrics.fanout, s.metrics.counterLag, s.metrics.writeRejected)
	if worker {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "hmdp_social_fanout_pending_tasks", Help: "Durable social fanout tasks still due, or -1 when sampling fails."}, func() float64 {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			var count int64
			if err := s.db.WithContext(ctx).Table("social_fanout_tasks").Where("due_at IS NOT NULL").Count(&count).Error; err != nil {
				return -1
			}
			return float64(count)
		}))
	}
}

func (s *Service) cacheMetric(outcome string) {
	if s.metrics != nil {
		s.metrics.cache.WithLabelValues(outcome).Inc()
	}
}
