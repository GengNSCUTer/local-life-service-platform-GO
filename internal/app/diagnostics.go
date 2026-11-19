package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func diagnosticsRegistry(deps *clients) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	pool, err := deps.mysql.DB()
	if err == nil {
		registry.MustRegister(collectors.NewDBStatsCollector(pool, "hmdp"))
	}
	registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "hmdp_redis_pool_timeouts_total", Help: "Redis connection pool timeouts.",
	}, func() float64 { return float64(deps.redis.PoolStats().Timeouts) }))
	return registry
}

func startDiagnostics(addr string, profiling bool, registry *prometheus.Registry, logger *slog.Logger) (func() error, error) {
	if addr == "" {
		return func() error { return nil }, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("OBSERVABILITY_ADDR requires a loopback IP and port, e.g. 127.0.0.1:9091")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for diagnostics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Timeout: 5 * time.Second}))
	if profiling {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 45 * time.Second}
	go func() {
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("diagnostics server stopped", "error", err)
		}
	}()
	logger.Info("diagnostics server listening", "addr", listener.Addr().String(), "pprof", profiling)
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}, nil
}
