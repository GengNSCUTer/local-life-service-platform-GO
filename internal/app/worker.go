package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/config"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/observe"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/social"
)

const backgroundLoops = 3

// RunWorker owns the background jobs and serves only health endpoints.
func RunWorker(ctx context.Context, cfg config.Config, logger *slog.Logger) (runErr error) {
	deps, err := openClients(cfg, true)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, deps.close()) }()
	registry := diagnosticsRegistry(deps)
	stopDiagnostics, err := startDiagnostics(cfg.ObservabilityAddr, cfg.PprofEnabled, registry, logger)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, stopDiagnostics()) }()

	listener, err := net.Listen("tcp", cfg.WorkerHealthAddr)
	if err != nil {
		return fmt.Errorf("listen for worker health: %w", err)
	}
	defer listener.Close()
	var ready atomic.Int32
	var draining atomic.Bool
	probes := append(deps.probes(), healthProbe{name: "workers", check: func(context.Context) error {
		if ready.Load() != 2 || draining.Load() {
			return errors.New("worker loops are not ready")
		}
		return nil
	}})
	server := &http.Server{
		Handler: newRouter(logger, probes, observe.HTTP(registry)), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	service := seckill.New(deps.mysql, deps.redis, logger, cfg.SeckillPendingTimeout, cfg.SeckillOutboxLimit, cfg.SeckillPendingLimit)
	service.RegisterMetrics(registry, true)
	community := social.New(deps.mysql, deps.redis, logger)
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	done := startBackground(workerCtx, cfg, deps, service, community, func() { ready.Add(1) })
	logger.Info("worker health server listening", "addr", listener.Addr().String())

	remaining := backgroundLoops
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		runErr = fmt.Errorf("serve worker health: %w", err)
	case err := <-done:
		remaining--
		if err == nil {
			err = errors.New("returned before shutdown")
		}
		runErr = fmt.Errorf("background task stopped: %w", err)
	}
	draining.Store(true)
	cancel()
	runErr = errors.Join(runErr, waitBackground(done, remaining))
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, err, server.Close())
	}
	logger.Info("worker stopped")
	return runErr
}

func startBackground(ctx context.Context, cfg config.Config, deps *clients, service *seckill.Service, community *social.Service, ready func()) <-chan error {
	done := make(chan error, backgroundLoops)
	go func() { done <- community.RunWorkers(ctx, cfg.KafkaBrokers, deps.kafka, deps.kafkaTransport, ready) }()
	go func() {
		done <- service.RunWorkers(ctx, cfg.KafkaBrokers, deps.kafka, deps.kafkaTransport,
			cfg.KafkaRelayWorkers, cfg.KafkaConsumerWorkers, ready)
	}()
	go func() {
		service.RunMonitoring(ctx, deps.kafka, cfg.InventoryAuditInterval)
		done <- nil
	}()
	return done
}

func waitBackground(done <-chan error, count int) error {
	var result error
	for range count {
		if err := <-done; err != nil {
			result = errors.Join(result, fmt.Errorf("close background task: %w", err))
		}
	}
	return result
}
