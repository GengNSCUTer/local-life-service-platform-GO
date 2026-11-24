package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/config"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/media"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/observe"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/shop"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/social"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/voucher"
)

// Run keeps the original single-process mode for local regression tests.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return runAPI(ctx, cfg, logger, true)
}

// RunAPI starts only the HTTP process. Background workers are owned by RunWorker.
func RunAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return runAPI(ctx, cfg, logger, false)
}

func runAPI(ctx context.Context, cfg config.Config, logger *slog.Logger, startWorkers bool) (runErr error) {
	deps, err := openClients(cfg, startWorkers)
	if err != nil {
		return err
	}
	defer func() {
		if err := deps.close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close clients: %w", err))
		}
	}()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	defer listener.Close()
	registry := diagnosticsRegistry(deps)
	stopDiagnostics, err := startDiagnostics(cfg.ObservabilityAddr, cfg.PprofEnabled, registry, logger)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, stopDiagnostics()) }()

	service := deps.seckillService(cfg, logger)
	service.RegisterMetrics(registry, startWorkers)
	router := newRouter(logger, deps.probes(), observe.HTTP(registry))
	users := user.New(deps.mysql, deps.redis, logger, cfg.AuthDevLogCodes)
	users.Register(router)
	requireLogin := users.RequireLogin()
	images, err := media.New(cfg.MediaEndpoint, cfg.MediaAccessKey, cfg.MediaSecretKey, cfg.MediaBucket, cfg.MediaPublicURL)
	if err != nil {
		return err
	}
	images.Register(router, requireLogin, logger)
	service.Register(router, requireLogin)
	shop.New(deps.mysql, deps.redis, logger, cfg.ShopAdminUserID).Register(router, requireLogin, users.VisitorLogin)
	voucher.New(deps.mysql, logger, cfg.VoucherAdminUserID, service).Register(router, requireLogin)
	community := social.New(deps.mysql, deps.redis, logger)
	community.Register(router, requireLogin, users.OptionalLogin())
	if startWorkers {
		workerCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
		done := startBackground(workerCtx, cfg, deps, service, community, func() {})
		defer func() {
			stop()
			runErr = errors.Join(runErr, waitBackground(done, backgroundLoops))
		}()
	}
	idleTimeout := cfg.HTTPIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = time.Minute
	}
	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       idleTimeout,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	logger.Info("http server listening", "addr", listener.Addr().String())

	select {
	case err := <-serveDone:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		logger.Info("http server draining")
	}

	// Give active requests a separate deadline after the signal cancels ctx.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return errors.Join(fmt.Errorf("drain HTTP: %w", err), server.Close())
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	logger.Info("http server stopped")
	return nil
}

func PrepareSeckill(ctx context.Context, cfg config.Config, logger *slog.Logger, activity seckill.Activity) (prepareErr error) {
	deps, err := openClients(cfg, true)
	if err != nil {
		return err
	}
	defer func() { prepareErr = errors.Join(prepareErr, deps.close()) }()
	if err := seckill.CreateTopics(ctx, deps.kafka); err != nil {
		return fmt.Errorf("prepare Kafka topics: %w", err)
	}
	return deps.seckillService(cfg, logger).Prepare(ctx, activity)
}

func AuditSeckill(ctx context.Context, cfg config.Config, logger *slog.Logger, voucherID uint64) (_ seckill.InventoryAudit, auditErr error) {
	deps, err := openClients(cfg, false)
	if err != nil {
		return seckill.InventoryAudit{}, err
	}
	defer func() { auditErr = errors.Join(auditErr, deps.close()) }()
	return deps.seckillService(cfg, logger).AuditInventory(ctx, voucherID)
}
