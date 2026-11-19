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
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/shop"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/voucher"
)

// Run owns the HTTP server and shared clients for the lifetime of the process.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) (runErr error) {
	deps, err := openClients(cfg)
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
	service := seckill.New(deps.mysql, deps.redis, logger)
	router := newRouter(logger, deps.probes())
	users := user.New(deps.mysql, deps.redis, logger, cfg.AuthDevLogCodes)
	users.Register(router)
	requireLogin := users.RequireLogin()
	service.Register(router, requireLogin)
	shop.New(deps.mysql, deps.redis, logger, cfg.ShopAdminUserID).Register(router, requireLogin)
	voucher.New(deps.mysql, logger, cfg.VoucherAdminUserID, service).Register(router, requireLogin)
	workerCtx, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	workersDone := make(chan error, 1)
	go func() {
		workersDone <- service.RunWorkers(workerCtx, cfg.KafkaBrokers, deps.kafka, deps.kafkaTransport,
			cfg.KafkaRelayWorkers, cfg.KafkaConsumerWorkers)
	}()
	defer func() {
		stopWorkers()
		if err := <-workersDone; err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close seckill workers: %w", err))
		}
	}()
	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
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
	deps, err := openClients(cfg)
	if err != nil {
		return err
	}
	defer func() { prepareErr = errors.Join(prepareErr, deps.close()) }()
	if err := seckill.CreateTopics(ctx, deps.kafka); err != nil {
		return fmt.Errorf("prepare Kafka topics: %w", err)
	}
	return seckill.New(deps.mysql, deps.redis, logger).Prepare(ctx, activity)
}
