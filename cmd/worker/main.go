package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/app"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.RunWorker(ctx, config.Load(), logger); err != nil {
		logger.Error("worker failed", "error", err)
		os.Exit(1)
	}
}
