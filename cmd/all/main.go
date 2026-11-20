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

// all keeps the pre-stage-7 single-process mode for regression and migration work.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, config.Load(), logger); err != nil {
		logger.Error("application failed", "error", err)
		os.Exit(1)
	}
}
