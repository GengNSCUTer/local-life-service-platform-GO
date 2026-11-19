package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/app"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/config"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], logger); err != nil && !errors.Is(err, flag.ErrHelp) {
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	cfg := config.Load()
	if len(args) == 0 {
		return app.Run(ctx, cfg, logger)
	}
	if args[0] == "audit-seckill" {
		flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		id := flags.Uint64("voucher-id", 0, "positive activity ID")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *id == 0 || flags.NArg() != 0 {
			return errors.New("provide a positive voucher ID")
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		audit, err := app.AuditSeckill(ctx, cfg, logger, *id)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(audit)
	}
	if args[0] != "prepare-seckill" {
		return fmt.Errorf("unknown command %q", args[0])
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	id := flags.Uint64("voucher-id", 0, "positive activity ID")
	stock := flags.Uint64("stock", 0, "initial stock")
	start := flags.String("starts-at", "", "start time (RFC3339)")
	end := flags.String("ends-at", "", "end time (RFC3339)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	startsAt, startErr := time.Parse(time.RFC3339, *start)
	endsAt, endErr := time.Parse(time.RFC3339, *end)
	if startErr != nil || endErr != nil || !endsAt.After(startsAt) || *id == 0 || *stock == 0 || *stock > 1<<32-1 || flags.NArg() != 0 {
		return errors.New("provide a positive voucher ID, stock between 1 and 4294967295, and a valid RFC3339 time range")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := app.PrepareSeckill(ctx, cfg, logger, seckill.Activity{
		ID: *id, TotalStock: uint32(*stock), StartsAt: startsAt, EndsAt: endsAt,
	}); err != nil {
		return err
	}
	logger.Info("seckill activity prepared", "voucher_id", *id)
	return nil
}
