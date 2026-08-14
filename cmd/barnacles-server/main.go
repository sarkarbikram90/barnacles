// Package main is the entrypoint for the central Barnacles Server binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/server"
)

var (
	version = "0.1.0-dev"
)

func main() {
	var (
		configPath  = flag.String("config", "config/server.yaml", "Path to server configuration YAML file")
		showVersion = flag.Bool("version", false, "Print version information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("barnacles-server version %s\n", version)
		os.Exit(0)
	}

	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Initializing Barnacles Server", "version", version, "config", *configPath)

	cfg, err := config.LoadServerConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load server configuration", "error", err)
		os.Exit(1)
	}

	serverMetrics := metrics.NewServerMetrics()
	srv, err := server.New(cfg, serverMetrics)
	if err != nil {
		slog.Error("Failed to initialize server", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		slog.Error("Server terminated with error", "error", err)
		os.Exit(1)
	}
}
