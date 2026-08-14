// Package main is the entrypoint for the Barnacles Agent binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/agent"
	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/health"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
)

var (
	version = "0.1.0-dev"
)

func main() {
	var (
		configPath  = flag.String("config", "config/agent.yaml", "Path to agent configuration YAML file")
		showVersion = flag.Bool("version", false, "Print version information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("barnacles-agent version %s\n", version)
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Initializing Barnacles Agent", "version", version, "config", *configPath)

	cfg, err := config.LoadAgentConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load agent configuration", "error", err)
		os.Exit(1)
	}

	agentMetrics := metrics.NewAgentMetrics()

	// Start local metrics / health server if configured
	if cfg.Agent.MetricsAddress != "" {
		healthHandler := health.NewHandler()
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", healthHandler.Healthz)
		mux.HandleFunc("/readyz", healthHandler.Readyz)
		mux.Handle("/metrics", agentMetrics.Handler())

		metricsServer := &http.Server{
			Addr:              cfg.Agent.MetricsAddress,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		go func() {
			slog.Info("Agent metrics server listening", "address", cfg.Agent.MetricsAddress)
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Warn("Agent metrics server stopped", "error", err)
			}
		}()
	}

	ag, err := agent.New(cfg, agentMetrics)
	if err != nil {
		slog.Error("Failed to initialize agent", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := ag.Start(ctx); err != nil {
		slog.Error("Agent terminated with error", "error", err)
		os.Exit(1)
	}
}
