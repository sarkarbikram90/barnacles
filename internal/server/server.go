package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/health"
	"github.com/sarkarbikram90/barnacles/internal/ingest"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
	"github.com/sarkarbikram90/barnacles/internal/stream"
)

// Server coordinates the central HTTP API, ingest pipeline, storage, and streaming.
type Server struct {
	cfg        config.ServerConfig
	httpServer *http.Server
	store      store.LogStore
	hub        *stream.Hub
	ingest     *ingest.Handler
	health     *health.Handler
	metrics    *metrics.ServerMetrics
	retention  *store.RetentionWorker
	staticFS   fs.FS

	wg sync.WaitGroup
}

// Option configures optional parameters on Server.
type Option func(*Server)

// WithStaticFS allows providing an embedded filesystem for the Web UI.
func WithStaticFS(static fs.FS) Option {
	return func(s *Server) {
		s.staticFS = static
	}
}

// New creates and configures a new central Barnacles Server instance.
func New(cfg config.ServerConfig, m *metrics.ServerMetrics, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate server config: %w", err)
	}

	if m == nil {
		m = metrics.NewServerMetrics()
	}

	fsStore, err := store.NewFileStore(store.Config{
		Directory:   cfg.Storage.Directory,
		SyncOnWrite: cfg.Storage.SyncOnWrite,
	})
	if err != nil {
		return nil, fmt.Errorf("init log store: %w", err)
	}

	recentBuf := stream.NewRecentBuffer(cfg.Stream.RecentEvents)
	hub := stream.NewHub(cfg.Stream, recentBuf, m)
	ingestHandler := ingest.NewHandler(cfg.Ingest, fsStore, hub, m)

	healthHandler := health.NewHandler()
	healthHandler.AddChecker("storage", func(ctx context.Context) error {
		if fsStore.DiskUsage() < 0 {
			return errors.New("invalid storage state")
		}
		return nil
	})

	var retentionWorker *store.RetentionWorker
	if cfg.Retention.Enabled {
		retentionWorker = store.NewRetentionWorker(fsStore, cfg.Retention)
	}

	s := &Server{
		cfg:       cfg,
		store:     fsStore,
		hub:       hub,
		ingest:    ingestHandler,
		health:    healthHandler,
		metrics:   m,
		retention: retentionWorker,
	}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	var handler http.Handler = mux
	handler = CORSMiddleware(handler)
	handler = AuthMiddleware(cfg.Auth, handler)
	handler = LoggingMiddleware(handler)
	handler = RecoveryMiddleware(handler)

	s.httpServer = &http.Server{
		Addr:              cfg.Server.Address,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
	}

	return s, nil
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Health & Metrics
	mux.HandleFunc("/healthz", s.health.Healthz)
	mux.HandleFunc("/readyz", s.health.Readyz)
	mux.Handle("/metrics", s.metrics.Handler())

	// Ingest endpoint
	mux.Handle("/api/v1/ingest", s.ingest)

	// REST Query endpoints
	mux.HandleFunc("/api/v1/logs", handleLogsQuery(s.store))
	mux.HandleFunc("/api/v1/sources", handleSourcesQuery(s.store))
	mux.HandleFunc("/api/v1/agents", handleHostsQuery(s.store))
	mux.HandleFunc("/api/v1/hosts", handleHostsQuery(s.store))

	// WebSocket endpoints
	mux.HandleFunc("/ws", handleWebSocketStream(s.hub))
	mux.HandleFunc("/api/v1/stream", handleWebSocketStream(s.hub))

	// Web UI Static Hosting
	s.registerStaticRoutes(mux)
}

func (s *Server) registerStaticRoutes(mux *http.ServeMux) {
	// If custom web_dir is configured on disk, use that
	if s.cfg.Server.WebDir != "" {
		if _, err := os.Stat(s.cfg.Server.WebDir); err == nil {
			fileServer := http.FileServer(http.Dir(s.cfg.Server.WebDir))
			mux.Handle("/", fileServer)
			return
		}
	}

	// If local ./web directory exists on disk (standard repo layout)
	if _, err := os.Stat("./web"); err == nil {
		fileServer := http.FileServer(http.Dir("./web"))
		mux.Handle("/", fileServer)
		return
	}

	// If embedded static filesystem is provided
	if s.staticFS != nil {
		fileServer := http.FileServer(http.FS(s.staticFS))
		mux.Handle("/", fileServer)
		return
	}

	// Fallback simple landing page
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>Barnacles Server</title></head><body><h1>Barnacles Server Online</h1><p>API endpoints active: /api/v1/ingest, /api/v1/logs, /ws, /metrics, /healthz</p></body></html>`))
	})
}

// Start runs the HTTP server and retention workers.
// Blocks until ctx is cancelled, then initiates graceful shutdown.
func (s *Server) Start(ctx context.Context) error {
	slog.Info("Starting Barnacles server", "address", s.cfg.Server.Address)

	// Start retention worker if configured
	if s.retention != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.retention.Start(ctx)
		}()
	}

	errCh := make(chan error, 1)
	go func() {
		if s.cfg.TLS.Enabled {
			slog.Info("Starting TLS listener", "cert", s.cfg.TLS.CertFile, "key", s.cfg.TLS.KeyFile)
			if err := s.httpServer.ListenAndServeTLS(s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		} else {
			if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server failed: %w", err)
	case <-ctx.Done():
		slog.Info("Shutting down Barnacles server...")
	}

	// Graceful shutdown with 5s timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	_ = s.hub.Close()

	s.wg.Wait()

	if err := s.store.Close(); err != nil {
		slog.Error("LogStore close error", "error", err)
	}

	slog.Info("Barnacles server stopped cleanly")
	return nil
}

// Hub returns the server's WebSocket stream Hub.
func (s *Server) Hub() *stream.Hub {
	return s.hub
}

// Store returns the underlying LogStore.
func (s *Server) Store() store.LogStore {
	return s.store
}
