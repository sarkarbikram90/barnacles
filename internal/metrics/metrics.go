// Package metrics provides Prometheus instrumentation for Barnacles Agent and Server instances.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AgentMetrics encapsulates all Prometheus metrics emitted by a Barnacles Agent.
type AgentMetrics struct {
	Registry          *prometheus.Registry
	EventsReadTotal   *prometheus.CounterVec
	EventsParsedTotal *prometheus.CounterVec
	ParseErrorsTotal  *prometheus.CounterVec
	EventsSentTotal   prometheus.Counter
	SendErrorsTotal   prometheus.Counter
	BatchesSentTotal  prometheus.Counter
	SpoolBytes        prometheus.Gauge
	SpoolEvents       prometheus.Gauge
	SourcesActive     prometheus.Gauge
}

// NewAgentMetrics initializes and registers Prometheus metrics for an Agent.
func NewAgentMetrics() *AgentMetrics {
	reg := prometheus.NewRegistry()

	m := &AgentMetrics{
		Registry: reg,
		EventsReadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "barnacles_agent_events_read_total",
			Help: "Total number of raw log lines read by the agent.",
		}, []string{"source"}),
		EventsParsedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "barnacles_agent_events_parsed_total",
			Help: "Total number of log events successfully parsed by the agent.",
		}, []string{"source"}),
		ParseErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "barnacles_agent_parse_errors_total",
			Help: "Total number of log parsing errors encountered.",
		}, []string{"source"}),
		EventsSentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_agent_events_sent_total",
			Help: "Total number of log events successfully sent to the server.",
		}),
		SendErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_agent_send_errors_total",
			Help: "Total number of HTTP send errors encountered.",
		}),
		BatchesSentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_agent_batches_sent_total",
			Help: "Total number of batches sent to the server.",
		}),
		SpoolBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "barnacles_agent_spool_bytes",
			Help: "Current size of the on-disk spool in bytes.",
		}),
		SpoolEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "barnacles_agent_spool_events",
			Help: "Current number of buffered events in the on-disk spool.",
		}),
		SourcesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "barnacles_agent_sources",
			Help: "Current number of actively watched log sources.",
		}),
	}

	reg.MustRegister(
		m.EventsReadTotal,
		m.EventsParsedTotal,
		m.ParseErrorsTotal,
		m.EventsSentTotal,
		m.SendErrorsTotal,
		m.BatchesSentTotal,
		m.SpoolBytes,
		m.SpoolEvents,
		m.SourcesActive,
	)

	return m
}

// Handler returns an http.Handler serving the agent Prometheus metrics.
func (m *AgentMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// ServerMetrics encapsulates all Prometheus metrics emitted by a Barnacles Central Server.
type ServerMetrics struct {
	Registry                  *prometheus.Registry
	EventsIngestedTotal       prometheus.Counter
	IngestErrorsTotal         prometheus.Counter
	EventsStoredTotal         prometheus.Counter
	WebsocketClients          prometheus.Gauge
	WebsocketDisconnectsTotal *prometheus.CounterVec
	EventsBroadcastTotal      prometheus.Counter
	StorageBytes              prometheus.Gauge
	QueryDuration             prometheus.Histogram
	IngestDuration            prometheus.Histogram
}

// NewServerMetrics initializes and registers Prometheus metrics for a Server.
func NewServerMetrics() *ServerMetrics {
	reg := prometheus.NewRegistry()

	m := &ServerMetrics{
		Registry: reg,
		EventsIngestedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_server_events_ingested_total",
			Help: "Total number of events received via HTTP ingest.",
		}),
		IngestErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_server_ingest_errors_total",
			Help: "Total number of ingest request validation or processing errors.",
		}),
		EventsStoredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_server_events_stored_total",
			Help: "Total number of events persisted to the server log store.",
		}),
		WebsocketClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "barnacles_server_websocket_clients",
			Help: "Current number of connected WebSocket clients.",
		}),
		WebsocketDisconnectsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "barnacles_server_websocket_disconnects_total",
			Help: "Total number of WebSocket client disconnects by reason.",
		}, []string{"reason"}),
		EventsBroadcastTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "barnacles_server_events_broadcast_total",
			Help: "Total number of events broadcast to WebSocket streams.",
		}),
		StorageBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "barnacles_server_storage_bytes",
			Help: "Total disk space in bytes consumed by server log storage.",
		}),
		QueryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "barnacles_server_query_duration_seconds",
			Help:    "Histogram of log query execution durations.",
			Buckets: prometheus.DefBuckets,
		}),
		IngestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "barnacles_server_ingest_duration_seconds",
			Help:    "Histogram of HTTP ingest request processing durations.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	reg.MustRegister(
		m.EventsIngestedTotal,
		m.IngestErrorsTotal,
		m.EventsStoredTotal,
		m.WebsocketClients,
		m.WebsocketDisconnectsTotal,
		m.EventsBroadcastTotal,
		m.StorageBytes,
		m.QueryDuration,
		m.IngestDuration,
	)

	return m
}

// Handler returns an http.Handler serving the server Prometheus metrics.
func (m *ServerMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
