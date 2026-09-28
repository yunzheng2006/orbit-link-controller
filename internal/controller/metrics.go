package controller

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics the controller exports on /metrics (controller-runtime's registry).
type Metrics struct {
	API         *prometheus.CounterVec
	Hostnames   *prometheus.GaugeVec
	SyncErrors  prometheus.Counter
	SyncSeconds prometheus.Histogram
	Connected   prometheus.Gauge
	Router      *prometheus.CounterVec
}

var states = []string{"created", "updated", "unchanged", "adopted", "conflict", "limit_exceeded", "forbidden", "invalid", "error"}

// NewMetrics registers the metrics once (reg nil: controller-runtime's).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = metrics.Registry
	}
	m := &Metrics{
		API: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_link_api_requests_total",
			Help: "Orbit API requests by route and HTTP status (0 = no answer)."}, []string{"route", "code"}),
		Hostnames: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "orbit_link_hostnames",
			Help: "Hostnames in the last sync, by state."}, []string{"state"}),
		SyncErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_link_sync_errors_total",
			Help: "Syncs that failed."}),
		SyncSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "orbit_link_sync_seconds",
			Help: "Duration of a full sync.", Buckets: prometheus.ExponentialBuckets(0.05, 2, 10)}),
		Connected: prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_link_tunnel_connected",
			Help: "1 when Orbit sees a connector attached to the tunnel."}),
		Router: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_link_router_requests_total",
			Help: "Requests the in-cluster router answered, by status."}, []string{"code"}),
	}
	reg.MustRegister(m.API, m.Hostnames, m.SyncErrors, m.SyncSeconds, m.Connected, m.Router)
	return m
}

// SetHostnames replaces the per-state gauge.
func (m *Metrics) SetHostnames(counts map[string]float64) {
	for _, s := range states {
		m.Hostnames.WithLabelValues(s).Set(counts[s])
	}
}

// ObserveAPI is the orbit client's hook.
func (m *Metrics) ObserveAPI(_ string, route string, code int) {
	m.API.WithLabelValues(route, strconv.Itoa(code)).Inc()
}

// ObserveRouter is the router's hook.
func (m *Metrics) ObserveRouter(code int) { m.Router.WithLabelValues(strconv.Itoa(code)).Inc() }
