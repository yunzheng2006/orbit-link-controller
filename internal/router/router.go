// Package router is the in-cluster HTTP front the Orbit Link connector
// forwards to. The connector deliberately forwards to ONE fixed address that
// nothing on the wire can change; this router, reachable only inside the
// cluster, is that address, and it dispatches by Host and path to Services
// according to the Ingresses and HTTPRoutes the controller accepted.
package router

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yunzheng2006/orbit-link-controller/internal/routes"
)

// Resolver turns a backend into host:port (a Service's cluster IP and port).
type Resolver func(ctx context.Context, b routes.Backend) (string, error)

// Router serves the current table.
type Router struct {
	table   atomic.Pointer[routes.Table]
	Resolve Resolver
	// Build, when set, rebuilds the table lazily after Invalidate.
	Build   func(ctx context.Context) (*routes.Table, error)
	dirty   atomic.Bool
	mu      sync.Mutex
	Observe func(code int)
	proxy   *httputil.ReverseProxy
}

type ctxKey struct{}

// New makes a router with an empty table.
func New(resolve Resolver) *Router {
	r := &Router{Resolve: resolve}
	r.table.Store(&routes.Table{Rules: map[string][]routes.Rule{}})
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 0, // long polls and streams are the backend's business
	}
	r.proxy = &httputil.ReverseProxy{
		Transport: tr,
		Rewrite: func(pr *httputil.ProxyRequest) {
			target := pr.In.Context().Value(ctxKey{}).(*url.URL)
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
			// The edge terminated TLS; the request reached us over the tunnel.
			pr.Out.Header.Set("X-Forwarded-Proto", "https")
			if p := pr.In.Header.Get("X-Forwarded-Proto"); p == "http" || p == "https" {
				pr.Out.Header.Set("X-Forwarded-Proto", p)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprintln(w, "the service for this address is not reachable")
		},
	}
	return r
}

// Set replaces the table.
func (r *Router) Set(t *routes.Table) { r.table.Store(t); r.dirty.Store(false) }

// Invalidate marks the table stale; the next request rebuilds it.
func (r *Router) Invalidate() { r.dirty.Store(true) }

// Table returns the current table (rebuilding first when stale).
func (r *Router) Table(ctx context.Context) *routes.Table {
	if r.dirty.Load() && r.Build != nil {
		r.mu.Lock()
		if r.dirty.Load() {
			r.dirty.Store(false)
			if t, err := r.Build(ctx); err == nil {
				r.table.Store(t)
			} else {
				r.dirty.Store(true)
			}
		}
		r.mu.Unlock()
	}
	return r.table.Load()
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	sw := &statusWriter{ResponseWriter: w, code: 200}
	defer func() {
		if r.Observe != nil {
			r.Observe(sw.code)
		}
	}()
	rule := r.Table(req.Context()).Match(req.Host, req.URL.Path)
	if rule == nil {
		sw.Header().Set("Content-Type", "text/plain; charset=utf-8")
		sw.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(sw, "no route for this address")
		return
	}
	be := pick(rule.Backends)
	hp, err := r.Resolve(req.Context(), be)
	if err != nil {
		sw.Header().Set("Content-Type", "text/plain; charset=utf-8")
		sw.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(sw, "the service for this address is not available")
		return
	}
	target := &url.URL{Scheme: "http", Host: hp}
	r.proxy.ServeHTTP(sw, req.WithContext(context.WithValue(req.Context(), ctxKey{}, target)))
}

// pick chooses a backend by weight.
func pick(bs []routes.Backend) routes.Backend {
	if len(bs) == 1 {
		return bs[0]
	}
	var total int64
	for _, b := range bs {
		total += int64(b.Weight)
	}
	n := rand.Int64N(total)
	for _, b := range bs {
		n -= int64(b.Weight)
		if n < 0 {
			return b
		}
	}
	return bs[len(bs)-1]
}

// HostPort formats an address.
func HostPort(ip string, port int32) string { return net.JoinHostPort(ip, strconv.Itoa(int(port))) }
