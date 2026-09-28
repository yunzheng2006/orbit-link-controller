// Package orbit is the small slice of the Orbit API (/api/v1) the controller
// uses: Link tunnels and the hostnames they serve. Every call goes through one
// client-side rate limiter and retries 429/5xx with backoff, so a cluster with
// many objects, or a controller restarting in a loop, cannot flood the API.
package orbit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// Client talks to the Orbit API with a Link-scoped token.
type Client struct {
	Base    string // e.g. https://dash.yunzheng.space/api/v1
	Token   func() (string, error)
	HTTP    *http.Client
	Limiter *rate.Limiter
	// Observe, when set, is told about every response (for metrics).
	Observe  func(method, route string, code int)
	MaxTries int
	Sleep    func(time.Duration) // tests replace it
}

// New returns a client with the given rate (requests per second) and burst.
func New(base string, token func() (string, error), qps float64, burst int) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), Token: token, HTTP: &http.Client{Timeout: 30 * time.Second},
		Limiter: rate.NewLimiter(rate.Limit(qps), burst), MaxTries: 5, Sleep: time.Sleep}
}

// APIError is a non-2xx answer.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("orbit api %d %s: %s", e.Status, e.Code, e.Message) }

// IsNotFound reports a 404 from the API.
func IsNotFound(err error) bool {
	var a *APIError
	return errors.As(err, &a) && a.Status == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path, route string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return err
		}
	}
	tries := c.MaxTries
	if tries < 1 {
		tries = 1
	}
	var last error
	for i := 0; i < tries; i++ {
		if err := c.Limiter.Wait(ctx); err != nil {
			return err
		}
		tok, err := c.Token()
		if err != nil {
			return fmt.Errorf("reading the Orbit API token: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.Base+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("User-Agent", "orbit-link-controller/"+Version)
		if in != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			last = err
			if c.Observe != nil {
				c.Observe(method, route, 0)
			}
			c.backoff(ctx, i, 0)
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if c.Observe != nil {
			c.Observe(method, route, resp.StatusCode)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(b) > 0 {
				return json.Unmarshal(b, out)
			}
			return nil
		}
		ae := &APIError{Status: resp.StatusCode}
		var eb struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &eb) == nil {
			ae.Code, ae.Message = eb.Error.Code, eb.Error.Message
		}
		if ae.Message == "" {
			ae.Message = strings.TrimSpace(string(b))
		}
		last = ae
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			ra, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			c.backoff(ctx, i, time.Duration(ra)*time.Second)
			continue
		}
		return ae
	}
	return last
}

func (c *Client) backoff(ctx context.Context, attempt int, min time.Duration) {
	d := time.Duration(250*(1<<attempt)) * time.Millisecond
	d += time.Duration(rand.Int64N(int64(d/2) + 1))
	if d < min {
		d = min
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	if ctx.Err() == nil {
		c.Sleep(d)
	}
}

// Version of the controller, sent as User-Agent.
var Version = "dev"

// Tunnel is the API's view of a Link tunnel.
type Tunnel struct {
	ID        int64    `json:"id"`
	Name      string   `json:"name"`
	LocalURL  string   `json:"local_url"`
	Connected bool     `json:"connected"`
	Sites     []string `json:"sites"`
	Token     string   `json:"token,omitempty"` // only on create / rotate
	Connect   *Connect `json:"connect,omitempty"`
	QUIC      *int     `json:"quic_sessions"`
}

// Connect is where a connector dials and whose keys it pins.
type Connect struct {
	Endpoints    []string `json:"endpoints"`
	Fingerprints []string `json:"fingerprints"`
}

// Pop is a landing point choice.
type Pop struct {
	ID   int64  `json:"id"`
	City string `json:"city"`
}

func (c *Client) Pops(ctx context.Context) ([]Pop, bool, error) {
	var r struct {
		Available bool  `json:"available"`
		Data      []Pop `json:"data"`
	}
	err := c.do(ctx, "GET", "/link/pops", "link.pops", nil, &r)
	return r.Data, r.Available, err
}

func (c *Client) Tunnels(ctx context.Context) ([]Tunnel, error) {
	var r struct {
		Data []Tunnel `json:"data"`
	}
	err := c.do(ctx, "GET", "/link/tunnels", "link.list", nil, &r)
	return r.Data, err
}

func (c *Client) Tunnel(ctx context.Context, id int64) (*Tunnel, error) {
	var t Tunnel
	err := c.do(ctx, "GET", "/link/tunnels/"+strconv.FormatInt(id, 10), "link.get", nil, &t)
	return &t, err
}

func (c *Client) CreateTunnel(ctx context.Context, name string, pop, pop2 int64, local string) (*Tunnel, error) {
	in := map[string]any{"name": name, "pop_id": pop, "local_url": local}
	if pop2 > 0 {
		in["pop_id2"] = pop2
	}
	var t Tunnel
	err := c.do(ctx, "POST", "/link/tunnels", "link.create", in, &t)
	return &t, err
}

func (c *Client) PatchTunnel(ctx context.Context, id int64, fields map[string]any) error {
	return c.do(ctx, "PATCH", "/link/tunnels/"+strconv.FormatInt(id, 10), "link.patch", fields, nil)
}

func (c *Client) RotateToken(ctx context.Context, id int64) (*Tunnel, error) {
	var t Tunnel
	err := c.do(ctx, "POST", "/link/tunnels/"+strconv.FormatInt(id, 10)+"/token", "link.token", nil, &t)
	return &t, err
}

func (c *Client) DeleteTunnel(ctx context.Context, id int64) error {
	return c.do(ctx, "DELETE", "/link/tunnels/"+strconv.FormatInt(id, 10), "link.delete", nil, nil)
}

// HostState is one hostname's result from a sync.
type HostState struct {
	Hostname string `json:"hostname"`
	State    string `json:"state"`
	SiteID   *int64 `json:"site_id"`
	Target   string `json:"target"`
	Verified bool   `json:"verified"`
	Message  string `json:"message"`
}

// OK: the hostname is served through the tunnel.
func (h HostState) OK() bool {
	switch h.State {
	case "created", "updated", "unchanged", "adopted":
		return true
	}
	return false
}

// SyncResult is the answer to PUT /link/tunnels/{id}/hostnames.
type SyncResult struct {
	Hostnames []HostState `json:"hostnames"`
	Removed   []string    `json:"removed"`
}

// SyncHostnames declares the complete hostname list for a tunnel.
func (c *Client) SyncHostnames(ctx context.Context, id int64, managedBy string, hosts []string, adopt bool) (*SyncResult, error) {
	if hosts == nil {
		hosts = []string{}
	}
	var r SyncResult
	err := c.do(ctx, "PUT", "/link/tunnels/"+strconv.FormatInt(id, 10)+"/hostnames", "link.hostnames",
		map[string]any{"managed_by": managedBy, "hostnames": hosts, "adopt": adopt}, &r)
	return &r, err
}

// PathEscape is exported for callers building URLs.
var PathEscape = url.PathEscape
