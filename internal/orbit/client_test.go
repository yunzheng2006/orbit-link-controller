package orbit_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yunzheng2006/orbit-link-controller/internal/orbit"
	"github.com/yunzheng2006/orbit-link-controller/internal/orbit/orbittest"
)

func tok() (string, error) { return "orb_test", nil }

func TestRetriesServerErrorsThenSucceeds(t *testing.T) {
	f := orbittest.New("orb_test")
	defer f.Close()
	f.Fail = 2
	c := orbit.New(f.URL+"/api/v1", tok, 1000, 100)
	var slept []time.Duration
	c.Sleep = func(d time.Duration) { slept = append(slept, d) }
	if _, err := c.Tunnels(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(slept) != 2 || slept[1] <= slept[0]/2 {
		t.Fatalf("backoff %v", slept)
	}
}

func TestHonoursRetryAfterAndGivesUp(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"slow down"}}`))
	}))
	defer srv.Close()
	c := orbit.New(srv.URL, tok, 1000, 100)
	c.MaxTries = 3
	var slept []time.Duration
	c.Sleep = func(d time.Duration) { slept = append(slept, d) }
	_, err := c.Tunnels(context.Background())
	var ae *orbit.APIError
	if err == nil || !asAPI(err, &ae) || ae.Status != 429 || ae.Code != "rate_limited" {
		t.Fatalf("err %v", err)
	}
	if n.Load() != 3 {
		t.Fatalf("tries %d", n.Load())
	}
	for _, d := range slept {
		if d < 7*time.Second {
			t.Fatalf("Retry-After ignored: %v", slept)
		}
	}
}

func TestClientErrorsAreNotRetried(t *testing.T) {
	f := orbittest.New("orb_test")
	defer f.Close()
	c := orbit.New(f.URL+"/api/v1", tok, 1000, 100)
	_, err := c.Tunnel(context.Background(), 42)
	if !orbit.IsNotFound(err) {
		t.Fatalf("err %v", err)
	}
	if f.Calls["GET /link/tunnels/42"] != 1 {
		t.Fatalf("calls %v", f.Calls)
	}
}

// The limiter spaces calls: 5 calls at 20/s with burst 1 take >= 200 ms.
func TestRateLimited(t *testing.T) {
	f := orbittest.New("orb_test")
	defer f.Close()
	c := orbit.New(f.URL+"/api/v1", tok, 20, 1)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := c.Tunnels(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d < 190*time.Millisecond {
		t.Fatalf("5 calls in %v: not rate limited", d)
	}
}

func asAPI(err error, out **orbit.APIError) bool {
	a, ok := err.(*orbit.APIError)
	*out = a
	return ok
}
