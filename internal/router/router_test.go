package router

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/routes"
)

// Backends are real HTTP servers; Services resolve to them by name.
func setup(t *testing.T) (*Router, map[string]*httptest.Server) {
	t.Helper()
	srv := map[string]*httptest.Server{}
	for _, n := range []string{"front", "api"} {
		n := n
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%s host=%s path=%s xfh=%s", n, r.Host, r.URL.Path, r.Header.Get("X-Forwarded-Host"))
		}))
		t.Cleanup(s.Close)
		srv[n] = s
	}
	rt := New(func(_ context.Context, b routes.Backend) (string, error) {
		s := srv[b.Service]
		if s == nil {
			return "", fmt.Errorf("no service %s", b.Service)
		}
		return strings.TrimPrefix(s.URL, "http://"), nil
	})
	return rt, srv
}

func table(paths map[string]string) *routes.Table {
	pt := networkingv1.PathTypePrefix
	var ps []networkingv1.HTTPIngressPath
	for p, svc := range paths {
		ps = append(ps, networkingv1.HTTPIngressPath{Path: p, PathType: &pt, Backend: networkingv1.IngressBackend{
			Service: &networkingv1.IngressServiceBackend{Name: svc, Port: networkingv1.ServiceBackendPort{Number: 80}}}})
	}
	return routes.Build(routes.Input{Ingresses: []networkingv1.Ingress{{ObjectMeta: metav1.ObjectMeta{Namespace: "web", Name: "i"},
		Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{Host: "app.example.org",
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: ps}}}}}}}})
}

func get(t *testing.T, h http.Handler, host, path string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://"+host+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, string(b)
}

func TestRoutesByHostAndPathAndKeepsHost(t *testing.T) {
	rt, _ := setup(t)
	rt.Set(table(map[string]string{"/": "front", "/api": "api"}))
	if c, b := get(t, rt, "app.example.org", "/x"); c != 200 || !strings.HasPrefix(b, "front host=app.example.org path=/x xfh=app.example.org") {
		t.Fatalf("%d %q", c, b)
	}
	if c, b := get(t, rt, "app.example.org", "/api/v1"); c != 200 || !strings.HasPrefix(b, "api ") {
		t.Fatalf("%d %q", c, b)
	}
	if c, _ := get(t, rt, "unknown.example.org", "/"); c != 404 {
		t.Fatalf("unknown host: %d", c)
	}
}

// A path change takes effect on the next request after Invalidate.
func TestInvalidateRebuilds(t *testing.T) {
	rt, _ := setup(t)
	cur := map[string]string{"/": "front"}
	rt.Build = func(context.Context) (*routes.Table, error) { return table(cur), nil }
	rt.Invalidate()
	if _, b := get(t, rt, "app.example.org", "/api"); !strings.HasPrefix(b, "front ") {
		t.Fatalf("%q", b)
	}
	cur = map[string]string{"/": "front", "/api": "api"}
	if _, b := get(t, rt, "app.example.org", "/api"); !strings.HasPrefix(b, "front ") {
		t.Fatal("rebuilt without being told")
	}
	rt.Invalidate()
	if _, b := get(t, rt, "app.example.org", "/api"); !strings.HasPrefix(b, "api ") {
		t.Fatalf("after invalidate: %q", b)
	}
}

func TestUnresolvableBackendIs503AndDownIs502(t *testing.T) {
	rt, srv := setup(t)
	rt.Set(table(map[string]string{"/": "missing"}))
	if c, _ := get(t, rt, "app.example.org", "/"); c != 503 {
		t.Fatalf("unresolvable: %d", c)
	}
	rt.Set(table(map[string]string{"/": "api"}))
	srv["api"].Close()
	if c, _ := get(t, rt, "app.example.org", "/"); c != 502 {
		t.Fatalf("down: %d", c)
	}
}

func TestWeightedPick(t *testing.T) {
	n := map[string]int{}
	bs := []routes.Backend{{Service: "a", Weight: 3}, {Service: "b", Weight: 1}}
	for i := 0; i < 4000; i++ {
		n[pick(bs).Service]++
	}
	if n["a"] < 2700 || n["a"] > 3300 {
		t.Fatalf("weights %v", n)
	}
	if pick([]routes.Backend{{Service: "only", Weight: 0}}).Service != "only" {
		t.Fatal("single backend")
	}
}
