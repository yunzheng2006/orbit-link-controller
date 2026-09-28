package routes

import (
	"strings"
	"testing"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func pt(t networkingv1.PathType) *networkingv1.PathType { return &t }

func ing(ns, name string, age int, rules ...networkingv1.IngressRule) networkingv1.Ingress {
	return networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name,
		CreationTimestamp: metav1.NewTime(time.Unix(1_700_000_000+int64(age), 0))},
		Spec: networkingv1.IngressSpec{Rules: rules}}
}

func rule(host string, paths ...networkingv1.HTTPIngressPath) networkingv1.IngressRule {
	return networkingv1.IngressRule{Host: host, IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: paths}}}
}

func path(p string, typ networkingv1.PathType, svc string, port int32) networkingv1.HTTPIngressPath {
	return networkingv1.HTTPIngressPath{Path: p, PathType: pt(typ), Backend: networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{Name: svc, Port: networkingv1.ServiceBackendPort{Number: port}}}}
}

func TestIngressPathsMatchMostSpecificFirst(t *testing.T) {
	tb := Build(Input{Ingresses: []networkingv1.Ingress{ing("web", "site", 0, rule("App.Example.org.",
		path("/", networkingv1.PathTypePrefix, "front", 80),
		path("/api/", networkingv1.PathTypePrefix, "api", 8080),
		path("/api/health", networkingv1.PathTypeExact, "health", 9000),
	))}})
	if got := tb.Hostnames(); len(got) != 1 || got[0] != "app.example.org" {
		t.Fatalf("hostnames %v", got)
	}
	for _, c := range []struct{ host, path, svc string }{
		{"app.example.org", "/", "front"},
		{"app.example.org", "/about", "front"},
		{"APP.example.org:443", "/api", "api"},
		{"app.example.org", "/api/users", "api"},
		{"app.example.org", "/apix", "front"}, // element-wise prefix
		{"app.example.org", "/api/health", "health"},
		{"app.example.org", "/api/health/deep", "api"},
	} {
		r := tb.Match(c.host, c.path)
		if r == nil || r.Backends[0].Service != c.svc {
			t.Errorf("%s%s -> %+v, want %s", c.host, c.path, r, c.svc)
		}
	}
	if tb.Match("other.example.org", "/") != nil {
		t.Error("an unknown host matched")
	}
}

func TestIngressProblemsAreReportedNotPublished(t *testing.T) {
	tb := Build(Input{Ingresses: []networkingv1.Ingress{ing("web", "bad", 0,
		rule("", path("/", networkingv1.PathTypePrefix, "a", 80)),
		rule("*.example.org", path("/", networkingv1.PathTypePrefix, "a", 80)),
		rule("localhost", path("/", networkingv1.PathTypePrefix, "a", 80)),
		rule("ok.example.org", path("/", networkingv1.PathTypePrefix, "a", 80)),
	)}})
	if got := tb.Hostnames(); len(got) != 1 || got[0] != "ok.example.org" {
		t.Fatalf("hostnames %v", got)
	}
	if len(tb.Problems) != 3 {
		t.Fatalf("problems %+v", tb.Problems)
	}
}

func TestIngressConflictOlderWins(t *testing.T) {
	older := ing("a", "first", 0, rule("x.example.org", path("/", networkingv1.PathTypePrefix, "one", 80)))
	newer := ing("b", "second", 10, rule("x.example.org", path("/", networkingv1.PathTypePrefix, "two", 80), path("/extra", networkingv1.PathTypePrefix, "two", 80)))
	for _, order := range [][]networkingv1.Ingress{{older, newer}, {newer, older}} {
		tb := Build(Input{Ingresses: order})
		if r := tb.Match("x.example.org", "/"); r == nil || r.Backends[0].Service != "one" {
			t.Fatalf("the older Ingress must keep /: %+v", r)
		}
		if r := tb.Match("x.example.org", "/extra/1"); r == nil || r.Backends[0].Service != "two" {
			t.Fatalf("a non-conflicting path of the newer one is published: %+v", r)
		}
		if len(tb.Problems) != 1 || tb.Problems[0].Reason != "Conflict" || tb.Problems[0].Source.Name != "second" {
			t.Fatalf("problems %+v", tb.Problems)
		}
	}
}

func TestIngressDefaultBackendIsLastResort(t *testing.T) {
	i := ing("web", "d", 0, rule("d.example.org", path("/api", networkingv1.PathTypePrefix, "api", 80)))
	i.Spec.DefaultBackend = &networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "fallback", Port: networkingv1.ServiceBackendPort{Name: "http"}}}
	tb := Build(Input{Ingresses: []networkingv1.Ingress{i}})
	if r := tb.Match("d.example.org", "/api/x"); r == nil || r.Backends[0].Service != "api" {
		t.Fatalf("%+v", r)
	}
	if r := tb.Match("d.example.org", "/other"); r == nil || r.Backends[0].Service != "fallback" || r.Backends[0].PortName != "http" {
		t.Fatalf("%+v", r)
	}
}

func gw(ns, name string, listeners ...gatewayv1.Listener) gatewayv1.Gateway {
	return gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: gatewayv1.GatewaySpec{GatewayClassName: "orbit-link", Listeners: listeners}}
}

func ptr[T any](v T) *T { return &v }

func route(ns, name string, parent string, hosts []string, rules ...gatewayv1.HTTPRouteRule) gatewayv1.HTTPRoute {
	var hs []gatewayv1.Hostname
	for _, h := range hosts {
		hs = append(hs, gatewayv1.Hostname(h))
	}
	pns, pname, _ := strings.Cut(parent, "/")
	return gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
			Namespace: ptr(gatewayv1.Namespace(pns)), Name: gatewayv1.ObjectName(pname)}}}, Hostnames: hs, Rules: rules}}
}

func backend(svc string, port int32, weight int32) gatewayv1.HTTPBackendRef {
	return gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{
		Name: gatewayv1.ObjectName(svc), Port: ptr(gatewayv1.PortNumber(port))}, Weight: ptr(weight)}}
}

func prefix(p string) []gatewayv1.HTTPRouteMatch {
	return []gatewayv1.HTTPRouteMatch{{Path: &gatewayv1.HTTPPathMatch{Type: ptr(gatewayv1.PathMatchPathPrefix), Value: ptr(p)}}}
}

func TestHTTPRouteOnOurGateway(t *testing.T) {
	g := gw("infra", "edge", gatewayv1.Listener{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80,
		AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{From: ptr(gatewayv1.NamespacesFromAll)}}})
	rt := route("shop", "store", "infra/edge", []string{"shop.example.org"},
		gatewayv1.HTTPRouteRule{Matches: prefix("/cart"), BackendRefs: []gatewayv1.HTTPBackendRef{backend("cart", 8080, 1)}},
		gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backend("web", 80, 3), backend("web-canary", 80, 1)}})
	tb := Build(Input{Gateways: []gatewayv1.Gateway{g}, HTTPRoutes: []gatewayv1.HTTPRoute{rt}})
	if r := tb.Match("shop.example.org", "/cart/1"); r == nil || r.Backends[0].Service != "cart" || r.Backends[0].Namespace != "shop" {
		t.Fatalf("%+v", r)
	}
	if r := tb.Match("shop.example.org", "/"); r == nil || len(r.Backends) != 2 || r.Backends[0].Weight != 3 {
		t.Fatalf("%+v", r)
	}
	ps := tb.Parents[Source{"HTTPRoute", "shop", "store"}]
	if len(ps) != 1 || !ps[0].Accepted || !ps[0].Resolved {
		t.Fatalf("parents %+v", ps)
	}
	if got := tb.GatewayHosts["infra/edge"]; len(got) != 1 || got[0] != "shop.example.org" {
		t.Fatalf("gateway hosts %v", got)
	}
}

func TestHTTPRouteRefusals(t *testing.T) {
	same := gw("infra", "edge", gatewayv1.Listener{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80, Hostname: ptr(gatewayv1.Hostname("*.example.org"))})
	// Another namespace, listener allows Same only: not accepted.
	r1 := route("shop", "r1", "infra/edge", []string{"a.example.org"}, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backend("web", 80, 1)}})
	// Hostname outside the listener's wildcard.
	r2 := route("infra", "r2", "infra/edge", []string{"a.other.org"}, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backend("web", 80, 1)}})
	// Cross-namespace backend: accepted, refs not resolved, nothing published for that rule.
	cross := backend("db", 80, 1)
	cross.Namespace = ptr(gatewayv1.Namespace("elsewhere"))
	r3 := route("infra", "r3", "infra/edge", []string{"b.example.org"}, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{cross}})
	// A route on somebody else's Gateway is not ours at all.
	r4 := route("infra", "r4", "infra/nginx", []string{"c.example.org"}, gatewayv1.HTTPRouteRule{BackendRefs: []gatewayv1.HTTPBackendRef{backend("web", 80, 1)}})
	// Filters are not supported: the rule is skipped and said so.
	r5 := route("infra", "r5", "infra/edge", []string{"d.example.org"}, gatewayv1.HTTPRouteRule{
		Filters: []gatewayv1.HTTPRouteFilter{{Type: gatewayv1.HTTPRouteFilterRequestHeaderModifier}}, BackendRefs: []gatewayv1.HTTPBackendRef{backend("web", 80, 1)}})
	tb := Build(Input{Gateways: []gatewayv1.Gateway{same}, HTTPRoutes: []gatewayv1.HTTPRoute{r1, r2, r3, r4, r5}})
	if h := tb.Hostnames(); len(h) != 0 {
		t.Fatalf("nothing should be published, got %v", h)
	}
	chk := func(name string, accepted, resolved bool, reason string) {
		t.Helper()
		ps := tb.Parents[Source{"HTTPRoute", strings.Split(name, "/")[0], strings.Split(name, "/")[1]}]
		if len(ps) != 1 || ps[0].Accepted != accepted || ps[0].Resolved != resolved || (reason != "" && ps[0].AcceptedReason != reason && ps[0].ResolvedReason != reason) {
			t.Fatalf("%s parents %+v", name, ps)
		}
	}
	chk("shop/r1", false, true, "NotAllowedByListeners")
	chk("infra/r2", false, true, "NoMatchingListenerHostname")
	chk("infra/r3", true, false, "RefNotPermitted")
	if _, ok := tb.Parents[Source{"HTTPRoute", "infra", "r4"}]; ok {
		t.Fatal("a route on another controller's Gateway got a status from us")
	}
	chk("infra/r5", true, true, "")
}

func TestCleanHost(t *testing.T) {
	for in, want := range map[string]string{"A.Example.ORG.": "a.example.org", "x-y.example.org": "x-y.example.org"} {
		if got, why := CleanHost(in); got != want || why != "" {
			t.Errorf("%q -> %q %q", in, got, why)
		}
	}
	for _, bad := range []string{"", "*.a.org", "nodot", "-a.example.org", "a..example.org", "a_b.example.org"} {
		if got, why := CleanHost(bad); got != "" || why == "" {
			t.Errorf("%q accepted", bad)
		}
	}
}

// ---- added after the mutation run ----

// Same creation time: the tie is broken by namespace, then name — whatever
// order the objects arrive in.
func TestConflictTieBreakIsStable(t *testing.T) {
	a := ing("alpha", "site", 0, rule("t.example.org", path("/", networkingv1.PathTypePrefix, "from-alpha", 80)))
	b := ing("beta", "site", 0, rule("t.example.org", path("/", networkingv1.PathTypePrefix, "from-beta", 80)))
	c := ing("alpha", "zzz", 0, rule("t.example.org", path("/", networkingv1.PathTypePrefix, "from-zzz", 80)))
	for _, order := range [][]networkingv1.Ingress{{a, b, c}, {c, b, a}, {b, c, a}} {
		tb := Build(Input{Ingresses: order})
		if r := tb.Match("t.example.org", "/"); r == nil || r.Backends[0].Service != "from-alpha" {
			t.Fatalf("order %v: %+v", []string{order[0].Name, order[1].Name, order[2].Name}, r)
		}
		if len(tb.Problems) != 2 || !strings.Contains(tb.Problems[0].Message, "prefix / on t.example.org is already published by Ingress alpha/site") {
			t.Fatalf("problems %+v", tb.Problems)
		}
	}
	e1 := ing("a", "x", 0, rule("u.example.org", path("/p", networkingv1.PathTypeExact, "one", 80)))
	e2 := ing("b", "y", 0, rule("u.example.org", path("/p", networkingv1.PathTypeExact, "two", 80)))
	if tb := Build(Input{Ingresses: []networkingv1.Ingress{e2, e1}}); len(tb.Problems) != 1 || !strings.Contains(tb.Problems[0].Message, "exact path /p") {
		t.Fatalf("problems %+v", tb.Problems)
	}
}

// On the same path, Exact beats Prefix; among prefixes the longer wins, and
// a host is listed once per object however many paths it has.
func TestSamePathExactBeatsPrefix(t *testing.T) {
	tb := Build(Input{Ingresses: []networkingv1.Ingress{ing("web", "s", 0, rule("s.example.org",
		path("/api", networkingv1.PathTypePrefix, "prefix", 80),
		path("/api", networkingv1.PathTypeExact, "exact", 80),
		path("/ap", networkingv1.PathTypePrefix, "short", 80),
		path("/api/v2", networkingv1.PathTypePrefix, "longer", 80),
	))}})
	for p, want := range map[string]string{"/api": "exact", "/api/x": "prefix", "/ap": "short", "/api/v2/y": "longer"} {
		if r := tb.Match("s.example.org", p); r == nil || r.Backends[0].Service != want {
			t.Errorf("%s -> %+v, want %s", p, r, want)
		}
	}
	if got := tb.BySource[Source{"Ingress", "web", "s"}]; len(got) != 1 {
		t.Fatalf("hosts per source %v", got)
	}
	// Two rules for the same hostname in one object: still one entry.
	two := Build(Input{Ingresses: []networkingv1.Ingress{ing("web", "d", 0,
		rule("d.example.org", path("/a", networkingv1.PathTypePrefix, "a", 80)),
		rule("d.example.org", path("/b", networkingv1.PathTypePrefix, "b", 80)))}})
	if got := two.BySource[Source{"Ingress", "web", "d"}]; len(got) != 1 {
		t.Fatalf("hosts per source %v", got)
	}
}
