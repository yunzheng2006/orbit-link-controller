// Package routes turns Ingresses and Gateway API HTTPRoutes into one routing
// table: hostname → path rules → Service backends. It is pure (no client, no
// clock) so the rules that decide what is published can be tested alone.
//
// Orbit serves exact hostnames, and TLS is terminated at the Orbit edge, so:
// a rule without a hostname, or with a wildcard, is reported and skipped; TLS
// sections are accepted and noted (the edge issues the certificate).
package routes

import (
	"fmt"
	"sort"
	"strings"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Source names the object a rule came from.
type Source struct {
	Kind      string // Ingress | HTTPRoute
	Namespace string
	Name      string
}

func (s Source) String() string { return s.Kind + " " + s.Namespace + "/" + s.Name }

// Backend is a Service port.
type Backend struct {
	Namespace string
	Service   string
	Port      int32  // 0 when PortName is used
	PortName  string // Ingress only
	Weight    int32
}

// Rule is one path on one hostname.
type Rule struct {
	Host     string
	Exact    bool
	Path     string // always starts with "/"
	Backends []Backend
	Source   Source
	created  time.Time
	fallback bool // an Ingress defaultBackend: only when no rule matches
}

// Problem is something in an object that cannot be published.
type Problem struct {
	Source  Source
	Host    string
	Reason  string // short CamelCase, for conditions and events
	Message string
}

// ParentStatus is the outcome for one parentRef of an HTTPRoute.
type ParentStatus struct {
	Ref            gatewayv1.ParentReference
	Accepted       bool
	AcceptedReason string
	AcceptedMsg    string
	Resolved       bool
	ResolvedReason string
	ResolvedMsg    string
}

// Table is the routing table and what it was built from.
type Table struct {
	Rules    map[string][]Rule   // by hostname, most specific first
	BySource map[Source][]string // hostnames each object publishes
	Problems []Problem
	Parents  map[Source][]ParentStatus // HTTPRoutes only
	// Gateways: for each Gateway (namespace/name), the hostnames of routes attached to it.
	GatewayHosts map[string][]string
}

// Hostnames is every hostname in the table, sorted.
func (t *Table) Hostnames() []string {
	out := make([]string, 0, len(t.Rules))
	for h := range t.Rules {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func newTable() *Table {
	return &Table{Rules: map[string][]Rule{}, BySource: map[Source][]string{}, Parents: map[Source][]ParentStatus{}, GatewayHosts: map[string][]string{}}
}

// CleanHost lower-cases a hostname and says whether Orbit can serve it.
func CleanHost(h string) (string, string) {
	h = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(h)), ".")
	switch {
	case h == "":
		return "", "a rule without a hostname cannot be published; Orbit serves named hostnames"
	case strings.Contains(h, "*"):
		return "", fmt.Sprintf("wildcard hostname %q is not supported; list each hostname", h)
	case len(h) > 253 || !strings.Contains(h, "."):
		return "", fmt.Sprintf("%q is not a fully qualified hostname", h)
	}
	for _, l := range strings.Split(h, ".") {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return "", fmt.Sprintf("%q is not a valid hostname", h)
		}
		for i := 0; i < len(l); i++ {
			c := l[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", fmt.Sprintf("%q is not a valid hostname", h)
			}
		}
	}
	return h, ""
}

// cleanPath normalises a rule path: leading slash, no trailing slash except "/".
func cleanPath(p string) string {
	if p == "" || p[0] != '/' {
		p = "/" + p
	}
	for len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

// Input is everything the table is built from. Objects being deleted must
// already be left out: their hostnames are exactly what the sync removes.
type Input struct {
	Ingresses      []networkingv1.Ingress
	HTTPRoutes     []gatewayv1.HTTPRoute
	Gateways       []gatewayv1.Gateway // only those of our GatewayClass(es)
	ControllerName gatewayv1.GatewayController
}

// Build makes the table.
func Build(in Input) *Table {
	t := newTable()
	var all []Rule
	for i := range in.Ingresses {
		all = append(all, ingressRules(t, &in.Ingresses[i])...)
	}
	gws := map[string]*gatewayv1.Gateway{}
	for i := range in.Gateways {
		g := &in.Gateways[i]
		gws[g.Namespace+"/"+g.Name] = g
	}
	for i := range in.HTTPRoutes {
		all = append(all, routeRules(t, &in.HTTPRoutes[i], gws)...)
	}
	// Conflicts: the same host, match kind and path from two objects. The
	// older object keeps it (then namespace/name), the way Ingress
	// controllers and the Gateway API resolve ties.
	sort.SliceStable(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if !a.created.Equal(b.created) {
			return a.created.Before(b.created)
		}
		if a.Source.Namespace != b.Source.Namespace {
			return a.Source.Namespace < b.Source.Namespace
		}
		if a.Source.Name != b.Source.Name {
			return a.Source.Name < b.Source.Name
		}
		return a.Source.Kind < b.Source.Kind
	})
	type key struct {
		host, path string
		exact, fb  bool
	}
	owner := map[key]Source{}
	hostSeen := map[Source]map[string]bool{}
	for _, r := range all {
		k := key{r.Host, r.Path, r.Exact, r.fallback}
		if o, ok := owner[k]; ok && o != r.Source {
			kind := "prefix"
			if r.Exact {
				kind = "exact path"
			}
			t.Problems = append(t.Problems, Problem{Source: r.Source, Host: r.Host, Reason: "Conflict",
				Message: fmt.Sprintf("%s %s on %s is already published by %s", kind, r.Path, r.Host, o)})
			continue
		}
		owner[k] = r.Source
		t.Rules[r.Host] = append(t.Rules[r.Host], r)
		if hostSeen[r.Source] == nil {
			hostSeen[r.Source] = map[string]bool{}
		}
		if !hostSeen[r.Source][r.Host] {
			hostSeen[r.Source][r.Host] = true
			t.BySource[r.Source] = append(t.BySource[r.Source], r.Host)
		}
	}
	for h, rs := range t.Rules {
		sort.SliceStable(rs, func(i, j int) bool { return moreSpecific(rs[i], rs[j]) })
		t.Rules[h] = rs
	}
	for s := range t.BySource {
		sort.Strings(t.BySource[s])
	}
	for g := range t.GatewayHosts {
		t.GatewayHosts[g] = uniqSorted(t.GatewayHosts[g])
	}
	return t
}

// moreSpecific orders rules for matching: real rules before an Ingress
// default backend; Exact before Prefix on the same path; longer paths first.
func moreSpecific(a, b Rule) bool {
	if a.fallback != b.fallback {
		return !a.fallback
	}
	if len(a.Path) != len(b.Path) {
		return len(a.Path) > len(b.Path)
	}
	if a.Exact != b.Exact {
		return a.Exact
	}
	return a.Path < b.Path
}

// Match finds the rule for a request, or nil.
func (t *Table) Match(host, path string) *Rule {
	h, _ := CleanHost(stripPort(host))
	rs := t.Rules[h]
	if path == "" {
		path = "/"
	}
	for i := range rs {
		r := &rs[i]
		if r.Exact {
			if path == r.Path {
				return r
			}
			continue
		}
		if prefixMatch(r.Path, path) {
			return r
		}
	}
	return nil
}

// prefixMatch is element-wise: /foo matches /foo and /foo/bar, not /foobar.
func prefixMatch(prefix, path string) bool {
	if prefix == "/" {
		return true
	}
	p := path
	for len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	return p == prefix || strings.HasPrefix(path, prefix+"/")
}

func stripPort(h string) string {
	if i := strings.LastIndexByte(h, ':'); i > 0 && !strings.Contains(h[i:], "]") {
		return h[:i]
	}
	return h
}

func uniqSorted(in []string) []string {
	sort.Strings(in)
	out := in[:0]
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}

func ingressBackend(ns string, b *networkingv1.IngressBackend) (Backend, string) {
	if b == nil || b.Service == nil {
		return Backend{}, "only Service backends are supported"
	}
	be := Backend{Namespace: ns, Service: b.Service.Name, Port: b.Service.Port.Number, PortName: b.Service.Port.Name, Weight: 1}
	if be.Port == 0 && be.PortName == "" {
		return Backend{}, "the backend names no port"
	}
	return be, ""
}

func ingressRules(t *Table, ing *networkingv1.Ingress) []Rule {
	src := Source{"Ingress", ing.Namespace, ing.Name}
	created := ing.CreationTimestamp.Time
	var out []Rule
	var hosts []string
	for _, r := range ing.Spec.Rules {
		h, why := CleanHost(r.Host)
		if why != "" {
			t.Problems = append(t.Problems, Problem{Source: src, Host: r.Host, Reason: "UnsupportedHost", Message: why})
			continue
		}
		hosts = append(hosts, h)
		if r.HTTP == nil {
			continue
		}
		for _, p := range r.HTTP.Paths {
			be, why := ingressBackend(ing.Namespace, &p.Backend)
			if why != "" {
				t.Problems = append(t.Problems, Problem{Source: src, Host: h, Reason: "UnsupportedBackend", Message: why})
				continue
			}
			exact := p.PathType != nil && *p.PathType == networkingv1.PathTypeExact
			path := p.Path
			if path == "" {
				path = "/"
			}
			if !strings.HasPrefix(path, "/") {
				t.Problems = append(t.Problems, Problem{Source: src, Host: h, Reason: "InvalidPath", Message: fmt.Sprintf("path %q must start with /", path)})
				continue
			}
			if !exact {
				path = cleanPath(path)
			}
			out = append(out, Rule{Host: h, Exact: exact, Path: path, Backends: []Backend{be}, Source: src, created: created})
		}
	}
	if db := ing.Spec.DefaultBackend; db != nil {
		be, why := ingressBackend(ing.Namespace, db)
		if why != "" {
			t.Problems = append(t.Problems, Problem{Source: src, Reason: "UnsupportedBackend", Message: "default backend: " + why})
		} else {
			for _, h := range uniqSorted(append([]string(nil), hosts...)) {
				out = append(out, Rule{Host: h, Path: "/", Backends: []Backend{be}, Source: src, created: created, fallback: true})
			}
			if len(hosts) == 0 {
				t.Problems = append(t.Problems, Problem{Source: src, Reason: "UnsupportedHost",
					Message: "a default backend without any hostname cannot be published; add a rule with a host"})
			}
		}
	}
	return out
}

func routeRules(t *Table, rt *gatewayv1.HTTPRoute, gws map[string]*gatewayv1.Gateway) []Rule {
	src := Source{"HTTPRoute", rt.Namespace, rt.Name}
	created := rt.CreationTimestamp.Time
	var out []Rule
	var parents []ParentStatus
	for _, pr := range rt.Spec.ParentRefs {
		if pr.Group != nil && *pr.Group != gatewayv1.GroupName || pr.Kind != nil && *pr.Kind != "Gateway" {
			continue
		}
		ns := rt.Namespace
		if pr.Namespace != nil {
			ns = string(*pr.Namespace)
		}
		g := gws[ns+"/"+string(pr.Name)]
		if g == nil {
			continue // not one of ours: another controller reports on it
		}
		ps := ParentStatus{Ref: pr, Accepted: true, AcceptedReason: "Accepted", Resolved: true, ResolvedReason: "ResolvedRefs"}
		// Which listeners admit this route, and the hostnames they allow.
		var allow []string // "" = any hostname
		admitted := false
		for _, l := range g.Spec.Listeners {
			if pr.SectionName != nil && *pr.SectionName != l.Name {
				continue
			}
			if pr.Port != nil && *pr.Port != l.Port {
				continue
			}
			if l.Protocol != gatewayv1.HTTPProtocolType && l.Protocol != gatewayv1.HTTPSProtocolType {
				continue
			}
			from := gatewayv1.NamespacesFromSame
			if l.AllowedRoutes != nil && l.AllowedRoutes.Namespaces != nil && l.AllowedRoutes.Namespaces.From != nil {
				from = *l.AllowedRoutes.Namespaces.From
			}
			if from == gatewayv1.NamespacesFromSame && rt.Namespace != g.Namespace || from == gatewayv1.NamespacesFromSelector {
				continue
			}
			admitted = true
			if l.Hostname == nil {
				allow = append(allow, "")
			} else {
				allow = append(allow, strings.ToLower(string(*l.Hostname)))
			}
		}
		if !admitted {
			ps.Accepted, ps.AcceptedReason, ps.AcceptedMsg = false, "NotAllowedByListeners", "no HTTP/HTTPS listener of this Gateway admits routes from this namespace (Same and All are supported)"
			parents = append(parents, ps)
			continue
		}
		var hosts []string
		if len(rt.Spec.Hostnames) == 0 {
			for _, a := range allow {
				if a != "" && !strings.HasPrefix(a, "*") {
					hosts = append(hosts, a)
				}
			}
			if len(hosts) == 0 {
				ps.Accepted, ps.AcceptedReason, ps.AcceptedMsg = false, "UnsupportedValue", "the route names no hostname and its listener has none either; Orbit serves named hostnames"
				parents = append(parents, ps)
				continue
			}
		}
		var bad []string
		for _, rh := range rt.Spec.Hostnames {
			h, why := CleanHost(string(rh))
			if why != "" {
				bad = append(bad, why)
				continue
			}
			if !listenerAllows(allow, h) {
				bad = append(bad, fmt.Sprintf("%s is not allowed by the Gateway's listeners", h))
				continue
			}
			hosts = append(hosts, h)
		}
		hosts = uniqSorted(hosts)
		if len(hosts) == 0 {
			ps.Accepted, ps.AcceptedReason = false, "NoMatchingListenerHostname"
			ps.AcceptedMsg = strings.Join(bad, "; ")
			parents = append(parents, ps)
			continue
		}
		if len(bad) > 0 {
			t.Problems = append(t.Problems, Problem{Source: src, Reason: "UnsupportedHost", Message: strings.Join(bad, "; ")})
		}
		var unsupported []string
		for ri, rule := range rt.Spec.Rules {
			if len(rule.Filters) > 0 {
				unsupported = append(unsupported, fmt.Sprintf("rule %d: filters are not supported", ri))
				continue
			}
			var bes []Backend
			var refErr string
			for _, br := range rule.BackendRefs {
				if len(br.Filters) > 0 {
					unsupported = append(unsupported, fmt.Sprintf("rule %d: backend filters are not supported", ri))
					continue
				}
				if br.Group != nil && *br.Group != "" || br.Kind != nil && *br.Kind != "Service" {
					refErr = "only Service backends are supported"
					ps.ResolvedReason = "InvalidKind"
					continue
				}
				bns := rt.Namespace
				if br.Namespace != nil && string(*br.Namespace) != rt.Namespace {
					refErr = fmt.Sprintf("backend %s/%s is in another namespace (ReferenceGrant is not supported)", *br.Namespace, br.Name)
					ps.ResolvedReason = "RefNotPermitted"
					continue
				}
				if br.Port == nil {
					refErr = fmt.Sprintf("backend %s names no port", br.Name)
					ps.ResolvedReason = "UnsupportedValue"
					continue
				}
				w := int32(1)
				if br.Weight != nil {
					w = *br.Weight
				}
				if w <= 0 {
					continue
				}
				bes = append(bes, Backend{Namespace: bns, Service: string(br.Name), Port: int32(*br.Port), Weight: w})
			}
			if refErr != "" {
				ps.Resolved, ps.ResolvedMsg = false, refErr
			}
			if len(bes) == 0 {
				continue
			}
			matches := rule.Matches
			if len(matches) == 0 {
				matches = []gatewayv1.HTTPRouteMatch{{}}
			}
			for _, m := range matches {
				if len(m.Headers) > 0 || len(m.QueryParams) > 0 || m.Method != nil {
					unsupported = append(unsupported, fmt.Sprintf("rule %d: header, query and method matches are not supported", ri))
					continue
				}
				exact, path := false, "/"
				if m.Path != nil {
					if m.Path.Value != nil {
						path = *m.Path.Value
					}
					if m.Path.Type != nil {
						switch *m.Path.Type {
						case gatewayv1.PathMatchExact:
							exact = true
						case gatewayv1.PathMatchPathPrefix:
						default:
							unsupported = append(unsupported, fmt.Sprintf("rule %d: %s path matches are not supported", ri, *m.Path.Type))
							continue
						}
					}
				}
				if !strings.HasPrefix(path, "/") {
					unsupported = append(unsupported, fmt.Sprintf("rule %d: path %q must start with /", ri, path))
					continue
				}
				if !exact {
					path = cleanPath(path)
				}
				for _, h := range hosts {
					out = append(out, Rule{Host: h, Exact: exact, Path: path, Backends: bes, Source: src, created: created})
				}
			}
		}
		if len(unsupported) > 0 {
			t.Problems = append(t.Problems, Problem{Source: src, Reason: "UnsupportedValue", Message: strings.Join(uniqSorted(unsupported), "; ")})
			ps.AcceptedMsg = "some rules were skipped: " + strings.Join(uniqSorted(unsupported), "; ")
		}
		key := g.Namespace + "/" + g.Name
		t.GatewayHosts[key] = append(t.GatewayHosts[key], hosts...)
		parents = append(parents, ps)
	}
	if len(parents) > 0 {
		t.Parents[src] = parents
	}
	return out
}

// listenerAllows: a listener without hostname allows any; "*.example.com"
// allows one more label; otherwise the names must be equal.
func listenerAllows(allow []string, h string) bool {
	for _, a := range allow {
		switch {
		case a == "" || a == h:
			return true
		case strings.HasPrefix(a, "*."):
			suf := a[1:]
			if strings.HasSuffix(h, suf) && !strings.Contains(strings.TrimSuffix(h, suf), ".") {
				return true
			}
		}
	}
	return false
}
