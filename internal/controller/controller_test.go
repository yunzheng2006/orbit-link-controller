package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/orbit"
	"github.com/yunzheng2006/orbit-link-controller/internal/orbit/orbittest"
)

type env struct {
	t   *testing.T
	c   client.Client
	f   *orbittest.Fake
	r   *Reconciler
	rec *record.FakeRecorder
}

func newEnv(t *testing.T, objs ...client.Object) *env {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = gatewayv1.Install(s)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
		WithStatusSubresource(&networkingv1.Ingress{}, &gatewayv1.HTTPRoute{}, &gatewayv1.Gateway{}, &gatewayv1.GatewayClass{}).Build()
	f := orbittest.New("orb_link")
	t.Cleanup(f.Close)
	oc := orbit.New(f.URL+"/api/v1", func() (string, error) { return "orb_link", nil }, 1000, 1000)
	cfg := Config{Cluster: "kind", Namespace: "orbit-link", Replicas: 3, GatewayAPI: true}
	if err := cfg.Defaults(); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(1000)
	r := &Reconciler{Client: c, Orbit: oc, Recorder: rec, Cfg: cfg, Metrics: NewMetrics(prometheus.NewRegistry())}
	return &env{t: t, c: c, f: f, r: r, rec: rec}
}

func (e *env) sync() {
	e.t.Helper()
	if _, err := e.r.Reconcile(context.Background(), nil); err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
}

func (e *env) events() string {
	var out []string
	for {
		select {
		case ev := <-e.rec.Events:
			out = append(out, ev)
		default:
			return strings.Join(out, "\n")
		}
	}
}

func (e *env) sites() map[string]*orbittest.Site {
	e.f.Mu.Lock()
	defer e.f.Mu.Unlock()
	out := map[string]*orbittest.Site{}
	for h, s := range e.f.Sites {
		cp := *s
		out[h] = &cp
	}
	return out
}

func strp(s string) *string { return &s }

func ingress(name, class string, hosts ...string) *networkingv1.Ingress {
	i := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: "web", Name: name}, Spec: networkingv1.IngressSpec{IngressClassName: strp(class)}}
	pt := networkingv1.PathTypePrefix
	for _, h := range hosts {
		i.Spec.Rules = append(i.Spec.Rules, networkingv1.IngressRule{Host: h, IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{Path: "/", PathType: &pt, Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "app", Port: networkingv1.ServiceBackendPort{Number: 80}}}}}}}})
	}
	return i
}

func (e *env) get(obj client.Object, ns, name string) bool {
	err := e.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, obj)
	return err == nil
}

// Apply an Ingress: a tunnel, a connector, the hostname published and the
// Ingress's status pointing at the CNAME target.
func TestIngressIsPublished(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "app.example.org"), ingress("other", "nginx", "nginx.example.org"))
	e.sync()
	if len(e.f.Tunnels) != 1 {
		t.Fatalf("tunnels %d", len(e.f.Tunnels))
	}
	for _, tn := range e.f.Tunnels {
		if tn.Name != "k8s-kind" || tn.LocalURL != "http://orbit-link-router.orbit-link.svc:80" || tn.Pop != 7 {
			t.Fatalf("tunnel %+v", tn)
		}
	}
	s := e.sites()
	if len(s) != 1 || s["app.example.org"] == nil || s["app.example.org"].ManagedBy != "k8s:kind/orbit-link" {
		t.Fatalf("sites %+v", s)
	}
	var sec corev1.Secret
	if !e.get(&sec, "orbit-link", "orbit-link-connector") || !strings.HasPrefix(string(sec.Data["token"]), "lnk_") || string(sec.Data["endpoint"]) != "198.51.100.7:7844" {
		t.Fatalf("secret %v", sec.Data)
	}
	var d appsv1.Deployment
	if !e.get(&d, "orbit-link", "orbit-link-connector") || *d.Spec.Replicas != 3 || d.Spec.Template.Spec.Containers[0].Image != "cr.yunzheng.space/orbit-link:1.3.0" {
		t.Fatalf("deployment %+v", d.Spec)
	}
	sc := d.Spec.Template.Spec.Containers[0].SecurityContext
	if !*sc.ReadOnlyRootFilesystem || *sc.AllowPrivilegeEscalation || !*d.Spec.Template.Spec.SecurityContext.RunAsNonRoot {
		t.Fatal("connector is not locked down")
	}
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	if len(ing.Finalizers) != 1 || ing.Finalizers[0] != Finalizer {
		t.Fatalf("finalizers %v", ing.Finalizers)
	}
	if lb := ing.Status.LoadBalancer.Ingress; len(lb) != 1 || lb[0].Hostname != "app-example-org.edge.test" {
		t.Fatalf("status %+v", lb)
	}
	var other networkingv1.Ingress
	e.get(&other, "web", "other")
	if len(other.Finalizers) != 0 || len(other.Status.LoadBalancer.Ingress) != 0 {
		t.Fatal("another class's Ingress was touched")
	}
	if ev := e.events(); !strings.Contains(ev, "Published app.example.org is published; point it at app-example-org.edge.test") {
		t.Fatalf("events %q", ev)
	}
	// Idempotent: a second sync creates nothing and says nothing new. The
	// API stores local_url without the default port; that is not a change.
	for _, tn := range e.f.Tunnels {
		tn.LocalURL = "http://orbit-link-router.orbit-link.svc"
	}
	calls := e.f.Calls["POST /link/tunnels"]
	e.sync()
	if e.f.Calls["POST /link/tunnels"] != calls || len(e.f.Tunnels) != 1 {
		t.Fatal("a second sync created another tunnel")
	}
	if e.f.Calls["PATCH /link/tunnels/1"] != 0 {
		t.Fatal("an unchanged local address was patched on every sync")
	}
	if ev := e.events(); ev != "" {
		t.Fatalf("a no-op sync repeated its events: %q", ev)
	}
}

// Changing hosts adds and removes; deleting removes and releases.
func TestHostChangeAndDelete(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "a.example.org", "b.example.org"))
	e.sync()
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	ing.Spec.Rules = ing.Spec.Rules[1:] // drop a
	ing.Spec.Rules = append(ing.Spec.Rules, ingress("x", "orbit-link", "c.example.org").Spec.Rules...)
	if err := e.c.Update(context.Background(), &ing); err != nil {
		t.Fatal(err)
	}
	e.sync()
	s := e.sites()
	if s["a.example.org"] != nil || s["b.example.org"] == nil || s["c.example.org"] == nil {
		t.Fatalf("after edit %v", keys(s))
	}
	e.get(&ing, "web", "site")
	if err := e.c.Delete(context.Background(), &ing); err != nil {
		t.Fatal(err)
	}
	if !e.get(&ing, "web", "site") {
		t.Fatal("the finalizer did not hold the Ingress")
	}
	e.sync()
	if len(e.sites()) != 0 {
		t.Fatalf("sites left %v", keys(e.sites()))
	}
	if e.get(&ing, "web", "site") {
		t.Fatal("the finalizer was not released")
	}
}

// If Orbit cannot be reached, a deleted Ingress keeps its finalizer: its
// hostname must not be left published with nothing in the cluster behind it.
func TestFinalizerHeldWhileOrbitIsDown(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "a.example.org"))
	e.sync()
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	_ = e.c.Delete(context.Background(), &ing)
	e.f.Mu.Lock()
	e.f.Fail = 1000
	e.f.Mu.Unlock()
	e.r.Orbit.MaxTries = 1
	if _, err := e.r.Reconcile(context.Background(), nil); err == nil {
		t.Fatal("reconcile succeeded with Orbit down")
	}
	if !e.get(&ing, "web", "site") {
		t.Fatal("the Ingress was released while its hostname is still published")
	}
	e.f.Mu.Lock()
	e.f.Fail = 0
	e.f.Mu.Unlock()
	e.sync()
	if e.get(&ing, "web", "site") || len(e.sites()) != 0 {
		t.Fatal("not cleaned up once Orbit answered")
	}
}

// Moving an Ingress to another class unpublishes it and lets go.
func TestClassChangeUnpublishes(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "a.example.org"))
	e.sync()
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	ing.Spec.IngressClassName = strp("nginx")
	_ = e.c.Update(context.Background(), &ing)
	e.sync()
	e.get(&ing, "web", "site")
	if len(e.sites()) != 0 || len(ing.Finalizers) != 0 {
		t.Fatalf("sites %v finalizers %v", keys(e.sites()), ing.Finalizers)
	}
}

// A hand-made site with the same name is never taken over; quota is reported.
func TestConflictAndQuotaAreEvents(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "hand.example.org", "one.example.org", "two.example.org"))
	e.f.Sites["hand.example.org"] = &orbittest.Site{Mine: true, Target: "hand.edge.test"}
	e.f.Quota = 2 // hand + one
	e.sync()
	s := e.sites()
	if s["hand.example.org"].ManagedBy != "" || s["hand.example.org"].Tunnel != 0 {
		t.Fatal("a hand-made site was taken over")
	}
	if s["one.example.org"] == nil || s["two.example.org"] != nil {
		t.Fatalf("sites %v", keys(s))
	}
	ev := e.events()
	if !strings.Contains(ev, "Warning HostnameConflict hand.example.org") || !strings.Contains(ev, "Warning QuotaExceeded two.example.org") {
		t.Fatalf("events %q", ev)
	}
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	if lb := ing.Status.LoadBalancer.Ingress; len(lb) != 1 || lb[0].Hostname != "one-example-org.edge.test" {
		t.Fatalf("only the published hostname's target belongs in status: %+v", lb)
	}
}

// A lost connector Secret: the same tunnel is found by name and its
// credential rotated; no second tunnel.
func TestLostSecretRotatesNotRecreates(t *testing.T) {
	e := newEnv(t)
	e.sync()
	var sec corev1.Secret
	e.get(&sec, "orbit-link", "orbit-link-connector")
	old := string(sec.Data["token"])
	_ = e.c.Delete(context.Background(), &sec)
	e.sync()
	e.get(&sec, "orbit-link", "orbit-link-connector")
	if len(e.f.Tunnels) != 1 || string(sec.Data["token"]) == old || e.f.Calls["POST /link/tunnels/1/token"] != 1 {
		t.Fatalf("tunnels %d rotate %d", len(e.f.Tunnels), e.f.Calls["POST /link/tunnels/1/token"])
	}
	var d appsv1.Deployment
	e.get(&d, "orbit-link", "orbit-link-connector")
	if d.Spec.Template.Annotations["link.yunzheng.space/credential-rev"] == "" {
		t.Fatal("the connector pods are not rolled on a new credential")
	}
}

func gatewayObjs() []client.Object {
	from := gatewayv1.NamespacesFromAll
	port := gatewayv1.PortNumber(80)
	return []client.Object{
		&gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "orbit-link"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: "yunzheng.space/orbit-link"}},
		&gatewayv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Spec: gatewayv1.GatewayClassSpec{ControllerName: "example.com/other"}},
		&gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "edge"}, Spec: gatewayv1.GatewaySpec{GatewayClassName: "orbit-link",
			Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80,
				AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{From: &from}}}}}},
		&gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "store"}, Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Namespace: (*gatewayv1.Namespace)(strp("infra")), Name: "edge"}}},
			Hostnames:       []gatewayv1.Hostname{"shop.example.org"},
			Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{Name: "web", Port: &port}}}}}}}},
	}
}

func TestGatewayAPIRouteIsPublished(t *testing.T) {
	e := newEnv(t, gatewayObjs()...)
	e.sync()
	if e.sites()["shop.example.org"] == nil {
		t.Fatalf("sites %v", keys(e.sites()))
	}
	var rt gatewayv1.HTTPRoute
	e.get(&rt, "shop", "store")
	if len(rt.Status.Parents) != 1 || string(rt.Status.Parents[0].ControllerName) != "yunzheng.space/orbit-link" {
		t.Fatalf("parents %+v", rt.Status.Parents)
	}
	for _, c := range rt.Status.Parents[0].Conditions {
		if c.Status != metav1.ConditionTrue {
			t.Fatalf("condition %+v", c)
		}
	}
	var g gatewayv1.Gateway
	e.get(&g, "infra", "edge")
	if len(g.Status.Addresses) != 1 || g.Status.Addresses[0].Value != "shop-example-org.edge.test" || g.Status.Listeners[0].AttachedRoutes != 1 {
		t.Fatalf("gateway status %+v", g.Status)
	}
	var gc gatewayv1.GatewayClass
	e.get(&gc, "", "orbit-link")
	if len(gc.Status.Conditions) == 0 || gc.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("class %+v", gc.Status)
	}
	var other gatewayv1.GatewayClass
	e.get(&other, "", "other")
	if len(other.Status.Conditions) != 0 {
		t.Fatal("another controller's GatewayClass got a status")
	}
	// Deleting the route unpublishes it.
	_ = e.c.Delete(context.Background(), &rt)
	e.sync()
	if len(e.sites()) != 0 || e.get(&rt, "shop", "store") {
		t.Fatal("route not cleaned up")
	}
}

func TestCleanupRemovesEverything(t *testing.T) {
	objs := append(gatewayObjs(), ingress("site", "orbit-link", "a.example.org"))
	e := newEnv(t, objs...)
	e.sync()
	if len(e.sites()) != 2 {
		t.Fatalf("sites %v", keys(e.sites()))
	}
	if err := Cleanup(context.Background(), e.c, e.r.Orbit, &e.r.Cfg, "", t.Logf); err != nil {
		t.Fatal(err)
	}
	if len(e.sites()) != 0 || len(e.f.Tunnels) != 0 {
		t.Fatalf("left: sites %v tunnels %d", keys(e.sites()), len(e.f.Tunnels))
	}
	var d appsv1.Deployment
	var sec corev1.Secret
	if e.get(&d, "orbit-link", "orbit-link-connector") || e.get(&sec, "orbit-link", "orbit-link-connector") {
		t.Fatal("connector left behind")
	}
	var ing networkingv1.Ingress
	e.get(&ing, "web", "site")
	if len(ing.Finalizers) != 0 || len(ing.Status.LoadBalancer.Ingress) != 0 {
		t.Fatal("finalizer or status left on the Ingress")
	}
}

func keys(m map[string]*orbittest.Site) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// staleSecrets is a cached client that has not seen the connector Secret yet.
type staleSecrets struct{ client.Client }

func (s staleSecrets) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

// The cache lagging behind a Secret just created must not rotate the
// credential: the Secret would keep the old one and every connector would be
// refused. Found on kind, 2026-09-28.
func TestStaleCacheDoesNotRotateTheCredential(t *testing.T) {
	e := newEnv(t, ingress("site", "orbit-link", "a.example.org"))
	real := e.r.Client
	e.r.Client = staleSecrets{real}
	e.r.APIReader = real
	e.sync()
	e.sync()
	if n := e.f.Calls["POST /link/tunnels/1/token"]; n != 0 {
		t.Fatalf("credential rotated %d times", n)
	}
	var sec corev1.Secret
	_ = real.Get(context.Background(), types.NamespacedName{Namespace: "orbit-link", Name: "orbit-link-connector"}, &sec)
	if string(sec.Data["token"]) != e.f.Tunnels[1].Token {
		t.Fatalf("the Secret holds %q, Orbit accepts %q", sec.Data["token"], e.f.Tunnels[1].Token)
	}
}

// ---- added after the mutation run (docs/ablation/2026-09-28-link-k8s.md) ----

// Every published object gets the finalizer, not just the first one; a
// second sync keeps live, finalized routes published (a live object holding
// our finalizer is not "being deleted").
func TestEveryObjectFinalizedAndKeptOnResync(t *testing.T) {
	objs := append(gatewayObjs(), ingress("one", "orbit-link", "one.example.org"), ingress("two", "orbit-link", "two.example.org"))
	e := newEnv(t, objs...)
	e.sync()
	e.sync()
	for _, n := range []string{"one", "two"} {
		var ing networkingv1.Ingress
		e.get(&ing, "web", n)
		if len(ing.Finalizers) != 1 {
			t.Fatalf("ingress %s finalizers %v", n, ing.Finalizers)
		}
	}
	var g gatewayv1.Gateway
	var rt gatewayv1.HTTPRoute
	e.get(&g, "infra", "edge")
	e.get(&rt, "shop", "store")
	if len(g.Finalizers) != 1 || len(rt.Finalizers) != 1 {
		t.Fatalf("gateway %v route %v", g.Finalizers, rt.Finalizers)
	}
	s := e.sites()
	if s["shop.example.org"] == nil || s["one.example.org"] == nil || s["two.example.org"] == nil {
		t.Fatalf("after a resync: %v", keys(s))
	}
}

// A route on another controller's Gateway is none of our business: no
// finalizer, no status.
func TestOtherControllersRouteUntouched(t *testing.T) {
	port := gatewayv1.PortNumber(80)
	objs := append(gatewayObjs(),
		&gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "nginx"}, Spec: gatewayv1.GatewaySpec{GatewayClassName: "other",
			Listeners: []gatewayv1.Listener{{Name: "http", Protocol: gatewayv1.HTTPProtocolType, Port: 80}}}},
		&gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "theirs"}, Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{Name: "nginx"}}},
			Hostnames:       []gatewayv1.Hostname{"theirs.example.org"},
			Rules: []gatewayv1.HTTPRouteRule{{BackendRefs: []gatewayv1.HTTPBackendRef{{BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{Name: "web", Port: &port}}}}}}}})
	e := newEnv(t, objs...)
	e.sync()
	var rt gatewayv1.HTTPRoute
	e.get(&rt, "infra", "theirs")
	if len(rt.Finalizers) != 0 || len(rt.Status.Parents) != 0 || e.sites()["theirs.example.org"] != nil {
		t.Fatalf("another controller's route was touched: %v %v", rt.Finalizers, rt.Status.Parents)
	}
	var g gatewayv1.Gateway
	e.get(&g, "infra", "nginx")
	if len(g.Finalizers) != 0 || len(g.Status.Conditions) != 0 {
		t.Fatal("another controller's Gateway was touched")
	}
}

// A failed sync is said: a Warning event on the object, a False condition on
// the route; the metrics count it. A good sync fills the hostname gauge and
// a released object says so.
func TestFailuresAreReportedAndCounted(t *testing.T) {
	e := newEnv(t, gatewayObjs()...)
	e.f.Mu.Lock()
	e.f.Fail = 1000
	e.f.Mu.Unlock()
	e.r.Orbit.MaxTries = 1
	_, _ = e.r.Reconcile(context.Background(), nil)
	if got := testutil.ToFloat64(e.r.Metrics.SyncErrors); got != 1 {
		t.Fatalf("sync errors %v", got)
	}
	if ev := e.events(); !strings.Contains(ev, "Warning TunnelUnavailable") {
		t.Fatalf("events %q", ev)
	}
	var rt gatewayv1.HTTPRoute
	e.get(&rt, "shop", "store")
	if len(rt.Status.Parents) != 1 || rt.Status.Parents[0].Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("route status while Orbit is down: %+v", rt.Status.Parents)
	}
	// The tunnel exists; now the hostname call alone fails.
	e.f.Mu.Lock()
	e.f.Fail = 0
	e.f.Mu.Unlock()
	e.sync()
	e.events()
	if got := testutil.ToFloat64(e.r.Metrics.Hostnames.WithLabelValues("created")); got != 1 {
		t.Fatalf("hostname gauge %v", got)
	}
	e.get(&rt, "shop", "store")
	_ = e.c.Delete(context.Background(), &rt)
	e.sync()
	if ev := e.events(); !strings.Contains(ev, "Normal Removed its hostnames were removed from Orbit") {
		t.Fatalf("events %q", ev)
	}
}

// Cleanup stops the controller first, finds its tunnel by the id on file even
// after a rename, never deletes somebody else's tunnel, and leaves another
// controller's Ingress status alone.
func TestCleanupIsPreciseAboutWhatItRemoves(t *testing.T) {
	two := int32(2)
	ctl := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "orbit-link", Name: "orbit-link-controller"},
		Spec: appsv1.DeploymentSpec{Replicas: &two}}
	other := ingress("theirs", "nginx", "nginx.example.org")
	other.Status.LoadBalancer.Ingress = []networkingv1.IngressLoadBalancerIngress{{IP: "192.0.2.10"}}
	e := newEnv(t, ctl, other, ingress("site", "orbit-link", "a.example.org"))
	e.sync()
	// Somebody's own tunnel on the same account, and ours renamed by hand.
	e.f.Mu.Lock()
	e.f.Tunnels[99] = &orbittest.Tunnel{ID: 99, Name: "shop-origin"}
	e.f.Tunnels[1].Name = "renamed-by-hand"
	e.f.Mu.Unlock()
	if err := Cleanup(context.Background(), e.c, e.r.Orbit, &e.r.Cfg, "orbit-link-controller", t.Logf); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.f.Tunnels[1]; ok {
		t.Fatal("our renamed tunnel was not found by its id")
	}
	if _, ok := e.f.Tunnels[99]; !ok {
		t.Fatal("somebody else's tunnel was deleted")
	}
	var d appsv1.Deployment
	e.get(&d, "orbit-link", "orbit-link-controller")
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		t.Fatal("the controller was not stopped before cleaning up")
	}
	var ing networkingv1.Ingress
	e.get(&ing, "web", "theirs")
	if len(ing.Status.LoadBalancer.Ingress) != 1 {
		t.Fatal("another controller's Ingress status was cleared")
	}
	// With the Secret gone, the tunnel is found by name — and only by name.
	e2 := newEnv(t)
	e2.sync()
	var sec corev1.Secret
	e2.get(&sec, "orbit-link", "orbit-link-connector")
	_ = e2.c.Delete(context.Background(), &sec)
	e2.f.Mu.Lock()
	e2.f.Tunnels[99] = &orbittest.Tunnel{ID: 99, Name: "shop-origin"}
	e2.f.Mu.Unlock()
	if err := Cleanup(context.Background(), e2.c, e2.r.Orbit, &e2.r.Cfg, "", t.Logf); err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.f.Tunnels[1]; ok {
		t.Fatal("ours not found by name")
	}
	if _, ok := e2.f.Tunnels[99]; !ok {
		t.Fatal("somebody else's tunnel was deleted when looking up by name")
	}
}
