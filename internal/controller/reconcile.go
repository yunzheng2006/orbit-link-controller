// Package controller keeps Orbit Link in step with the cluster: one tunnel per
// installation, a connector Deployment that forwards to the in-cluster router,
// and the tunnel's hostnames = every hostname in Ingresses of our class and in
// HTTPRoutes attached to Gateways of our class.
//
// The whole state is reconciled under ONE key: hostnames from all objects go
// into one declarative API call (PUT /link/tunnels/{id}/hostnames), which is
// idempotent and only ever touches sites carrying this installation's
// managed_by tag. Objects being deleted are left out of that list and lose
// their finalizer only after the call succeeded.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/orbit"
	"github.com/yunzheng2006/orbit-link-controller/internal/routes"
)

// SyncKey is the single work item.
var SyncKey = types.NamespacedName{Name: "orbit-link-sync"}

const specAnnotation = "link.yunzheng.space/spec"

// Resync is how often the state is refreshed with nothing changing (DNS
// verification and connection state live in Orbit).
var Resync = 5 * time.Minute

// Reconciler is the leader's loop.
type Reconciler struct {
	Client client.Client
	// APIReader reads straight from the API server. The connector Secret is
	// read through it: the cache can lag a Secret created a moment ago, and
	// "no Secret" makes the controller rotate the credential — the Secret
	// kept the old one and every connector was refused (kind, 2026-09-28).
	APIReader client.Reader
	Orbit    *orbit.Client
	Recorder record.EventRecorder
	Cfg      Config
	Metrics  *Metrics

	mu     sync.Mutex
	events map[string]string // object key -> digest of what was last said about it
	tunnel int64             // cached tunnel id
}

// Objects are what the controller manages in the cluster, read once per sync.
type Objects struct {
	Ingresses      []networkingv1.Ingress // ours, live
	DelIngresses   []networkingv1.Ingress // ours, being deleted, still holding our finalizer
	Classes        []string               // our GatewayClass names
	Gateways       []gatewayv1.Gateway    // ours, live
	DelGateways    []gatewayv1.Gateway
	HTTPRoutes     []gatewayv1.HTTPRoute // attached to one of ours, live
	DelHTTPRoutes  []gatewayv1.HTTPRoute
	GatewayClasses []gatewayv1.GatewayClass
}

// IsOurIngress: spec.ingressClassName, or the legacy annotation.
func (c *Config) IsOurIngress(ing *networkingv1.Ingress) bool {
	if ing.Spec.IngressClassName != nil {
		return *ing.Spec.IngressClassName == c.IngressClass
	}
	return ing.Annotations["kubernetes.io/ingress.class"] == c.IngressClass
}

func deleting(o metav1.Object) bool { return o.GetDeletionTimestamp() != nil }

// Gather reads the cluster.
func Gather(ctx context.Context, c client.Reader, cfg *Config) (*Objects, error) {
	o := &Objects{}
	var il networkingv1.IngressList
	if err := c.List(ctx, &il); err != nil {
		return nil, err
	}
	for _, ing := range il.Items {
		ours := cfg.IsOurIngress(&ing)
		switch {
		case deleting(&ing) && controllerutil.ContainsFinalizer(&ing, Finalizer):
			o.DelIngresses = append(o.DelIngresses, ing)
		case deleting(&ing):
		case ours:
			o.Ingresses = append(o.Ingresses, ing)
		case controllerutil.ContainsFinalizer(&ing, Finalizer):
			// Moved to another class: publish nothing for it, then let go.
			o.DelIngresses = append(o.DelIngresses, ing)
		}
	}
	if !cfg.GatewayAPI {
		return o, nil
	}
	var gcl gatewayv1.GatewayClassList
	if err := c.List(ctx, &gcl); err != nil {
		return nil, err
	}
	classes := map[string]bool{}
	for _, gc := range gcl.Items {
		if string(gc.Spec.ControllerName) == cfg.ControllerName {
			classes[gc.Name] = true
			o.Classes = append(o.Classes, gc.Name)
			o.GatewayClasses = append(o.GatewayClasses, gc)
		}
	}
	var gl gatewayv1.GatewayList
	if err := c.List(ctx, &gl); err != nil {
		return nil, err
	}
	ourGW := map[string]bool{}
	for _, g := range gl.Items {
		ours := classes[string(g.Spec.GatewayClassName)]
		key := g.Namespace + "/" + g.Name
		switch {
		case deleting(&g) && controllerutil.ContainsFinalizer(&g, Finalizer):
			o.DelGateways = append(o.DelGateways, g)
			ourGW[key] = true
		case deleting(&g):
		case ours:
			o.Gateways = append(o.Gateways, g)
			ourGW[key] = true
		case controllerutil.ContainsFinalizer(&g, Finalizer):
			o.DelGateways = append(o.DelGateways, g)
		}
	}
	var rl gatewayv1.HTTPRouteList
	if err := c.List(ctx, &rl); err != nil {
		return nil, err
	}
	for _, rt := range rl.Items {
		attached := false
		for _, pr := range rt.Spec.ParentRefs {
			ns := rt.Namespace
			if pr.Namespace != nil {
				ns = string(*pr.Namespace)
			}
			if ourGW[ns+"/"+string(pr.Name)] {
				attached = true
			}
		}
		switch {
		case deleting(&rt) && controllerutil.ContainsFinalizer(&rt, Finalizer):
			o.DelHTTPRoutes = append(o.DelHTTPRoutes, rt)
		case deleting(&rt):
		case attached:
			o.HTTPRoutes = append(o.HTTPRoutes, rt)
		case controllerutil.ContainsFinalizer(&rt, Finalizer):
			o.DelHTTPRoutes = append(o.DelHTTPRoutes, rt)
		}
	}
	return o, nil
}

// BuildTable makes the routing table from what Gather found.
func BuildTable(o *Objects, cfg *Config) *routes.Table {
	return routes.Build(routes.Input{Ingresses: o.Ingresses, HTTPRoutes: o.HTTPRoutes, Gateways: o.Gateways,
		ControllerName: gatewayv1.GatewayController(cfg.ControllerName)})
}

// Reconcile is one full sync.
func (r *Reconciler) Reconcile(ctx context.Context, _ any) (time.Duration, error) {
	lg := log.FromContext(ctx)
	start := time.Now()
	defer func() { r.Metrics.SyncSeconds.Observe(time.Since(start).Seconds()) }()

	objs, err := Gather(ctx, r.Client, &r.Cfg)
	if err != nil {
		return 0, err
	}
	// Finalizers first, on everything we will publish: an object deleted a
	// moment from now must still get its hostnames removed.
	if err := r.addFinalizers(ctx, objs); err != nil {
		return 0, err
	}
	table := BuildTable(objs, &r.Cfg)

	tunnel, err := r.ensureTunnel(ctx)
	if err != nil {
		r.Metrics.SyncErrors.Inc()
		r.reportAll(ctx, objs, table, nil, "TunnelUnavailable", err.Error())
		return 0, err
	}
	if err := r.ensureConnector(ctx, tunnel); err != nil {
		r.Metrics.SyncErrors.Inc()
		return 0, fmt.Errorf("connector: %w", err)
	}
	res, err := r.Orbit.SyncHostnames(ctx, tunnel.ID, r.Cfg.ManagedBy(), table.Hostnames(), r.Cfg.Adopt)
	if err != nil {
		r.Metrics.SyncErrors.Inc()
		r.reportAll(ctx, objs, table, nil, "SyncFailed", err.Error())
		return 0, err
	}
	states := map[string]orbit.HostState{}
	counts := map[string]float64{}
	for _, h := range res.Hostnames {
		states[h.Hostname] = h
		counts[h.State]++
	}
	r.Metrics.SetHostnames(counts)
	lg.V(1).Info("synced", "hostnames", len(res.Hostnames), "removed", res.Removed)

	r.reportAll(ctx, objs, table, states, "", "")
	if err := r.releaseDeleted(ctx, objs); err != nil {
		return 0, err
	}
	return Resync, nil
}

func (r *Reconciler) addFinalizers(ctx context.Context, o *Objects) error {
	add := func(obj client.Object) error {
		if controllerutil.ContainsFinalizer(obj, Finalizer) {
			return nil
		}
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		controllerutil.AddFinalizer(obj, Finalizer)
		return client.IgnoreNotFound(r.Client.Patch(ctx, obj, patch))
	}
	for i := range o.Ingresses {
		if err := add(&o.Ingresses[i]); err != nil {
			return err
		}
	}
	for i := range o.Gateways {
		if err := add(&o.Gateways[i]); err != nil {
			return err
		}
	}
	for i := range o.HTTPRoutes {
		if err := add(&o.HTTPRoutes[i]); err != nil {
			return err
		}
	}
	return nil
}

// releaseDeleted drops our finalizer from objects whose hostnames are gone
// from Orbit (the sync that just succeeded did not list them).
func (r *Reconciler) releaseDeleted(ctx context.Context, o *Objects) error {
	rel := func(obj client.Object) error {
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		controllerutil.RemoveFinalizer(obj, Finalizer)
		if err := r.Client.Patch(ctx, obj, patch); client.IgnoreNotFound(err) != nil {
			return err
		}
		r.Recorder.Event(obj, corev1.EventTypeNormal, "Removed", "its hostnames were removed from Orbit")
		return nil
	}
	var errs []error
	for i := range o.DelIngresses {
		errs = append(errs, rel(&o.DelIngresses[i]))
	}
	for i := range o.DelHTTPRoutes {
		errs = append(errs, rel(&o.DelHTTPRoutes[i]))
	}
	for i := range o.DelGateways {
		errs = append(errs, rel(&o.DelGateways[i]))
	}
	return errors.Join(errs...)
}

// ---- tunnel and connector ----

func (r *Reconciler) secretKey() types.NamespacedName {
	return types.NamespacedName{Namespace: r.Cfg.Namespace, Name: r.Cfg.ConnectorName()}
}

func (r *Reconciler) pickLanding(ctx context.Context) (int64, int64, error) {
	pops, ok, err := r.Orbit.Pops(ctx)
	if err != nil {
		return 0, 0, err
	}
	if !ok || len(pops) == 0 {
		return 0, 0, errors.New("Orbit Link is not available on this account or platform")
	}
	find := func(want string) (int64, error) {
		if want == "" {
			return 0, nil
		}
		for _, p := range pops {
			if strings.EqualFold(p.City, want) || strconv.FormatInt(p.ID, 10) == want {
				return p.ID, nil
			}
		}
		var cities []string
		for _, p := range pops {
			cities = append(cities, p.City)
		}
		return 0, fmt.Errorf("landing point %q is not offered (choose one of: %s)", want, strings.Join(cities, ", "))
	}
	a, err := find(r.Cfg.Landing)
	if err != nil {
		return 0, 0, err
	}
	if a == 0 {
		a = pops[0].ID
	}
	b, err := find(r.Cfg.Landing2)
	return a, b, err
}

// ensureTunnel finds this installation's tunnel (by the id in the connector
// Secret, then by name) or creates it, and makes sure the Secret holds a
// working credential and the current endpoints.
func (r *Reconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *Reconciler) ensureTunnel(ctx context.Context) (*orbit.Tunnel, error) {
	var sec corev1.Secret
	err := r.reader().Get(ctx, r.secretKey(), &sec)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	have := err == nil
	var t *orbit.Tunnel
	if have {
		if id, _ := strconv.ParseInt(string(sec.Data["tunnel-id"]), 10, 64); id > 0 {
			if got, err := r.Orbit.Tunnel(ctx, id); err == nil {
				t = got
			} else if !orbit.IsNotFound(err) {
				return nil, err
			}
		}
	}
	if t == nil {
		list, err := r.Orbit.Tunnels(ctx)
		if err != nil {
			return nil, err
		}
		for i := range list {
			if list[i].Name == r.Cfg.TunnelName {
				if t, err = r.Orbit.Tunnel(ctx, list[i].ID); err != nil {
					return nil, err
				}
				break
			}
		}
	}
	token := ""
	if t == nil {
		a, b, err := r.pickLanding(ctx)
		if err != nil {
			return nil, err
		}
		if t, err = r.Orbit.CreateTunnel(ctx, r.Cfg.TunnelName, a, b, r.Cfg.RouterURL); err != nil {
			return nil, err
		}
		token = t.Token
		if tg := r.eventTarget(ctx); tg != nil {
			r.Recorder.Eventf(tg, corev1.EventTypeNormal, "TunnelCreated", "Orbit Link tunnel %q created", r.Cfg.TunnelName)
		}
	} else if sameURL(t.LocalURL, r.Cfg.RouterURL) == false {
		if err := r.Orbit.PatchTunnel(ctx, t.ID, map[string]any{"local_url": r.Cfg.RouterURL}); err != nil {
			return nil, err
		}
	}
	if token == "" && (!have || len(sec.Data["token"]) == 0 || string(sec.Data["tunnel-id"]) != strconv.FormatInt(t.ID, 10)) {
		// The credential is shown once. Without it on file, a new one is the
		// only way; nothing is connected with the old one from here.
		rt, err := r.Orbit.RotateToken(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		token = rt.Token
		if rt.Connect != nil {
			t.Connect = rt.Connect
		}
	}
	if t.Connect == nil {
		if full, err := r.Orbit.Tunnel(ctx, t.ID); err == nil {
			t.Connect = full.Connect
		} else {
			return nil, err
		}
	}
	if t.Connect == nil || len(t.Connect.Endpoints) == 0 {
		return nil, errors.New("the tunnel has no landing point endpoints yet")
	}
	want := map[string][]byte{
		"tunnel-id":   []byte(strconv.FormatInt(t.ID, 10)),
		"endpoint":    []byte(strings.Join(t.Connect.Endpoints, ",")),
		"fingerprint": []byte(strings.Join(t.Connect.Fingerprints, ",")),
	}
	if token != "" {
		want["token"] = []byte(token)
	} else {
		want["token"] = sec.Data["token"]
	}
	if !have {
		sec = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.Cfg.ConnectorName(), Namespace: r.Cfg.Namespace,
			Labels: r.labels()}, Type: corev1.SecretTypeOpaque, Data: want}
		if err := r.Client.Create(ctx, &sec); apierrors.IsAlreadyExists(err) {
			// Someone (a previous leader) made it meanwhile: this sync's
			// credential is the newest one Orbit knows, so it wins.
			var cur corev1.Secret
			if err := r.reader().Get(ctx, r.secretKey(), &cur); err != nil {
				return nil, err
			}
			cur.Data = want
			if err := r.Client.Update(ctx, &cur); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	} else if !sameData(sec.Data, want) {
		sec.Data = want
		if err := r.Client.Update(ctx, &sec); err != nil {
			return nil, err
		}
	}
	r.mu.Lock()
	r.tunnel = t.ID
	r.mu.Unlock()
	r.Metrics.Connected.Set(b2f(t.Connected))
	return t, nil
}

// sameURL compares the way the API stores local_url: an origin without the
// scheme's default port and without a trailing slash.
func sameURL(a, b string) bool {
	norm := func(s string) string {
		u, err := url.Parse(strings.TrimSpace(s))
		if err != nil {
			return s
		}
		h := u.Host
		if (u.Scheme == "http" && strings.HasSuffix(h, ":80")) || (u.Scheme == "https" && strings.HasSuffix(h, ":443")) {
			h = h[:strings.LastIndexByte(h, ':')]
		}
		return u.Scheme + "://" + h + strings.TrimRight(u.Path, "/")
	}
	return norm(a) == norm(b)
}

func sameData(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if string(a[k]) != string(v) {
			return false
		}
	}
	return true
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (r *Reconciler) labels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": "orbit-link", "app.kubernetes.io/instance": r.Cfg.Release,
		"app.kubernetes.io/component": "connector", "app.kubernetes.io/managed-by": "orbit-link-controller"}
}

// ensureConnector keeps the connector Deployment as configured.
func (r *Reconciler) ensureConnector(ctx context.Context, t *orbit.Tunnel) error {
	var sec corev1.Secret
	if err := r.reader().Get(ctx, r.secretKey(), &sec); err != nil {
		return err
	}
	sum := sha256.Sum256(append(append([]byte{}, sec.Data["token"]...), sec.Data["endpoint"]...))
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: r.Cfg.ConnectorName(), Namespace: r.Cfg.Namespace}}
	spec := ConnectorSpec(&r.Cfg, r.labels(), hex.EncodeToString(sum[:8]))
	js, _ := json.Marshal(spec)
	hs := sha256.Sum256(js)
	want := hex.EncodeToString(hs[:8])
	// Only a changed desired spec is written: the API server fills defaults
	// in, so comparing the objects themselves would rewrite it every sync.
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, d, func() error {
		d.Labels = r.labels()
		if d.Annotations == nil {
			d.Annotations = map[string]string{}
		}
		if d.Annotations[specAnnotation] != want || d.Spec.Replicas == nil || *d.Spec.Replicas != r.Cfg.Replicas {
			d.Spec = spec
			d.Annotations[specAnnotation] = want
		}
		return nil
	})
	return err
}

// eventTarget: events that belong to no single object go on the connector
// Deployment when it exists.
func (r *Reconciler) eventTarget(ctx context.Context) client.Object {
	d := &appsv1.Deployment{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: r.Cfg.Namespace, Name: r.Cfg.ConnectorName()}, d); err == nil {
		return d
	}
	return nil
}

// ---- status and events ----

// reportAll writes status to every object and says what changed. With
// states == nil the sync failed: objects get the failure as a condition.
func (r *Reconciler) reportAll(ctx context.Context, o *Objects, t *routes.Table, states map[string]orbit.HostState, failReason, failMsg string) {
	problems := map[routes.Source][]routes.Problem{}
	for _, p := range t.Problems {
		problems[p.Source] = append(problems[p.Source], p)
	}
	lg := log.FromContext(ctx)
	for i := range o.Ingresses {
		ing := &o.Ingresses[i]
		src := routes.Source{Kind: "Ingress", Namespace: ing.Namespace, Name: ing.Name}
		hosts := t.BySource[src]
		if err := r.ingressStatus(ctx, ing, hosts, states); err != nil {
			lg.Error(err, "ingress status", "ingress", src.String())
		}
		r.say(ing, src, hosts, states, problems[src], failReason, failMsg, len(ing.Spec.TLS) > 0)
	}
	for i := range o.HTTPRoutes {
		rt := &o.HTTPRoutes[i]
		src := routes.Source{Kind: "HTTPRoute", Namespace: rt.Namespace, Name: rt.Name}
		if err := r.routeStatus(ctx, rt, t.Parents[src], t.BySource[src], states, failReason, failMsg); err != nil {
			lg.Error(err, "httproute status", "route", src.String())
		}
		r.say(rt, src, t.BySource[src], states, problems[src], failReason, failMsg, false)
	}
	for i := range o.Gateways {
		g := &o.Gateways[i]
		if err := r.gatewayStatus(ctx, g, t, states, failReason, failMsg, o.HTTPRoutes); err != nil {
			lg.Error(err, "gateway status", "gateway", g.Namespace+"/"+g.Name)
		}
	}
	for i := range o.GatewayClasses {
		gc := &o.GatewayClasses[i]
		patch := client.MergeFrom(gc.DeepCopy())
		if meta.SetStatusCondition(&gc.Status.Conditions, metav1.Condition{Type: string(gatewayv1.GatewayClassConditionStatusAccepted),
			Status: metav1.ConditionTrue, Reason: string(gatewayv1.GatewayClassReasonAccepted), Message: "handled by the Orbit Link controller",
			ObservedGeneration: gc.Generation}) {
			if err := r.Client.Status().Patch(ctx, gc, patch); err != nil {
				lg.Error(err, "gatewayclass status", "class", gc.Name)
			}
		}
	}
}

func targetsOf(hosts []string, states map[string]orbit.HostState) []string {
	var out []string
	seen := map[string]bool{}
	for _, h := range hosts {
		s, ok := states[h]
		if !ok || !s.OK() || s.Target == "" || seen[s.Target] {
			continue
		}
		seen[s.Target] = true
		out = append(out, s.Target)
	}
	sort.Strings(out)
	return out
}

func (r *Reconciler) ingressStatus(ctx context.Context, ing *networkingv1.Ingress, hosts []string, states map[string]orbit.HostState) error {
	if states == nil {
		return nil // keep the last good status while Orbit is unreachable
	}
	var lb []networkingv1.IngressLoadBalancerIngress
	for _, tg := range targetsOf(hosts, states) {
		lb = append(lb, networkingv1.IngressLoadBalancerIngress{Hostname: tg})
	}
	if equalLB(ing.Status.LoadBalancer.Ingress, lb) {
		return nil
	}
	patch := client.MergeFrom(ing.DeepCopy())
	ing.Status.LoadBalancer.Ingress = lb
	return r.Client.Status().Patch(ctx, ing, patch)
}

func equalLB(a, b []networkingv1.IngressLoadBalancerIngress) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Hostname != b[i].Hostname || a[i].IP != b[i].IP {
			return false
		}
	}
	return true
}

func (r *Reconciler) routeStatus(ctx context.Context, rt *gatewayv1.HTTPRoute, parents []routes.ParentStatus, hosts []string,
	states map[string]orbit.HostState, failReason, failMsg string) error {
	orig := rt.DeepCopy()
	var keep []gatewayv1.RouteParentStatus
	for _, p := range rt.Status.Parents {
		if string(p.ControllerName) != r.Cfg.ControllerName {
			keep = append(keep, p)
		}
	}
	for _, ps := range parents {
		var prev []metav1.Condition
		for _, old := range orig.Status.Parents {
			if string(old.ControllerName) == r.Cfg.ControllerName && sameParent(old.ParentRef, ps.Ref) {
				prev = old.Conditions
			}
		}
		conds := append([]metav1.Condition(nil), prev...)
		acc := metav1.Condition{Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionTrue,
			Reason: ps.AcceptedReason, Message: ps.AcceptedMsg, ObservedGeneration: rt.Generation}
		if !ps.Accepted {
			acc.Status = metav1.ConditionFalse
		} else if failReason != "" {
			acc.Status, acc.Reason, acc.Message = metav1.ConditionFalse, "Pending", failMsg
		} else if bad := badHosts(hosts, states); bad != "" {
			acc.Message = strings.TrimPrefix(acc.Message+"; "+bad, "; ")
		} else if acc.Message == "" {
			acc.Message = "published through Orbit Link"
		}
		meta.SetStatusCondition(&conds, acc)
		res := metav1.Condition{Type: string(gatewayv1.RouteConditionResolvedRefs), Status: metav1.ConditionTrue,
			Reason: ps.ResolvedReason, Message: ps.ResolvedMsg, ObservedGeneration: rt.Generation}
		if !ps.Resolved {
			res.Status = metav1.ConditionFalse
		}
		meta.SetStatusCondition(&conds, res)
		keep = append(keep, gatewayv1.RouteParentStatus{ParentRef: ps.Ref, ControllerName: gatewayv1.GatewayController(r.Cfg.ControllerName), Conditions: conds})
	}
	rt.Status.Parents = keep
	if equalParents(orig.Status.Parents, rt.Status.Parents) {
		return nil
	}
	return r.Client.Status().Patch(ctx, rt, client.MergeFrom(orig))
}

func sameParent(a, b gatewayv1.ParentReference) bool {
	s := func(p gatewayv1.ParentReference) string {
		ns, sec := "", ""
		if p.Namespace != nil {
			ns = string(*p.Namespace)
		}
		if p.SectionName != nil {
			sec = string(*p.SectionName)
		}
		return ns + "/" + string(p.Name) + "#" + sec
	}
	return s(a) == s(b)
}

func equalParents(a, b []gatewayv1.RouteParentStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameParent(a[i].ParentRef, b[i].ParentRef) || a[i].ControllerName != b[i].ControllerName || len(a[i].Conditions) != len(b[i].Conditions) {
			return false
		}
		for j := range a[i].Conditions {
			x, y := a[i].Conditions[j], b[i].Conditions[j]
			if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message || x.ObservedGeneration != y.ObservedGeneration {
				return false
			}
		}
	}
	return true
}

func badHosts(hosts []string, states map[string]orbit.HostState) string {
	var bad []string
	for _, h := range hosts {
		if s, ok := states[h]; ok && !s.OK() {
			bad = append(bad, h+": "+s.Message)
		}
	}
	return strings.Join(bad, "; ")
}

func (r *Reconciler) gatewayStatus(ctx context.Context, g *gatewayv1.Gateway, t *routes.Table, states map[string]orbit.HostState,
	failReason, failMsg string, rts []gatewayv1.HTTPRoute) error {
	orig := g.DeepCopy()
	key := g.Namespace + "/" + g.Name
	if states != nil {
		var addrs []gatewayv1.GatewayStatusAddress
		ht := gatewayv1.HostnameAddressType
		for _, tg := range targetsOf(t.GatewayHosts[key], states) {
			addrs = append(addrs, gatewayv1.GatewayStatusAddress{Type: &ht, Value: tg})
		}
		g.Status.Addresses = addrs
	}
	acc := metav1.Condition{Type: string(gatewayv1.GatewayConditionAccepted), Status: metav1.ConditionTrue,
		Reason: string(gatewayv1.GatewayReasonAccepted), ObservedGeneration: g.Generation, Message: "handled by the Orbit Link controller"}
	prog := metav1.Condition{Type: string(gatewayv1.GatewayConditionProgrammed), Status: metav1.ConditionTrue,
		Reason: string(gatewayv1.GatewayReasonProgrammed), ObservedGeneration: g.Generation, Message: "hostnames are published through Orbit Link"}
	if failReason != "" {
		prog.Status, prog.Reason, prog.Message = metav1.ConditionFalse, string(gatewayv1.GatewayReasonPending), failMsg
	}
	meta.SetStatusCondition(&g.Status.Conditions, acc)
	meta.SetStatusCondition(&g.Status.Conditions, prog)
	// Listeners: how many routes attach to each.
	var ls []gatewayv1.ListenerStatus
	for _, l := range g.Spec.Listeners {
		n := int32(0)
		for _, rt := range rts {
			for _, pr := range rt.Spec.ParentRefs {
				ns := rt.Namespace
				if pr.Namespace != nil {
					ns = string(*pr.Namespace)
				}
				if ns == g.Namespace && string(pr.Name) == g.Name && (pr.SectionName == nil || *pr.SectionName == l.Name) {
					n++
					break
				}
			}
		}
		var prev []metav1.Condition
		for _, o := range orig.Status.Listeners {
			if o.Name == l.Name {
				prev = o.Conditions
			}
		}
		ok := l.Protocol == gatewayv1.HTTPProtocolType || l.Protocol == gatewayv1.HTTPSProtocolType
		st, reason, msg := metav1.ConditionTrue, string(gatewayv1.ListenerReasonAccepted), ""
		if !ok {
			st, reason, msg = metav1.ConditionFalse, string(gatewayv1.ListenerReasonUnsupportedProtocol), "only HTTP and HTTPS listeners are supported"
		}
		if l.Protocol == gatewayv1.HTTPSProtocolType {
			msg = "TLS is terminated at the Orbit edge; the certificate is issued there for each verified hostname"
		}
		conds := append([]metav1.Condition(nil), prev...)
		meta.SetStatusCondition(&conds, metav1.Condition{Type: string(gatewayv1.ListenerConditionAccepted), Status: st, Reason: reason, Message: msg, ObservedGeneration: g.Generation})
		meta.SetStatusCondition(&conds, metav1.Condition{Type: string(gatewayv1.ListenerConditionProgrammed), Status: st, Reason: map[bool]string{true: string(gatewayv1.ListenerReasonProgrammed), false: string(gatewayv1.ListenerReasonInvalid)}[ok], ObservedGeneration: g.Generation})
		meta.SetStatusCondition(&conds, metav1.Condition{Type: string(gatewayv1.ListenerConditionResolvedRefs), Status: metav1.ConditionTrue, Reason: string(gatewayv1.ListenerReasonResolvedRefs), ObservedGeneration: g.Generation})
		ls = append(ls, gatewayv1.ListenerStatus{Name: l.Name, AttachedRoutes: n, Conditions: conds,
			SupportedKinds: []gatewayv1.RouteGroupKind{{Group: ptrGroup(gatewayv1.GroupName), Kind: "HTTPRoute"}}})
	}
	g.Status.Listeners = ls
	return r.Client.Status().Patch(ctx, g, client.MergeFrom(orig))
}

func ptrGroup(s string) *gatewayv1.Group { g := gatewayv1.Group(s); return &g }

// say emits events for an object when what there is to say about it changed.
func (r *Reconciler) say(obj client.Object, src routes.Source, hosts []string, states map[string]orbit.HostState,
	problems []routes.Problem, failReason, failMsg string, hasTLS bool) {
	type ev struct{ typ, reason, msg string }
	var evs []ev
	if failReason != "" {
		evs = append(evs, ev{corev1.EventTypeWarning, failReason, failMsg})
	}
	for _, p := range problems {
		evs = append(evs, ev{corev1.EventTypeWarning, p.Reason, p.Message})
	}
	for _, h := range hosts {
		s, ok := states[h]
		if !ok {
			continue
		}
		switch {
		case !s.OK():
			reason := map[string]string{"conflict": "HostnameConflict", "limit_exceeded": "QuotaExceeded", "forbidden": "NotAllowed"}[s.State]
			if reason == "" {
				reason = "HostnameRejected"
			}
			evs = append(evs, ev{corev1.EventTypeWarning, reason, h + ": " + s.Message})
		case !s.Verified:
			evs = append(evs, ev{corev1.EventTypeNormal, "Published", fmt.Sprintf("%s is published; point it at %s with a CNAME record", h, s.Target)})
		default:
			evs = append(evs, ev{corev1.EventTypeNormal, "Published", fmt.Sprintf("%s is published (%s)", h, s.Target)})
		}
	}
	if hasTLS && len(hosts) > 0 {
		evs = append(evs, ev{corev1.EventTypeNormal, "EdgeTLS", "TLS is terminated at the Orbit edge; spec.tls is not used"})
	}
	h := sha256.New()
	for _, e := range evs {
		fmt.Fprintf(h, "%s|%s|%s\n", e.typ, e.reason, e.msg)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	key := src.String() + "@" + strconv.FormatInt(obj.GetGeneration(), 10)
	r.mu.Lock()
	if r.events == nil {
		r.events = map[string]string{}
	}
	same := r.events[key] == digest
	r.events[key] = digest
	r.mu.Unlock()
	if same {
		return
	}
	for _, e := range evs {
		r.Recorder.Event(obj, e.typ, e.reason, e.msg)
	}
}
