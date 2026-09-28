package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/router"
	"github.com/yunzheng2006/orbit-link-controller/internal/routes"
)

// HasGatewayAPI reports whether the Gateway API (v1 HTTPRoute) is installed.
func HasGatewayAPI(mgr manager.Manager) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"}, "v1")
	return err == nil
}

// specChanged: what a user changes (spec, annotations, deletion, finalizers)
// — never our own status writes, which would otherwise loop.
var specChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		o, n := e.ObjectOld, e.ObjectNew
		return o.GetGeneration() != n.GetGeneration() ||
			!reflect.DeepEqual(o.GetAnnotations(), n.GetAnnotations()) ||
			!reflect.DeepEqual(o.GetFinalizers(), n.GetFinalizers()) ||
			(o.GetDeletionTimestamp() == nil) != (n.GetDeletionTimestamp() == nil) ||
			!reflect.DeepEqual(o.GetLabels(), n.GetLabels())
	},
}

// Setup registers the reconciler (leader only) and the router (every replica).
func Setup(mgr manager.Manager, r *Reconciler, rt *router.Router) error {
	one := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: SyncKey}}
	})
	own := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetNamespace() == r.Cfg.Namespace && o.GetName() == r.Cfg.ConnectorName()
	})
	b := ctrl.NewControllerManagedBy(mgr).Named("orbit-link").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Watches(&networkingv1.Ingress{}, one, builder.WithPredicates(specChanged)).
		Watches(&corev1.Secret{}, one, builder.WithPredicates(own, predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
			return !reflect.DeepEqual(e.ObjectOld.(*corev1.Secret).Data, e.ObjectNew.(*corev1.Secret).Data)
		}}))
	// A first sync without waiting for an event: the tunnel and connector
	// exist as soon as the controller leads, even with nothing to publish.
	kick := make(chan event.GenericEvent, 1)
	kick <- event.GenericEvent{Object: &corev1.Secret{}}
	b = b.WatchesRawSource(source.Channel(kick, one))
	if r.Cfg.GatewayAPI {
		b = b.Watches(&gatewayv1.GatewayClass{}, one, builder.WithPredicates(specChanged)).
			Watches(&gatewayv1.Gateway{}, one, builder.WithPredicates(specChanged)).
			Watches(&gatewayv1.HTTPRoute{}, one, builder.WithPredicates(specChanged))
	}
	err := b.Complete(reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
		after, err := r.Reconcile(ctx, nil)
		if err != nil {
			return reconcile.Result{}, err // controller-runtime backs off
		}
		return reconcile.Result{RequeueAfter: after}, nil
	}))
	if err != nil {
		return err
	}

	// Router: every replica serves, from the same cache.
	rt.Build = func(ctx context.Context) (*routes.Table, error) {
		o, err := Gather(ctx, mgr.GetClient(), &r.Cfg)
		if err != nil {
			return nil, err
		}
		return BuildTable(o, &r.Cfg), nil
	}
	rt.Resolve = func(ctx context.Context, be routes.Backend) (string, error) {
		var svc corev1.Service
		if err := mgr.GetClient().Get(ctx, types.NamespacedName{Namespace: be.Namespace, Name: be.Service}, &svc); err != nil {
			return "", err
		}
		port := be.Port
		if be.PortName != "" {
			for _, p := range svc.Spec.Ports {
				if p.Name == be.PortName {
					port = p.Port
				}
			}
		}
		if port == 0 {
			return "", fmt.Errorf("service %s/%s has no port %q", be.Namespace, be.Service, be.PortName)
		}
		host := svc.Spec.ClusterIP
		if host == "" || host == corev1.ClusterIPNone {
			host = be.Service + "." + be.Namespace + ".svc"
		}
		if svc.Spec.Type == corev1.ServiceTypeExternalName {
			host = svc.Spec.ExternalName
		}
		return router.HostPort(host, port), nil
	}
	rt.Invalidate()
	inv := toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { rt.Invalidate() },
		UpdateFunc: func(any, any) { rt.Invalidate() },
		DeleteFunc: func(any) { rt.Invalidate() },
	}
	kinds := []client.Object{&networkingv1.Ingress{}}
	if r.Cfg.GatewayAPI {
		kinds = append(kinds, &gatewayv1.GatewayClass{}, &gatewayv1.Gateway{}, &gatewayv1.HTTPRoute{})
	}
	for _, k := range kinds {
		inf, err := mgr.GetCache().GetInformer(context.Background(), k)
		if err != nil {
			return err
		}
		if _, err := inf.AddEventHandler(inv); err != nil {
			return err
		}
	}
	// Services are read through the cache on demand; make sure it is started.
	if _, err := mgr.GetCache().GetInformer(context.Background(), &corev1.Service{}); err != nil {
		return err
	}
	return nil
}

// Serve runs an HTTP server as a manager runnable on every replica.
type Serve struct {
	Addr    string
	Handler http.Handler
}

func (s Serve) NeedLeaderElection() bool { return false }

func (s Serve) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
