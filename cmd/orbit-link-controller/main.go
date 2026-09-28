// orbit-link-controller publishes Kubernetes Ingresses (class orbit-link) and
// Gateway API HTTPRoutes (Gateways of a GatewayClass whose controllerName is
// yunzheng.space/orbit-link) through Orbit Link.
//
//	orbit-link-controller [flags]            run (leader-elected reconciler + router on every replica)
//	orbit-link-controller cleanup [flags]    remove everything this installation created (Helm pre-delete hook)
//	orbit-link-controller version
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/controller"
	"github.com/yunzheng2006/orbit-link-controller/internal/orbit"
	"github.com/yunzheng2006/orbit-link-controller/internal/router"
)

var version = "0.1.1"

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	orbit.Version = version
	sub := ""
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		// Two statements: in one, append would overwrite os.Args[1] before
		// (or after — the order is unspecified) it is read.
		sub = os.Args[1]
		os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
	}
	if sub == "version" {
		fmt.Println("orbit-link-controller", version)
		return
	}
	var cfg controller.Config
	var (
		api        = flag.String("orbit-api", env("ORBIT_API", "https://dash.yunzheng.space/api/v1"), "Orbit API base URL")
		tokenFile  = flag.String("orbit-token-file", env("ORBIT_TOKEN_FILE", "/var/run/orbit/api-token"), "file holding a Link-scoped Orbit API token")
		qps        = flag.Float64("api-qps", 2, "Orbit API requests per second (client-side limit)")
		burst      = flag.Int("api-burst", 5, "Orbit API burst")
		leader     = flag.Bool("leader-elect", true, "elect one active reconciler among replicas")
		routerAddr = flag.String("router-addr", ":8080", "where the in-cluster router listens (the connector forwards here)")
		metrics    = flag.String("metrics-addr", ":8081", "Prometheus metrics")
		probes     = flag.String("health-addr", ":8082", "/healthz and /readyz")
		ctrlDeploy = flag.String("controller-deployment", "", "cleanup: the controller Deployment to stop first")
	)
	flag.StringVar(&cfg.Cluster, "cluster-name", os.Getenv("ORBIT_CLUSTER"), "names this cluster in Orbit (tunnel k8s-<name>, sites tagged k8s:<name>/<release>)")
	flag.StringVar(&cfg.Release, "release", env("ORBIT_RELEASE", "orbit-link"), "Helm release name (prefix of created objects)")
	flag.StringVar(&cfg.Namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace of the controller and connector")
	flag.StringVar(&cfg.IngressClass, "ingress-class", "orbit-link", "IngressClass name to handle")
	flag.StringVar(&cfg.ControllerName, "controller-name", "yunzheng.space/orbit-link", "IngressClass controller / GatewayClass controllerName")
	flag.StringVar(&cfg.TunnelName, "tunnel-name", "", "tunnel name (default k8s-<cluster-name>)")
	flag.StringVar(&cfg.Landing, "landing", "", "landing point: city or id (default: the first offered)")
	flag.StringVar(&cfg.Landing2, "landing2", "", "optional second landing point")
	flag.StringVar(&cfg.RouterURL, "router-url", "", "where the connector forwards (default http://<release>-router.<namespace>.svc:80)")
	flag.StringVar(&cfg.ConnectorImage, "connector-image", "cr.yunzheng.space/orbit-link:1.3.0", "connector image")
	var replicas int
	flag.IntVar(&replicas, "connector-replicas", 2, "connector replicas")
	flag.BoolVar(&cfg.ConnectorTCP, "connector-tcp", false, "connector uses the TCP fallback only")
	flag.BoolVar(&cfg.Adopt, "adopt", false, "take over existing hand-made Shield sites with the same hostnames")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	cfg.Replicas = int32(replicas)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	lg := ctrl.Log.WithName("orbit-link")
	if err := cfg.Defaults(); err != nil {
		lg.Error(err, "configuration")
		os.Exit(2)
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.Install(scheme)
	rc := ctrl.GetConfigOrDie()
	rc.UserAgent = "orbit-link-controller/" + version

	readToken := func() (string, error) {
		b, err := os.ReadFile(*tokenFile)
		return strings.TrimSpace(string(b)), err
	}
	oc := orbit.New(*api, readToken, *qps, *burst)

	if sub == "cleanup" {
		c, err := client.New(rc, client.Options{Scheme: scheme})
		if err != nil {
			lg.Error(err, "client")
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		mapper := c.RESTMapper()
		_, gerr := mapper.RESTMapping(gatewayv1.SchemeGroupVersion.WithKind("HTTPRoute").GroupKind(), "v1")
		cfg.GatewayAPI = gerr == nil
		if err := controller.Cleanup(ctx, c, oc, &cfg, *ctrlDeploy, func(f string, a ...any) { lg.Info(fmt.Sprintf(f, a...)) }); err != nil {
			lg.Error(err, "cleanup")
			os.Exit(1)
		}
		lg.Info("cleanup done")
		return
	}
	if sub != "" {
		fmt.Fprintf(os.Stderr, "unknown command %q\n", sub)
		os.Exit(2)
	}

	own := map[string]cache.Config{cfg.Namespace: {}}
	mgr, err := ctrl.NewManager(rc, ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: *metrics},
		HealthProbeBindAddress:        *probes,
		LeaderElection:                *leader,
		LeaderElectionID:              "orbit-link-controller." + cfg.Release,
		LeaderElectionNamespace:       cfg.Namespace,
		LeaderElectionReleaseOnCancel: true,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			// Secrets and Deployments only in our own namespace: the RBAC
			// grants nothing more, and nothing more is needed.
			&corev1.Secret{}:     {Namespaces: own},
			&appsv1.Deployment{}: {Namespaces: own},
		}},
	})
	if err != nil {
		lg.Error(err, "manager")
		os.Exit(1)
	}
	cfg.GatewayAPI = controller.HasGatewayAPI(mgr)
	lg.Info("starting", "version", version, "cluster", cfg.Cluster, "gatewayAPI", cfg.GatewayAPI, "ingressClass", cfg.IngressClass)
	m := controller.NewMetrics(nil)
	oc.Observe = m.ObserveAPI
	rec := &controller.Reconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Orbit: oc, Recorder: mgr.GetEventRecorderFor("orbit-link-controller"), Cfg: cfg, Metrics: m}
	rt := router.New(nil)
	rt.Observe = m.ObserveRouter
	if err := controller.Setup(mgr, rec, rt); err != nil {
		lg.Error(err, "setup")
		os.Exit(1)
	}
	if err := mgr.Add(controller.Serve{Addr: *routerAddr, Handler: rt}); err != nil {
		lg.Error(err, "router")
		os.Exit(1)
	}
	_ = mgr.AddHealthzCheck("ping", healthz.Ping)
	_ = mgr.AddReadyzCheck("cache", func(r *http.Request) error {
		if !mgr.GetCache().WaitForCacheSync(r.Context()) {
			return fmt.Errorf("cache not synced")
		}
		return nil
	})
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		lg.Error(err, "manager stopped")
		os.Exit(1)
	}
}
