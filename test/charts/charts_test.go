// Package charts tests the two Helm charts (charts/orbit-link and
// charts/orbit-link-controller) with `helm lint` and `helm template`: what
// they render, and what they refuse to render. Skipped without helm.
package charts

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// root finds the charts directory by walking up from this file: it is
// <repo>/charts both in the standalone repository and in the monorepo.
func root() string {
	_, f, _, _ := runtime.Caller(0)
	for d := filepath.Dir(f); ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "charts", "orbit-link-controller", "Chart.yaml")); err == nil {
			return filepath.Join(d, "charts")
		}
		if filepath.Dir(d) == d {
			panic("charts directory not found")
		}
	}
}

func helm(t *testing.T, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	return string(out), err
}

type obj = map[string]any

func render(t *testing.T, chart string, set ...string) []obj {
	t.Helper()
	args := []string{"template", "rel", filepath.Join(root(), chart), "--namespace", "ns"}
	for _, s := range set {
		if strings.HasPrefix(s, "--") {
			args = append(args, s)
		} else {
			args = append(args, "--set", s)
		}
	}
	out, err := helm(t, args...)
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", set, err, out)
	}
	var docs []obj
	for _, d := range strings.Split(out, "\n---") {
		var m obj
		if err := yaml.Unmarshal([]byte(d), &m); err != nil {
			t.Fatalf("bad yaml: %v\n%s", err, d)
		}
		if m != nil {
			docs = append(docs, m)
		}
	}
	return docs
}

func refuse(t *testing.T, chart, want string, set ...string) {
	t.Helper()
	args := []string{"template", "rel", filepath.Join(root(), chart)}
	for _, s := range set {
		args = append(args, "--set", s)
	}
	out, err := helm(t, args...)
	if err == nil || !strings.Contains(out, want) {
		t.Fatalf("%v rendered (want refusal containing %q): %v\n%s", set, want, err, out)
	}
}

func find(docs []obj, kind string) []obj {
	var out []obj
	for _, d := range docs {
		if d["kind"] == kind {
			out = append(out, d)
		}
	}
	return out
}

// get walks a path like "spec.template.spec.containers.0.image".
func get(o any, path string) any {
	for _, p := range strings.Split(path, ".") {
		switch v := o.(type) {
		case map[string]any:
			o = v[p]
		case []any:
			var i int
			if _, err := fmt.Sscan(p, &i); err != nil || i >= len(v) {
				return nil
			}
			o = v[i]
		default:
			return nil
		}
	}
	return o
}

// The endpoint list holds commas and brackets, which --set parses; the
// documented way is a values file, and that is what these tests use.
const linkValues = `tunnel:
  token: lnk_abc
  endpoint: "203.0.113.8:7844,[2001:db8::8]:7844"
  fingerprint: "fpA,fpA"
  local: http://web.default.svc.cluster.local:8080
`

var linkBase []string

func TestMain(m *testing.M) {
	dir, _ := os.MkdirTemp("", "charts")
	f := filepath.Join(dir, "values.yaml")
	_ = os.WriteFile(f, []byte(linkValues), 0o600)
	linkBase = []string{"--values=" + f}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func TestLinkChartLints(t *testing.T) {
	args := []string{"lint", filepath.Join(root(), "orbit-link"), "--strict"}
	args = append(args, linkBase...)
	if out, err := helm(t, args...); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestLinkChartDefaults(t *testing.T) {
	docs := render(t, "orbit-link", linkBase...)
	d := find(docs, "Deployment")
	if len(d) != 1 {
		t.Fatalf("deployments %d", len(d))
	}
	dep := d[0]
	if get(dep, "spec.replicas") != float64(2) {
		t.Fatalf("replicas %v", get(dep, "spec.replicas"))
	}
	c := get(dep, "spec.template.spec.containers.0").(obj)
	if c["image"] != "cr.yunzheng.space/orbit-link:1.3.0" {
		t.Fatalf("image %v", c["image"])
	}
	for path, want := range map[string]any{
		"securityContext.readOnlyRootFilesystem":   true,
		"securityContext.allowPrivilegeEscalation": false,
		"securityContext.capabilities.drop.0":      "ALL",
		"readinessProbe.exec.command.1":            "-healthcheck",
		"startupProbe.exec.command.1":              "-healthcheck",
	} {
		if got := get(c, path); got != want {
			t.Errorf("%s = %v, want %v", path, got, want)
		}
	}
	if get(c, "livenessProbe") != nil {
		t.Error("liveness restarts a connector that is reconnecting by itself; it is off by default")
	}
	if get(dep, "spec.template.spec.securityContext.runAsNonRoot") != true || get(dep, "spec.template.spec.automountServiceAccountToken") != false {
		t.Error("pod security")
	}
	if get(dep, "spec.template.spec.terminationGracePeriodSeconds") != float64(13) {
		t.Errorf("grace %v (drain 8s + 5)", get(dep, "spec.template.spec.terminationGracePeriodSeconds"))
	}
	// The credential is a mounted file, never an environment variable.
	for _, e := range c["env"].([]any) {
		if v, _ := e.(obj)["value"].(string); strings.HasPrefix(v, "lnk_") {
			t.Fatal("the token is in the environment")
		}
	}
	env := map[string]string{}
	for _, e := range c["env"].([]any) {
		m := e.(obj)
		v, _ := m["value"].(string)
		env[m["name"].(string)] = v
	}
	if env["ORBIT_LINK_TOKEN_FILE"] != "/var/run/orbit-link/token" || env["ORBIT_LINK_ENDPOINT"] != "203.0.113.8:7844,[2001:db8::8]:7844" || env["ORBIT_LINK_LOCAL"] != "http://web.default.svc.cluster.local:8080" {
		t.Fatalf("env %v", env)
	}
	if s := find(docs, "Secret"); len(s) != 1 || get(s[0], "stringData.token") != "lnk_abc" {
		t.Fatal("secret")
	}
	if p := find(docs, "PodDisruptionBudget"); len(p) != 1 || get(p[0], "spec.minAvailable") != float64(1) {
		t.Fatal("pdb")
	}
	if get(dep, "spec.template.spec.topologySpreadConstraints.0.topologyKey") != "kubernetes.io/hostname" {
		t.Fatal("spread")
	}
	if len(find(docs, "NetworkPolicy")) != 0 {
		t.Fatal("network policy is opt-in")
	}
}

func TestLinkChartVariants(t *testing.T) {
	one := render(t, "orbit-link", append(linkBase, "replicaCount=1", "drain=20s", "tunnel.tcp=true", "networkPolicy.enabled=true", "networkPolicy.localPort=8080")...)
	if len(find(one, "PodDisruptionBudget")) != 0 {
		t.Error("a PDB with one replica blocks every node drain")
	}
	dep := find(one, "Deployment")[0]
	if get(dep, "spec.template.spec.topologySpreadConstraints") != nil {
		t.Error("spread with one replica")
	}
	if get(dep, "spec.template.spec.terminationGracePeriodSeconds") != float64(25) || get(dep, "spec.template.spec.containers.0.args.0") != "-tcp" {
		t.Errorf("grace %v args %v", get(dep, "spec.template.spec.terminationGracePeriodSeconds"), get(dep, "spec.template.spec.containers.0.args"))
	}
	if np := find(one, "NetworkPolicy"); len(np) != 1 || get(np[0], "spec.egress.2.ports.0.port") != float64(8080) {
		t.Error("network policy")
	}
	ext := render(t, "orbit-link", "tunnel.existingSecret=mine", "tunnel.existingSecretKey=cred", "tunnel.endpoint=a:7844", "tunnel.fingerprint=f", "tunnel.local=http://x:1")
	if len(find(ext, "Secret")) != 0 {
		t.Error("a Secret was created although one exists")
	}
	vol := get(find(ext, "Deployment")[0], "spec.template.spec.volumes.0.secret").(obj)
	if vol["secretName"] != "mine" || get(vol, "items.0.key") != "cred" {
		t.Errorf("volume %v", vol)
	}
}

func TestLinkChartRefuses(t *testing.T) {
	refuse(t, "orbit-link", "tunnel.existingSecret", "tunnel.endpoint=a:7844", "tunnel.fingerprint=f", "tunnel.local=http://x:1")
	refuse(t, "orbit-link", "endpoint", "tunnel.token=lnk_x", "tunnel.fingerprint=f", "tunnel.local=http://x:1")
	refuse(t, "orbit-link", "token", "tunnel.token=not-a-token", "tunnel.endpoint=a:7844", "tunnel.fingerprint=f", "tunnel.local=http://x:1")
	refuse(t, "orbit-link", "local", "tunnel.token=lnk_x", "tunnel.endpoint=a:7844", "tunnel.fingerprint=f", "tunnel.local=x:1")
}

var ctlBase = []string{"clusterName=kind", "orbit.apiToken=orb_abcdefghijklmnopqrstuvwx"}

func TestControllerChartLints(t *testing.T) {
	args := []string{"lint", filepath.Join(root(), "orbit-link-controller"), "--strict"}
	for _, s := range ctlBase {
		args = append(args, "--set", s)
	}
	if out, err := helm(t, args...); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestControllerChart(t *testing.T) {
	docs := render(t, "orbit-link-controller", ctlBase...)
	dep := find(docs, "Deployment")[0]
	c := get(dep, "spec.template.spec.containers.0").(obj)
	if c["image"] != "cr.yunzheng.space/orbit-link-controller:0.1.1" {
		t.Fatalf("image %v", c["image"])
	}
	args := fmt.Sprint(c["args"])
	for _, want := range []string{"--cluster-name=kind", "--release=rel", "--connector-image=cr.yunzheng.space/orbit-link:1.3.0", "--connector-replicas=2", "--ingress-class=orbit-link"} {
		if !strings.Contains(args, want) {
			t.Errorf("args %s lack %s", args, want)
		}
	}
	if get(c, "securityContext.readOnlyRootFilesystem") != true || get(dep, "spec.template.spec.securityContext.runAsNonRoot") != true {
		t.Error("controller security context")
	}
	// A 0400 secret file is root's unless the pod has an fsGroup: without it
	// the non-root controller cannot read its own API token (kind, 09-28).
	if get(dep, "spec.template.spec.securityContext.fsGroup") != float64(65532) {
		t.Error("no fsGroup: the non-root controller cannot read the mounted token")
	}
	if ic := find(docs, "IngressClass"); len(ic) != 1 || get(ic[0], "spec.controller") != "yunzheng.space/orbit-link" {
		t.Error("ingress class")
	}
	if len(find(docs, "GatewayClass")) != 0 {
		t.Error("GatewayClass rendered without the Gateway API installed")
	}
	if svc := find(docs, "Service"); len(svc) != 2 || get(svc[0], "metadata.name") != "rel-router" {
		t.Error("router service")
	}
	job := find(docs, "Job")
	if len(job) != 1 || get(job[0], "metadata.annotations.helm\\.sh/hook") == nil && !strings.Contains(fmt.Sprint(get(job[0], "metadata.annotations")), "pre-delete") {
		t.Fatal("cleanup hook")
	}
	if get(job[0], "spec.template.spec.containers.0.args.0") != "cleanup" {
		t.Error("cleanup job args")
	}
	// Least privilege: Secrets and Deployments only in the release namespace,
	// never cluster-wide; no wildcard anywhere.
	for _, cr := range find(docs, "ClusterRole") {
		for _, r := range cr["rules"].([]any) {
			res := fmt.Sprint(get(r, "resources"))
			if strings.Contains(res, "secrets") || strings.Contains(res, "deployments") || strings.Contains(res, "*") || strings.Contains(fmt.Sprint(get(r, "verbs")), "*") {
				t.Errorf("cluster-wide rule %v", r)
			}
			if strings.Contains(fmt.Sprint(get(r, "verbs")), "delete") {
				t.Errorf("the controller deletes nothing cluster-wide: %v", r)
			}
		}
	}
	role := find(docs, "Role")
	if len(role) != 1 || !strings.Contains(fmt.Sprint(role[0]["rules"]), "secrets") {
		t.Error("namespaced role")
	}
	gw := render(t, "orbit-link-controller", append(ctlBase, "--api-versions=gateway.networking.k8s.io/v1/GatewayClass")...)
	if g := find(gw, "GatewayClass"); len(g) != 1 || get(g[0], "spec.controllerName") != "yunzheng.space/orbit-link" {
		t.Error("GatewayClass with the Gateway API installed")
	}
	one := render(t, "orbit-link-controller", append(ctlBase, "replicaCount=1")...)
	if len(find(one, "PodDisruptionBudget")) != 0 {
		t.Error("pdb with one replica")
	}
}

func TestControllerChartRefuses(t *testing.T) {
	refuse(t, "orbit-link-controller", "clusterName", "orbit.apiToken=orb_abcdefghijklmnopqrstuvwx")
	refuse(t, "orbit-link-controller", "clusterName", "clusterName=Bad_Name", "orbit.apiToken=orb_abcdefghijklmnopqrstuvwx")
	refuse(t, "orbit-link-controller", "orbit.existingSecret", "clusterName=kind")
	refuse(t, "orbit-link-controller", "apiToken", "clusterName=kind", "orbit.apiToken=lnk_wrongkind")
}
