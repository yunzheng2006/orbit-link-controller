package controller

import (
	"fmt"
	"regexp"
	"strings"
)

// Config is how one installation of the controller is set up (flags / Helm).
type Config struct {
	Cluster        string // names this cluster in Orbit: tunnel name and managed_by tag
	Release        string // Helm release (prefix of the objects the controller creates)
	Namespace      string // where the controller, the connector and their Secrets live
	IngressClass   string // IngressClass name handled (spec.ingressClassName)
	ControllerName string // IngressClass .spec.controller and GatewayClass .spec.controllerName
	TunnelName     string // default k8s-<cluster>
	Landing        string // landing point: city name or id; empty = the first offered
	Landing2       string // optional second landing point
	RouterURL      string // where the connector forwards: the router Service
	ConnectorImage string
	Replicas       int32
	ConnectorTCP   bool
	Adopt          bool // take over hand-made CNAME sites with the same hostname
	GatewayAPI     bool // set at start when the Gateway API CRDs are installed
}

var tunnelNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,38}[A-Za-z0-9]$`)
var clusterRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

// Defaults fills derived values and checks the rest.
func (c *Config) Defaults() error {
	if !clusterRE.MatchString(c.Cluster) {
		return fmt.Errorf("cluster name %q: 1-32 lower-case letters, digits and -", c.Cluster)
	}
	if c.Release == "" {
		c.Release = "orbit-link"
	}
	if c.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if c.IngressClass == "" {
		c.IngressClass = "orbit-link"
	}
	if c.ControllerName == "" {
		c.ControllerName = "yunzheng.space/orbit-link"
	}
	if c.TunnelName == "" {
		c.TunnelName = "k8s-" + c.Cluster
	}
	if !tunnelNameRE.MatchString(c.TunnelName) {
		return fmt.Errorf("tunnel name %q: 2-40 letters, digits, spaces and . _ -", c.TunnelName)
	}
	if c.RouterURL == "" {
		c.RouterURL = fmt.Sprintf("http://%s-router.%s.svc:80", c.Release, c.Namespace)
	}
	if !strings.HasPrefix(c.RouterURL, "http://") && !strings.HasPrefix(c.RouterURL, "https://") {
		return fmt.Errorf("router URL %q must be http:// or https://", c.RouterURL)
	}
	if c.ConnectorImage == "" {
		c.ConnectorImage = "cr.yunzheng.space/orbit-link:1.3.0"
	}
	if c.Replicas < 0 {
		c.Replicas = 0
	}
	return nil
}

// ManagedBy tags the Shield sites this installation creates.
func (c *Config) ManagedBy() string { return "k8s:" + c.Cluster + "/" + c.Release }

// ConnectorName is the connector Deployment and its Secret.
func (c *Config) ConnectorName() string { return c.Release + "-connector" }

// Finalizer guards objects whose hostnames must be removed from Orbit first.
const Finalizer = "link.yunzheng.space/cleanup"
