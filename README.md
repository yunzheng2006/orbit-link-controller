# Orbit Link controller

A Kubernetes controller that publishes Ingresses and Gateway API routes through
[Orbit Link](https://orbit.yunzheng.space/docs/link/). Each hostname you declare
in the cluster is served through Orbit Link, with no inbound port and no load
balancer.

This repository holds the controller source and the two Helm charts:

| Chart | What it does |
|---|---|
| `orbit-link-controller` | Publishes Ingresses of class `orbit-link` and HTTPRoutes attached to a Gateway of GatewayClass `orbit-link`. Creates one tunnel for the cluster and runs its connector. |
| `orbit-link` | Runs the connector for one tunnel you created in the Orbit console, forwarding to one in-cluster address. |

Both are published at `oci://cr.yunzheng.space/charts`, with the images at
`cr.yunzheng.space/orbit-link-controller` and `cr.yunzheng.space/orbit-link`.

## Features

- Ingresses of class `orbit-link`, and HTTPRoutes attached to a Gateway of
  GatewayClass `orbit-link`, are published. Host and path rules (Prefix and
  Exact) are honoured.
- The Ingress status, or the Gateway's addresses, show the name to point each
  hostname's CNAME record at. `kubectl describe` shows events for each
  hostname: published, already in use, or over the plan's limit.
- Changing a host or a path takes effect at once; deleting the Ingress or route
  removes its hostnames from Orbit. `helm uninstall` removes everything the
  controller created.
- TLS is terminated at the Orbit edge, which issues the certificate once the
  hostname's CNAME is in place.
- Several controller replicas share the work: one of them makes changes (leader
  election), all of them serve traffic.
- Runs as a non-root user on a read-only file system; the API token is mounted
  as a file.

## Install the controller

1. In the Orbit console, create an API token with the scope **Orbit Link only**
   (Console → API → Create token) and save it in a file.
2. Install the chart:

```sh
kubectl create namespace orbit-link
kubectl -n orbit-link create secret generic orbit-api --from-file=token=./orbit-api-token
helm install orbit-link oci://cr.yunzheng.space/charts/orbit-link-controller -n orbit-link \
  --set clusterName=my-cluster --set orbit.existingSecret=orbit-api
```

`clusterName` names this cluster in Orbit. The controller creates one tunnel
for it (`k8s-<clusterName>`) and tags the hostnames it publishes
`k8s:<clusterName>/<release>`; it only changes hostnames that carry its own tag.

## Publish a service with an Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: my-app
spec:
  ingressClassName: orbit-link
  rules:
    - host: app.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: my-app
                port:
                  number: 80
```

`kubectl get ingress my-app` shows the name to point `app.example.com`'s CNAME
record at.

## Publish a service with the Gateway API

The GatewayClass is created when the Gateway API CRDs are installed in the
cluster.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: web
spec:
  gatewayClassName: orbit-link
  listeners:
    - name: http
      protocol: HTTP
      port: 80
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: my-app
spec:
  parentRefs:
    - name: web
  hostnames: [app.example.com]
  rules:
    - matches:
        - path: { type: PathPrefix, value: / }
      backendRefs:
        - name: my-app
          port: 80
```

`kubectl get gateway web` shows the addresses; `kubectl describe httproute
my-app` shows whether each hostname was accepted.

## Configuration

Values of the `orbit-link-controller` chart (see
[`charts/orbit-link-controller/values.yaml`](charts/orbit-link-controller/values.yaml)
and its `values.schema.json`):

| Value | Default | Meaning |
|---|---|---|
| `clusterName` | (required) | Names this cluster in Orbit; 1-32 lower-case letters, digits and `-`. |
| `orbit.existingSecret` / `orbit.existingSecretKey` | `""` / `token` | A Secret you manage holding the API token. |
| `orbit.apiToken` | `""` | Alternatively, the token itself; the chart creates the Secret. |
| `orbit.landing`, `orbit.landing2` | `""` | Landing point: a city from the console's Link page, or its id. Empty: the first one offered. `landing2` adds a second one. |
| `orbit.adopt` | `false` | Take over existing Shield sites on your account with the same hostnames. Off: such hostnames are reported as conflicts. |
| `orbit.apiQPS`, `orbit.apiBurst` | `2`, `5` | Client-side limit on Orbit API calls. |
| `ingressClass.create`, `.name`, `.default` | `true`, `orbit-link`, `false` | The IngressClass; `default` makes it the cluster default. |
| `gatewayClass.create`, `.name` | `true`, `orbit-link` | The GatewayClass. |
| `replicaCount` | `2` | Controller replicas. |
| `connector.replicas`, `connector.tcp` | `2`, `false` | Connector replicas; `tcp` uses the TCP fallback only, for networks that drop UDP. |
| `cleanupOnUninstall` | `true` | On `helm uninstall`, remove the hostnames, the tunnel and the connector from Orbit first. |
| `metrics.serviceMonitor` | `false` | Create a Prometheus Operator ServiceMonitor. |

The controller binary takes the same settings as flags; `orbit-link-controller
-h` lists them.

## Permissions

The chart creates a ServiceAccount with:

| Scope | Resources | Verbs |
|---|---|---|
| Cluster | `ingresses`, `gatewayclasses`, `gateways`, `httproutes` | get, list, watch, update, patch (finalizer) |
| Cluster | their `/status` subresources | get, update, patch |
| Cluster | `ingressclasses`, `services` | get, list, watch |
| Cluster | `events` | create, patch |
| Own namespace | `secrets`, `deployments` (the connector and its credential), `leases` (leader election) | get, list, watch, create, update, patch, delete |

## Run one connector without the controller

```yaml
# orbit-link-values.yaml  (chmod 600 orbit-link-values.yaml)
tunnel:
  token: lnk_…
  endpoint: "203.0.113.4:7844"
  fingerprint: "…"
  local: http://my-app.default.svc.cluster.local:8080
```

```sh
helm install orbit-link oci://cr.yunzheng.space/charts/orbit-link \
  --namespace orbit-link --create-namespace -f orbit-link-values.yaml
```

The console's install dialog (Link → your tunnel → Kubernetes) fills in these
values for your tunnel.

## Build and test

```sh
go vet ./...
go test ./...                 # the chart tests run when helm is installed
helm lint charts/orbit-link-controller --set clusterName=dev --set orbit.existingSecret=orbit-api
go build ./cmd/orbit-link-controller
```

## Documentation

- [Orbit Link documentation](https://orbit.yunzheng.space/docs/link/)
- [Contributing](CONTRIBUTING.md) and [security reports](SECURITY.md)

## License

[Apache License 2.0](LICENSE).
