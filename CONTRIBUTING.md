# Contributing

Issues and pull requests are welcome.

- Describe the cluster setup (Kubernetes version, Ingress or Gateway API, chart values) when reporting a problem. Leave out API tokens and tunnel credentials.
- Before opening a pull request, run `go vet ./...`, `go test ./...` (with `helm` installed, so the chart tests run) and `helm lint` on the chart you changed.
- Keep changes focused; add a test for behaviour you change.

This repository is published from the maintainers' source tree. An accepted pull request is applied there and appears here with the next sync, so the commit history here does not keep individual commits. Release images and charts are built and published by the maintainers.

Contributions are licensed under the [Apache License 2.0](LICENSE).
