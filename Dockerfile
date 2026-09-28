# Orbit Link controller. The binaries are cross-compiled by container.sh
# (Go, CGO off, -trimpath); this only COPYs the one for the target platform.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION
ARG CREATED
COPY --chown=0:0 bin/orbit-link-controller-${TARGETOS}-${TARGETARCH}${TARGETVARIANT} /orbit-link-controller
LABEL org.opencontainers.image.title="Orbit Link controller" \
      org.opencontainers.image.description="Publishes Kubernetes Ingress and Gateway API routes through Orbit Link." \
      org.opencontainers.image.vendor="YunZheng" \
      org.opencontainers.image.url="https://orbit.yunzheng.space/docs/link/" \
      org.opencontainers.image.documentation="https://orbit.yunzheng.space/docs/link/" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}"
USER 65532:65532
ENTRYPOINT ["/orbit-link-controller"]
