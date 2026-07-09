# Container image

Every tagged pgstream release publishes a container image to the GitHub
Container Registry. Images are multi-architecture, signed and ship with a
Software Bill of Materials (SBOM).

```
ghcr.io/xataio/pgstream
```

## Tags

Each release produces the following tags, all pointing at the same multi-arch
manifest:

| Tag         | Example  | Notes                    |
| ----------- | -------- | ------------------------ |
| `<version>` | `1.1.0`  | Recommended for pinning. |
| `latest`    | `latest` | Moves with every release. |

```sh
docker pull ghcr.io/xataio/pgstream:1.1.0
```

## Architectures

Images are built for both `linux/amd64` and `linux/arm64`, so they run on x86
hosts as well as ARM (AWS Graviton, Azure Cobalt, Apple Silicon, etc.). Docker
automatically selects the matching architecture from the manifest.

## Running

The image's entrypoint is the `pgstream` binary, so any CLI command works
directly:

```sh
docker run --rm ghcr.io/xataio/pgstream:1.1.0 --version
docker run --rm -v "$PWD/pgstream.yaml:/pgstream.yaml:ro" \
  ghcr.io/xataio/pgstream:1.1.0 run -c /pgstream.yaml
```

The image is built `FROM scratch` — it contains only the statically-linked
`pgstream` binary and nothing else (no shell, package manager or libraries). The
image does not hardcode a user, so **non-root must be enforced by the runtime**
(see the Kubernetes example below, or `docker run --user`). pgstream itself does
not require root.

> **Note:** because the image ships nothing but the binary, it does not include
> CA certificates. If you connect pgstream to endpoints whose TLS certificates
> must be verified against the system trust store, mount a CA bundle into the
> container (e.g. `-v /etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro`).

## Verifying provenance

The image and the released binaries carry signed [GitHub artifact
attestations](https://docs.github.com/actions/security-guides/using-artifact-attestations-to-establish-provenance-for-builds)
(build provenance, backed by Sigstore). Verify with the `gh` CLI:

```sh
# Container image (provenance is pushed to the registry alongside the image).
gh attestation verify oci://ghcr.io/xataio/pgstream:1.1.0 --owner xataio

# A downloaded release binary.
gh attestation verify pgstream.linux.amd64 --owner xataio
```

`--owner xataio` pins the attestation to this GitHub organization; add
`--signer-workflow xataio/pgstream/.github/workflows/build.yml` to also pin the
exact workflow that produced it.

## SBOM

An SPDX SBOM is attached to the image as a signed SBOM attestation. Inspect it
with:

```sh
gh attestation verify oci://ghcr.io/xataio/pgstream:1.1.0 \
  --owner xataio \
  --predicate-type https://spdx.dev/Document \
  --format json | jq '.[].verificationResult.statement.predicate'
```

## Running hardened on Kubernetes

pgstream does not need root and does not write to its root filesystem, so it
works with a locked-down `securityContext`. Because the image does not declare a
user, you must set `runAsUser` to a non-zero uid — `runAsNonRoot: true` on its
own would refuse to start the pod (the image default is uid `0`). Any uid works;
`65532` is just a convention:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pgstream
spec:
  replicas: 1
  selector:
    matchLabels:
      app: pgstream
  template:
    metadata:
      labels:
        app: pgstream
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: pgstream
          image: ghcr.io/xataio/pgstream:1.1.0
          args: ["run", "-c", "/etc/pgstream/pgstream.yaml"]
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /etc/pgstream
              readOnly: true
            # The root filesystem is read-only; mount a writable /tmp for any
            # transient files the runtime may need.
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: config
          configMap:
            name: pgstream-config
        - name: tmp
          emptyDir: {}
```
