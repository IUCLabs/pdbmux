# pdbmux — merging PuppetDB proxy

`pdbmux` is a small HTTP daemon that fronts **two** PuppetDB backends and serves
a single, merged PuppetDB v4 query surface on one address. Point a PuppetDB client,
Puppetboard, or anything else at `pdbmux` instead of a raw
PuppetDB and it sees one consistent view spanning both.

## Why

During the VM→k8s Puppet migration there are two PuppetDBs:

- **old** — the legacy VM-registered `http://puppetdb.example.com:8080`
- **new** — the k8s `https://puppetdb.k8s.example.net` (TLS terminated at
  the gateway; backends are plain PuppetDB on 8080)

Nodes move from old to new as they migrate, so at any moment a given node's
current data lives in exactly one of them. `pdbmux` merges both so consumers
don't have to know (or query twice) which PuppetDB a node currently lives in.

## Endpoints

`pdbmux` proxies **GET** requests only. The `query` param (PuppetDB AST JSON,
not PQL) is forwarded verbatim.

| Path | Behaviour |
|---|---|
| `GET /pdb/query/v4/nodes` | Fan out to both backends, dedupe by `certname`, keep the record with the newer `report_timestamp`. |
| `GET /pdb/query/v4/facts` | Fan out to both, and per `certname` keep **all** facts from the backend that owns that node (see merge semantics). |
| `GET /pdb/query/v4/reports` | Fan out to both and serve the **union**, deduped by report `hash`, re-ordered and re-paged across the two backends. |
| `GET /pdb/query/v4/events` | Fan out to both and serve the **union**, deduped by record identity, re-ordered and re-paged. |
| `GET /pdb/query/v4/reports/<hash>/{events,logs,metrics}` | Ask both; serve the answer from whichever backend actually holds that report. `404` when neither does. |
| `GET /pdb/query/v4/*` (any other) | Transparently proxied to the **primary** backend, unmerged, streamed verbatim. |
| `GET /healthz` | Per-backend reachability. `200 {"status":"ok"}` if all reachable, `200 degraded` if some fail, `503 down` if all fail. |

Fan-out is concurrent. If one backend errors or times out, `pdbmux` serves the
survivor's results and logs a warning; a merged endpoint only returns `502` when
**every** backend fails. Response records are passed through as raw JSON so
unknown fields survive untouched.

## Merge semantics

- **`/nodes`** — dedupe by `certname`; the record with the strictly-newer
  `report_timestamp` wins. On a tie (or when a node exists in only one backend),
  the **preferred** backend's record is kept.
- **`/facts`** — node-level granularity. For a `certname` present in both
  backends, `pdbmux` keeps **all** of that node's facts from **one** backend and
  drops the other's, chosen by the merge strategy:
  - **`freshness`** (default) — attribute each `certname` to whichever backend
    holds its newer `report_timestamp`. `pdbmux` derives this from a per-certname
    freshness map built by querying `/nodes` from both backends, cached for
    `freshness_ttl` (default 30s). Ties/fallbacks use `prefer`.
  - **`static`** — always keep the `prefer` backend's facts for shared nodes.
    No extra `/nodes` query.
  - A node present in only one backend always appears (falls back to whichever
    backend actually returned facts for it).
- **`/reports`, `/events`** — **union**, not a per-node winner. Reports are
  immutable history, so a node that migrated legitimately has reports in the old
  PuppetDB *and* the new one and both belong in the merged view. Reports dedupe
  on `hash`; events, which carry no id of their own, dedupe on the verbatim
  record (a node briefly reporting to both PuppetDBs stores identical records in
  each). Records the merge cannot key — `extract`/`group_by` aggregate rows — are
  all kept rather than collapsed, so no backend's rows are silently dropped;
  summing those aggregates across backends is not implemented yet.

### Paging and ordering on the merged endpoints

Each backend applies `order_by`/`limit`/`offset` to its own slice only, so
`pdbmux` re-does all three over the union:

- `order_by` is parsed and the merged set re-sorted by those fields (ties keep
  backend precedence). A record missing an ordered field sorts first.
- Backends are asked for the first `offset + limit` records — never an `offset`
  — and the requested window is then cut from the merged, re-sorted set.
- `include_total=true` makes `pdbmux` sum each backend's `X-Records` header into
  one merged header. Deduped records are counted once per backend, so the total
  is an upper bound.
- A malformed `limit`, `offset` or `order_by` gets a `400` rather than being
  forwarded.

## Config

Precedence (lowest → highest): **defaults < config file < env vars (`PDBMUX_*`) < flags**.

Config file: `$XDG_CONFIG_HOME/pdbmux/config.yaml`. In Kubernetes, configuration
is supplied entirely via `PDBMUX_*` env vars (no config file), which is the
supported deployment path — see [Deployment](#deployment).

```yaml
# ~/.config/pdbmux/config.yaml  (local dev; in k8s use PDBMUX_* env instead)
listen: ":8080"
backends:
  - name: old
    url: http://puppetdb.example.com:8080
  - name: new
    url: https://puppetdb.k8s.example.net
primary: new          # backend used for non-merged /pdb/query/v4/* pass-through
merge: freshness      # freshness | static
prefer: new           # winner on ties / static merge / fallback
timeout: 10s          # per-upstream request timeout
freshness_ttl: 30s    # freshness-map cache TTL (freshness merge only)
```

`backends[*].url` is a **base** URL (`scheme://host[:port]`), without the
`/pdb/query/v4/...` path — `pdbmux` appends the path per request.

| Env var | Overrides |
|---|---|
| `PDBMUX_LISTEN` | `listen` |
| `PDBMUX_PRIMARY` | `primary` |
| `PDBMUX_MERGE` | `merge` |
| `PDBMUX_PREFER` | `prefer` |
| `PDBMUX_TIMEOUT` | `timeout` (Go duration, e.g. `10s`) |
| `PDBMUX_FRESHNESS_TTL` | `freshness_ttl` |
| `PDBMUX_BACKENDS` | whole backend list, as `name=url,name=url` |

Flags: `--listen`, `--primary`, `--merge`.

## Running

```bash
pdbmux                 # start the proxy (serve is the default action)
pdbmux serve           # explicit
pdbmux config init     # write a default config file
pdbmux config show     # print active config after all overrides
pdbmux version
```

Point a consumer at it:

```bash
curl -s http://localhost:8080/pdb/query/v4/facts
curl -s 'http://localhost:8080/pdb/query/v4/reports?limit=1'
```

## Build

```bash
make build             # -> dist/pdbmux (CGO disabled, static)
make test              # go test -race ./...
make lint              # golangci-lint
```

Requires Go 1.25+. Dependencies: `github.com/spf13/cobra` (CLI),
`gopkg.in/yaml.v3` (config file).

## Deployment

`pdbmux` runs **in Kubernetes** as a container, in line with the all-in-k8s
estate direction — it is not shipped as a per-VM RPM/systemd service. The image
is built and pushed on every `v*` tag (the release pipeline) to:

```
registry.example.com/internal/pdbmux:<tag>
```

It is a minimal static (`CGO_ENABLED=0`) binary on a distroless base
(`Dockerfile`), configured entirely via `PDBMUX_*` env vars, with a single HTTP
listener and `/healthz` for liveness/readiness probes.

The Deployment/Service/Gateway manifests live in your GitOps
repo (namespace `pdbmux`, 2 replicas), and it is exposed to
VM/workstation clients over HTTPS at:

```
https://pdbmux.k8s.example.net
```

Locally you can still run the binary directly for development:

```bash
PDBMUX_BACKENDS='old=http://puppetdb.example.com:8080,new=http://puppetdb.puppet.svc.cluster.local:8080' \
  pdbmux serve
curl -s localhost:8080/healthz
```

## Version bumps

```bash
make patch             # tag vX.Y.(Z+1) and push (triggers the docker release)
make minor             # tag vX.(Y+1).0
make major             # tag v(X+1).0.0
```
