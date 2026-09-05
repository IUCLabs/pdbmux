# pdbmux — merging PuppetDB proxy

`pdbmux` is a small HTTP daemon that fronts **two** PuppetDB backends and serves
a single, merged PuppetDB v4 query surface on one address. Point a PuppetDB client,
or anything else at `pdbmux` instead of a raw PuppetDB and it
sees one consistent view spanning both.

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

## Config

Precedence (lowest → highest): **defaults < config file < env vars (`PDBMUX_*`) < flags**.

Config file: `$XDG_CONFIG_HOME/pdbmux/config.yaml`. In Kubernetes there is no
config file — everything comes from `PDBMUX_*` env vars.

```yaml
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

`backends[*].url` is a **base** URL (`scheme://host[:port]`); `pdbmux` appends
the `/pdb/query/v4/...` path per request.

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

Subcommands: `serve` (default), `config init`, `config show`, `version`. Run
`pdbmux --help` for details.

```bash
PDBMUX_BACKENDS='old=http://puppetdb.example.com:8080,new=https://puppetdb.k8s.example.net' pdbmux
curl -s http://localhost:8080/pdb/query/v4/facts
```

## Build

`make build` (static binary into `dist/`), `make test`, `make lint`. Requires Go 1.25+.

## Deployment

Kubernetes only — no RPM. Every `v*` tag builds and pushes
`registry.example.com/internal/pdbmux:<tag>`
(the release pipeline); tag with `make patch` / `minor` / `major`.

Manifests live in your GitOps repo (namespace `pdbmux`,
2 replicas). Use `/healthz` for liveness/readiness probes. Reachable from VMs and
workstations at `https://pdbmux.k8s.example.net`.
