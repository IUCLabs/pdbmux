# pdbmux — merging PuppetDB proxy

`pdbmux` is a small HTTP daemon that fronts **several** PuppetDB backends and
serves a single, merged PuppetDB v4 query surface on one address. Point
Puppetboard, or any other PuppetDB API client, at `pdbmux` instead of a raw
PuppetDB and it sees one consistent view spanning all of them.

## Why

Running more than one PuppetDB — during a migration between two of them, or
across regions — means a given node's current data lives in exactly one at any
moment, and consumers have to know which, or query each in turn. Consider two
backends being merged during a migration:

- **old** — the PuppetDB nodes are moving off, e.g. `http://puppetdb1.example.com:8080`
- **new** — the PuppetDB nodes are moving on to, e.g. `http://puppetdb2.example.com:8080`

Nodes move from old to new as they migrate. `pdbmux` merges both so consumers
don't have to know (or query twice) which PuppetDB a node currently lives in.
The backend names are arbitrary labels; there is no fixed number of backends.

## Endpoints

`pdbmux` proxies **GET** requests only. The `query` param (PuppetDB AST JSON,
not PQL) is forwarded verbatim.

| Path | Behaviour |
|---|---|
| `GET /pdb/query/v4/nodes` | Fan out to all backends, dedupe by `certname`, keep the record with the newer `report_timestamp`. |
| `GET /pdb/query/v4/facts` | Fan out to all, and per `certname` keep **all** facts from the backend that owns that node (see merge semantics). |
| `GET /pdb/query/v4/reports` | Fan out to all and serve the **union**, deduped by report `hash`, re-ordered and re-paged across backends. |
| `GET /pdb/query/v4/events` | Fan out to all and serve the **union**, deduped by record identity, re-ordered and re-paged. |
| `GET /pdb/query/v4/reports/<hash>/{events,logs,metrics}` | Ask every backend; serve the answer from whichever backend actually holds that report. `404` when neither does. |
| `GET /pdb/query/v4/*` (any other) | Transparently proxied to the **primary** backend, unmerged, streamed verbatim. |
| `GET /healthz` | Per-backend reachability. `200 {"status":"ok"}` if all reachable, `200 degraded` if some fail, `503 down` if all fail. |

Fan-out is concurrent. If one backend errors or times out, `pdbmux` serves the
surviving backends' results and logs a warning; a merged endpoint only returns `502` when
**every** backend fails. Response records are passed through as raw JSON so
unknown fields survive untouched.

## Merge semantics

- **`/nodes`** — dedupe by `certname`; the record with the strictly-newer
  `report_timestamp` wins. On a tie (or when a node exists in only one backend),
  the **preferred** backend's record is kept.
- **`/facts`** — node-level granularity. For a `certname` present in more than
  one backend, `pdbmux` keeps **all** of that node's facts from **one** backend and
  drops the other's, chosen by the merge strategy:
  - **`freshness`** (default) — attribute each `certname` to whichever backend
    holds its newer `report_timestamp`. `pdbmux` derives this from a per-certname
    freshness map built by querying `/nodes` from every backend, cached for
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
  never deduped, so every backend's rows pass through even when byte-identical;
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

Config file: `$XDG_CONFIG_HOME/pdbmux/config.yaml`. In a container, configuration
is supplied entirely via `PDBMUX_*` env vars (no config file) — see
[Deployment](#deployment).

**`backends` has no default.** `pdbmux` refuses to start until at least one
backend is configured, via the config file or `PDBMUX_BACKENDS`. `primary` and
`prefer` default to the first configured backend.

```yaml
# ~/.config/pdbmux/config.yaml  (local dev; in a container use PDBMUX_* env instead)
listen: ":8080"
backends:
  - name: old
    url: http://puppetdb1.example.com:8080
  - name: new
    url: https://puppetdb2.example.com
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
pdbmux config init     # write an example config file to edit
pdbmux config show     # print active config after all overrides
pdbmux version
```

Point a consumer at it — any PuppetDB v4 client works, it just needs the
`pdbmux` base URL in place of a PuppetDB one. For Puppetboard, that is its
PuppetDB host/port setting; for a raw query:

```bash
curl -s --get http://localhost:8080/pdb/query/v4/nodes \
  --data-urlencode 'query=["=","certname","host1.example.com"]'
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

`pdbmux` is distributed as a container image only — there is no OS package. The
image is built and pushed on every `v*` tag (the release pipeline); the
registry and repository are pipeline settings, so point them at your own.

It is a minimal static (`CGO_ENABLED=0`) binary on a distroless base
(`Dockerfile`), configured entirely via `PDBMUX_*` env vars, with a single HTTP
listener and `/healthz` for liveness/readiness probes. It is stateless, so run
as many replicas as you like behind an ordinary Service/Ingress.

A container needs at minimum `PDBMUX_BACKENDS`; everything else has a default:

```yaml
env:
  - name: PDBMUX_BACKENDS
    value: "old=http://puppetdb1.example.com:8080,new=http://puppetdb2.example.com:8080"
  - name: PDBMUX_PRIMARY
    value: "new"
  - name: PDBMUX_PREFER
    value: "new"
```

Locally you can run the binary directly for development:

```bash
PDBMUX_BACKENDS='old=http://puppetdb1.example.com:8080,new=http://puppetdb2.example.com:8080' \
  pdbmux serve
curl -s localhost:8080/healthz
```

## Version bumps

```bash
make patch             # tag vX.Y.(Z+1) and push (triggers the docker release)
make minor             # tag vX.(Y+1).0
make major             # tag v(X+1).0.0
```
