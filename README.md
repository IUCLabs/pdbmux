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
| `GET /pdb/query/v4/event-counts` | Fan out to all and **sum** each subject's counts into one row per subject. |
| `GET /pdb/query/v4/aggregate-event-counts` | Fan out to all and **sum** the summary object's counts. |
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
  each).
- **Aggregates** — `extract`/`group_by` rows are counts, not records, so each
  backend returns a partial answer that has to be **added**, not deduped. This
  covers `/event-counts`, `/aggregate-event-counts`, and a `/reports` query whose
  `extract` carries a `["function", ...]` column.
  - The grouping key is the row's non-aggregate fields: for `/reports` they come
    from the query — the plain `extract` fields plus any `group_by` clause — and
    for the event-count endpoints from the row itself (`subject_type`/`subject`,
    or `summarize_by`), whose remaining fields are all counts.
  - Rows sharing a key collapse into one with their numeric columns summed. A key
    only one backend reported is passed through byte-for-byte. An aggregate column
    that is absent or non-numeric in a row is skipped, never zeroed, so the
    backends that did report a number still count.
  - A `/reports` query with no `function` column is a projection of real reports,
    not an aggregate, and stays on the union path.
  - `include_total=true` on a summed endpoint reports the **merged** row count,
    not the sum of the backends' `X-Records`, since shared keys collapse.

### Paging and ordering on the merged endpoints

Each backend applies `order_by`/`limit`/`offset` to its own slice only, so
`pdbmux` re-does all three over the union:

- `order_by` is parsed and the merged set re-sorted by those fields (ties keep
  backend precedence). A record missing an ordered field sorts first.
- Backends are asked for the first `offset + limit` records — never an `offset`
  — and the requested window is then cut from the merged, re-sorted set.
- `include_total=true` on a union endpoint makes `pdbmux` sum each backend's
  `X-Records` header into one merged header. Deduped records are counted once per
  backend, so the total is an upper bound. Summed endpoints report the merged row
  count instead.
- A malformed `limit`, `offset` or `order_by` gets a `400` rather than being
  forwarded.

## Config

Precedence (lowest → highest): **defaults < config file < env vars (`PDBMUX_*`) < flags**.

Config file: `$XDG_CONFIG_HOME/pdbmux/config.yaml`. In a container there is no
config file — everything comes from `PDBMUX_*` env vars.

```yaml
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
`pdbmux --help` for details. Any PuppetDB v4 client works against the `pdbmux`
base URL in place of a PuppetDB one.

```bash
PDBMUX_BACKENDS='old=http://puppetdb1.example.com:8080,new=http://puppetdb2.example.com:8080' pdbmux
curl -s --get http://localhost:8080/pdb/query/v4/nodes \
  --data-urlencode 'query=["=","certname","host1.example.com"]'
```

## Build

`make build` (static binary into `dist/`), `make test`, `make lint`. Requires Go 1.25+.

## Deployment

Container image only — no OS package. Every `v*` tag builds and pushes the image
(the release pipeline); registry and repository are pipeline settings. Tag
with `make patch` / `minor` / `major`.

A static (`CGO_ENABLED=0`) binary on a distroless base, configured entirely via
`PDBMUX_*` env vars; a container needs at minimum `PDBMUX_BACKENDS`. Stateless,
so run as many replicas as you like; use `/healthz` for liveness/readiness
probes.
