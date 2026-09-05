# pdbmux — merging PuppetDB proxy

`pdbmux` is a small HTTP daemon that fronts **several** PuppetDB backends and
serves a single, merged PuppetDB v4 query surface on one address. Point
Puppetboard, or any other PuppetDB API client, at `pdbmux` instead of a raw
PuppetDB and it sees one consistent view spanning all of them.

## Why

Running more than one PuppetDB — during a migration between two of them, or
across regions — means a given node's current data lives in exactly one at any
moment, and consumers have to know which, or query each in turn. `pdbmux`
merges them all so consumers don't have to know (or query twice) which PuppetDB
a node currently lives in.

All backends are equal — `pdbmux` is never told which one to favour. Backend
names are arbitrary labels and there is no fixed number of them. The configured
order is used only as a tie-break, so output is reproducible.

## Endpoints

`pdbmux` proxies **GET** requests only. The `query` param (PuppetDB AST JSON,
not PQL) is forwarded verbatim.

| Path | Behaviour |
|---|---|
| `GET /pdb/query/v4/nodes` | Fan out to all backends, dedupe by `certname`, keep the record with the newer `report_timestamp`, stamped with the winning backend's name (see provenance). An `extract`/`count` query is **summed** instead. |
| `GET /pdb/query/v4/facts` | Fan out to all, and per `certname` keep **all** facts from the backend that owns that node (see merge semantics), plus a synthetic `pdbmux_source` fact naming it. |
| `GET /pdb/query/v4/resources` | An `extract`/`count` query is fanned out and **summed**; any other query is an unmerged pass-through. |
| `GET /pdb/query/v4/reports` | Fan out to all and serve the **union**, deduped by report `hash`, re-ordered and re-paged across backends. |
| `GET /pdb/query/v4/events` | Fan out to all and serve the **union**, deduped by record identity, re-ordered and re-paged. |
| `GET /pdb/query/v4/event-counts` | Fan out to all and **sum** each subject's counts into one row per subject. |
| `GET /pdb/query/v4/aggregate-event-counts` | Fan out to all and **sum** the summary object's counts. |
| `GET /pdb/query/v4/reports/<hash>/{events,logs,metrics}` | Ask every backend; serve the answer from whichever backend actually holds that report. `404` when none does. |
| `GET /pdb/query/v4/*` (any other) | No merge rule, so backends are tried in configured order and the first success is streamed back verbatim; if all reject it, the first upstream error response is replayed. |
| `GET /pdb/meta/v1/version` | Fan out to all and report the **lowest** version any backend runs. |
| `GET /pdb/meta/v1/server-time` | Fan out to all and serve the first reachable backend's clock. |
| `GET /metrics/v2/read/<mbean>` | Fan out to all and merge the Jolokia response; numeric attributes are **summed** by default (see merge semantics). |
| `GET /metrics/v2/list` | Fan out to all and serve the **union** of the backends' MBean trees. |
| `GET /metrics/v1/mbeans[/<mbean>]` | Same merge, applied to the legacy envelope-less body. |
| `GET /healthz` | Per-backend reachability plus cache state. `200 {"status":"ok"}` if all reachable, `200 degraded` if some fail, `503 down` if all fail. |

Fan-out is concurrent. If one backend errors or times out, `pdbmux` serves the
surviving backends' results and logs a warning; a merged endpoint only returns `502` when
**every** backend fails. Response records are passed through as raw JSON so
unknown fields survive untouched.

## Merge semantics

- **`/nodes`** — dedupe by `certname`; the record with the strictly-newer
  `report_timestamp` wins. On a tie, the backend listed first in `backends`
  supplies the record — a tie-break only, so the merged output is deterministic.
- **`/facts`** — node-level granularity. For a `certname` present in more than
  one backend, `pdbmux` keeps **all** of that node's facts from **one** backend and
  drops the others', chosen by the merge strategy:
  - **`freshness`** (default) — attribute each `certname` to whichever backend
    holds its newer `report_timestamp`. `pdbmux` derives this from a per-certname
    freshness map built by querying `/nodes` from every backend, cached for
    `freshness_ttl` (default 30s).
  - **`static`** — skip the extra `/nodes` query and take each shared node's
    facts from the first backend in configured order that holds it.
  - A node present in only one backend always appears (falls back to whichever
    backend actually returned facts for it).
- **`/reports`, `/events`** — **union**, not a per-node winner. Reports are
  immutable history, so a node's reports can legitimately exist in more than one
  backend and all of them belong in the merged view. Reports dedupe on `hash`;
  events, which carry no id of their own, dedupe on the verbatim record (a node
  reporting to more than one backend stores identical records in each).
- **Aggregates** — `extract`/`group_by` rows are counts, not records, so each
  backend returns a partial answer that has to be **added**, not deduped. This
  covers `/event-counts`, `/aggregate-event-counts`, and any `/reports`,
  `/nodes` or `/resources` query whose `extract` carries a `["function", ...]`
  column.
  - The grouping key is the row's non-aggregate fields: for `/reports`,
    `/nodes` and `/resources` they come from the query — the plain `extract`
    fields plus any `group_by` clause — and for the event-count endpoints from
    the row itself (`subject_type`/`subject`, or `summarize_by`), whose
    remaining fields are all counts.
  - On `/nodes` this takes precedence over the `certname` merge: a count row has
    no `certname`, so deduping would collapse every backend's count into one
    backend's number. A `/nodes` query with no `function` column — including a
    plain `extract` projection — still merges by `certname`.
  - `/resources` has no cross-backend record identity to dedupe on, so only its
    aggregate queries merge; everything else stays an unmerged pass-through.
  - Rows sharing a key collapse into one with their numeric columns summed. A key
    only one backend reported is passed through byte-for-byte. An aggregate column
    that is absent or non-numeric in a row is skipped, never zeroed, so the
    backends that did report a number still count.
  - A `/reports` query with no `function` column is a projection of real reports,
    not an aggregate, and stays on the union path.
  - `include_total=true` on a summed endpoint reports the **merged** row count,
    not the sum of the backends' `X-Records`, since shared keys collapse.

### Provenance: the `pdbmux_source` fact

Once several PuppetDBs sit behind one endpoint, a consumer can no longer tell
which backend a node's data came from. `pdbmux` makes that visible in the
response itself, so nothing has to query each backend to find out:

- **`/facts`** gains one extra fact record per `certname`, alongside the node's
  real facts, in the shape of a real fact record — `certname`, `name`, `value`,
  `environment` — with `value` set to the **backend name** from `backends` /
  `PDBMUX_BACKENDS`. `environment` is copied from that node's own facts (all
  four keys are always present, since clients index them directly).
- **`/nodes`** gains a `pdbmux_source` **key** on each merged node record. A
  node record carries no facts, so this is a synthetic field, not a fact — the
  one key outside PuppetDB's documented node schema. Clients read node fields by
  name, so an extra key is ignored by anything that doesn't want it.

The value always names the backend **whose data won that endpoint's merge**, not
a backend that merely holds the node. The two endpoints resolve their winner
separately, so under `merge: static` they can legitimately disagree: `/facts`
attributes a shared node to the first backend in configured order, while
`/nodes` always attributes it to the backend holding the newer
`report_timestamp`. Each answer describes the record it is attached to.

While the fact is enabled, any `/facts` record whose own `name` field equals the
configured name is dropped, on every query shape — including the shapes below,
where nothing is injected in its place. The rule reads the record, not the query,
so a projection that filters on `name` without returning it — say
`["extract",["certname","value"],["=","name","pdbmux_source"]]` — produces rows
that no longer identify themselves, and an upstream value of that name comes
through. Ask for the `name` column and the guarantee holds. Each request that
drops a record logs it once. Only `source_fact_enabled: false` restores upstream
records of that name; rename the synthetic fact via `source_fact` if the real one
matters more.

**Injection is skipped**, and no synthetic record is added, when:

- the query contains an `extract` outside a subquery — it projects a column
  subset, and with a `["function", ...]` column it aggregates. Injecting there
  would break the row shape or silently inflate a `count()`, so **aggregate
  results are never changed**. The whole query is walked, so an `extract` nested
  under `and`/`or`/`not`/`from` skips injection too; an `extract` under `in`,
  `subquery`, or `select_<entity>` projects that subquery rather than the
  response, so it does not;
- the query is not an AST array — every **PQL-syntax** query (`facts { certname
  = "web1" }`) lands here. `pdbmux` cannot tell what such a query projects, so it
  never injects into a PQL response. Use the AST form to get the fact;
- (`/facts` only) the query constrains `name` — `["=","name","osfamily"]` and
  friends ask for specific facts, and the synthetic record is not one of them.
  Only the outer query is inspected: a `name` filter inside an `in`/`select_facts`
  subquery narrows which *nodes* match, not which facts come back, so injection
  still happens;
- injection is turned off (see `source_fact_enabled`).

**Not supported in v1: server-side filtering on the fact.** A query that selects
it — `["=","name","pdbmux_source"]`, or an `extract` naming it — is forwarded to
the backends like any other, and they return nothing, because the fact does not
exist upstream. `pdbmux` does not evaluate the AST itself, so it cannot answer
such a query correctly for every operator (`not`, `or`, subqueries) and does not
pretend to for some. Read the fact from an unfiltered (or `certname`-filtered)
`/facts` response and filter client-side. The same applies to the
`/pdb/query/v4/facts/<name>` route, which is served unmerged pass-through.

**Not covered:** `/factsets` and `/inventory`. Both carry facts, but `pdbmux`
does not merge either today — they take the unmerged pass-through path, where
the answer comes from whichever backend replied first rather than from a merge
winner, so there is no owner to attribute. Injecting there would state a
provenance that isn't true.

### Metadata and metrics

- **`/pdb/meta/v1/version`** — when the backends agree, that version is served.
  When they differ, `pdbmux` reports the **lowest**: a client reads this as the
  feature level it may rely on, and the estate can only be relied on for what its
  oldest PuppetDB implements. Versions compare segment by segment, numerically
  where both segments are numbers (`7.9.0` < `7.12.0`), lexically otherwise.
  A backend whose body is unparseable is skipped rather than treated as lowest.
- **`/pdb/meta/v1/server-time`** — the clock of whichever PuppetDB answered is
  not estate state and has no meaningful merge, so the first **reachable**
  backend in configured order supplies it, the same tie-break used elsewhere.
- **`/metrics/...`** — the Jolokia envelope's `value` is merged and the rest of
  the envelope comes from the first backend (with the newest `timestamp`).
  Values merge recursively:
  - Objects merge over the **union** of their keys, so an MBean attribute only
    one backend exposes still survives.
  - Numbers combine by the attribute's own name. The default is a **sum** —
    almost everything here is a population count (`num-nodes`, `num-resources`,
    queue depth, command totals) whose estate-wide value is the total, and rates
    are additive throughput. The exceptions describe a distribution or a bound,
    where adding two servers' numbers yields a figure that was never true of
    either: `Min` takes the minimum; `Max`, `Uptime` and `StartTime` take the
    maximum; `Mean`, `Median`, `StdDev` and `*Percentile` take the unweighted
    arithmetic mean (`pdbmux` has no per-backend sample counts to weight by).
    Matching is case-insensitive.
  - Strings, booleans, arrays, nulls and mixed kinds keep the first backend's
    value — there is no sound way to add them.
  - Jolokia signals a bad MBean as a non-2xx `status` **inside** an HTTP 200.
    Such a backend is skipped; if every backend does so, the first one's error
    envelope is replayed verbatim so the client sees the real reason.
  - MBean names arrive percent-encoded over Jolokia's own `!`-escaping; the raw
    path is forwarded so neither layer is lost.

### Paging and ordering on the merged endpoints

Each backend applies `order_by`/`limit`/`offset` to its own slice only, so
`pdbmux` re-does all three over the union:

- `order_by` is parsed and the merged set re-sorted by those fields (ties keep
  the merged set's existing order). A record missing an ordered field sorts first.
- Backends are asked for the first `offset + limit` records — never an `offset`
  — and the requested window is then cut from the merged, re-sorted set.
- `include_total=true` on a union endpoint makes `pdbmux` sum each backend's
  `X-Records` header into one merged header. Deduped records are counted once per
  backend, so the total is an upper bound. Summed endpoints report the merged row
  count instead.
- A malformed `limit`, `offset` or `order_by` gets a `400` rather than being
  forwarded.

## Caching

`pdbmux` caches merged `/nodes` and `/facts` record sets **in memory** so a busy
Puppetboard does not re-fan-out the same query every few seconds. Everything else
runs uncached — including `extract`/`count` aggregates on those two paths, and
the `/pdb/meta/v1/*` and `/metrics/*` endpoints, which are served live on every
request. The cache is an interface, and `/reports` gets its own (S3-backed)
backend later without further handler changes.

- **Key** — `<path>?<params>`, where the params are the ones that actually
  determine the response, URL-encoded with keys sorted ascending and a repeated
  param's values sorted ascending. Param order in the request is therefore
  irrelevant: one canonical key per distinct request. A request with no params
  keys on the bare path.
- **TTL** — `facts_ttl`, default `30s`, **hard cap `30s`**. A larger configured
  value is **clamped** down to the cap, not rejected, so a stray env var cannot
  crash-loop a container; `pdbmux config show` prints
  `facts_ttl : 30s (clamped from 600s, cap 30s)` when that happens. `facts_ttl: 0`
  disables the cache entirely and the merged endpoints behave exactly as before.
- **Stale on failure only** — an expired entry is kept, not dropped. When the TTL
  has passed `pdbmux` always re-queries the backends; the expired copy is served
  **only** if every backend fails, which turns a `502` into slightly-old data. A
  healthy backend is never shadowed by a stale entry.
- **Bounded** — `facts_cache_bytes` (default 64 MiB) is a byte budget, evicted
  least-recently-used; reads count as use, so a stale entry that is still being
  asked for survives. A single response larger than the whole budget is not
  cached at all. The budget counts stored response bodies only — cache keys and
  the list/map bookkeeping are not accounted for, so it is a target for body
  bytes rather than a hard cap on process memory.
- **Single-flight** — concurrent requests for the same key collapse into one
  upstream fan-out; the rest wait for it and share the result.
- **Visibility** — `/healthz` carries a `cache` object: `backend`
  (`memory`/`none`), `ttl`, `entries`, `stale_entries`, `bytes`, `serving_stale`,
  `stale_served` and `last_stale_served`. `serving_stale` is `true` from the
  moment a stale fallback is served until the next response comes from a live
  fan-out or a fresh entry.

## Config

Precedence (lowest → highest): **defaults < config file < env vars (`PDBMUX_*`) < flags**.

The config file is optional; a file, env vars, or both work equally well,
including in a container.

Which file is read: `--config <path>`, else `PDBMUX_CONFIG`, else the first that
exists of `$XDG_CONFIG_HOME/pdbmux/config.yaml` (or `$HOME/.config/pdbmux/config.yaml`),
then `/etc/pdbmux/config.yaml`. A path given via `--config`/`PDBMUX_CONFIG` **must**
exist — pdbmux fails rather than silently falling back — while a missing file on
the default search path is fine. `pdbmux config show` prints the file it loaded,
or the paths it searched.

```yaml
listen: ":8080"
backends:                          # order is a tie-break only, not a ranking
  - name: pdb-a
    url: http://puppetdb1.example.com:8080
  - name: pdb-b
    url: https://puppetdb2.example.com
merge: freshness             # freshness | static
timeout: 10s                 # per-upstream request timeout
freshness_ttl: 30s           # freshness-map cache TTL (freshness merge only)
facts_ttl: 30s               # /facts + /nodes response cache TTL; 0 disables, capped at 30s
facts_cache_bytes: 67108864  # byte budget for that cache (64 MiB), LRU-evicted
source_fact: pdbmux_source   # name of the synthetic provenance fact
source_fact_enabled: true    # false serves backends' records untouched
```

`backends[*].url` is a **base** URL (`scheme://host[:port]`); `pdbmux` appends
the `/pdb/query/v4/...` path per request.

| Env var | Overrides |
|---|---|
| `PDBMUX_CONFIG` | config file path (not a file key) |
| `PDBMUX_LISTEN` | `listen` |
| `PDBMUX_MERGE` | `merge` |
| `PDBMUX_TIMEOUT` | `timeout` (Go duration, e.g. `10s`) |
| `PDBMUX_FRESHNESS_TTL` | `freshness_ttl` |
| `PDBMUX_FACTS_TTL` | `facts_ttl` (clamped to 30s) |
| `PDBMUX_FACTS_CACHE_BYTES` | `facts_cache_bytes` (plain integer bytes) |
| `PDBMUX_BACKENDS` | whole backend list, as `name=url,name=url` |
| `PDBMUX_SOURCE_FACT` | `source_fact` (default `pdbmux_source`) |
| `PDBMUX_SOURCE_FACT_ENABLED` | `source_fact_enabled` (default `true`); `false` disables injection |

Flags: `--config`, `--listen`, `--merge`.

`config init` writes to `--config`/`PDBMUX_CONFIG` when set, else to
`$XDG_CONFIG_HOME/pdbmux/config.yaml`.

## Running

Subcommands: `serve` (default), `config init`, `config show`, `version`. Run
`pdbmux --help` for details. Any PuppetDB v4 client works against the `pdbmux`
base URL in place of a PuppetDB one.

```bash
PDBMUX_BACKENDS='pdb-a=http://puppetdb1.example.com:8080,pdb-b=http://puppetdb2.example.com:8080' pdbmux
curl -s --get http://localhost:8080/pdb/query/v4/nodes \
  --data-urlencode 'query=["=","certname","host1.example.com"]'
```

## Build

`make build` (static binary into `dist/`), `make test`, `make lint`. Requires Go 1.25+.

## Deployment

Container image only — no OS package. Every `v*` tag builds and pushes the image
(the release pipeline); registry and repository are pipeline settings. Tag
with `make patch` / `minor` / `major`.

A static (`CGO_ENABLED=0`) binary on a distroless base. Configure it with
`PDBMUX_*` env vars (at minimum `PDBMUX_BACKENDS`), or mount a config file — a
configmap at `/etc/pdbmux/config.yaml` is picked up with no env var at all, and
any other mount path works via `PDBMUX_CONFIG`. Env vars still override file
values, so the two mix. Run as many replicas as you like — the only state is the
in-memory cache, which is per-replica and bounded by `facts_cache_bytes`, so size
the memory limit above it. Use `/healthz` for liveness/readiness probes.
