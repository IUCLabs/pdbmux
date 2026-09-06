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
| `GET /pdb/query/v4/facts` | Fan out to all, and per `certname` keep **all** facts from the backend that owns that node (see merge semantics), plus a synthetic `pdbmux_source` fact naming it. An `extract`/`count` query is **summed** instead. |
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
| `GET /healthz` | Per-backend reachability, probe state and cache state. `200 {"status":"ok"}` if all reachable, `200 degraded` if some fail, `503 down` if all fail. |

Fan-out is concurrent, and goes only to the backends the health prober currently
believes are up — see [Backend health](#backend-health). If one backend errors or
times out, `pdbmux` serves the surviving backends' results and logs a warning; a
merged endpoint only returns `502` when **every** backend fails. Response records
are passed through as raw JSON so unknown fields survive untouched.

Responses carry PuppetDB's `X-Records` when the query asked for a total, and on
the merged paths `X-Backends` (see [Backend health](#backend-health)). Cached
paths add two more headers `pdbmux` sets itself, `X-Cache` and `Age` — see
[Caching](#caching).

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
  `/nodes`, `/facts` or `/resources` query whose `extract` carries a
  `["function", ...]` column.
  - The grouping key is the row's non-aggregate fields: for `/reports`,
    `/nodes`, `/facts` and `/resources` they come from the query — the plain
    `extract` fields plus any `group_by` clause — and for the event-count
    endpoints from the row itself (`subject_type`/`subject`, or
    `summarize_by`), whose remaining fields are all counts.
  - On `/nodes` and `/facts` this takes precedence over the `certname` merge: an
    aggregate row has no `certname`, so deduping would collapse every backend's
    rows into one backend's numbers. A query with no `function` column —
    including a plain `extract` projection — still merges by `certname`.
  - PuppetDB accepts `count`, `sum`, `avg`, `min`, `max`, `to_string` and
    `jsonb_typeof` as `extract` functions. Only `count` and `sum` are additive,
    so only those two merge correctly. `avg`, `min` and `max` are folded like any
    other numeric column and their merged value is **wrong**; query a single
    backend directly for those.
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

## Backend health

A backend that is down otherwise costs a full `timeout` stall on **every**
request, since fan-out has no way to know before it asks. `pdbmux` polls each
backend's status endpoint in the background instead, and skips the ones that are
not answering.

- **Endpoint** — `health_probe_path`, default `/status/v1/services`, PuppetDB's
  trapperkeeper status service (unauthenticated by default). A backend is healthy
  when it answers `200` **and** every service in the body reports
  `"state": "running"` — a `200` whose body says `starting`, `stopping`, `error`
  or `unknown` counts as a failure. A body that is not in that shape is judged on
  its status code alone, so pointing `health_probe_path` at some other endpoint
  still works.
- **A refused probe is not a sick backend.** Probe replies split in two. Evidence
  about the *backend* is a transport failure (connection refused, DNS, TLS,
  timeout), a `5xx` — `503` included, since trapperkeeper answers `503` exactly
  when its services are not nominal — or a `429`, which is the backend reporting
  its own capacity rather than judging the request, so an overloaded backend gets
  backed off instead of kept at full traffic. A reply that refuses the *probe
  request* is evidence about the probe: the other `4xx` are the backend
  answering that our request is the problem (`404`/`410` the path is not there,
  `405` it does not take a `GET`, `401`/`403` we are not allowed to ask), and
  `501` says it does not implement the endpoint.
- **A backend is only gated on a probe that has worked for it.** Each backend
  carries one latch: has its probe endpoint ever *answered* — replied with
  something readable as healthy or unhealthy — since `pdbmux` started? A
  rejection refused the request and a transport failure never reached the
  endpoint, so neither one sets it; a `200`, a `503` or a degraded body does.
  The latch decides which rule applies, and it never clears, so no repeating
  pattern of failures can argue a backend back into service.
  - **Never answered** — there is no health signal for this backend, so nothing
    gates on one. It is **left in service** — still queried, still contributing
    records — permanently, reported as `probe_unsupported` rather than
    `healthy` so an operator can tell "verified healthy" from "not actually
    being checked". Real failures do not take it out either: no conclusion about
    a backend can be drawn from a probe that cannot run. This is the
    misconfigured-path case, and it degrades that backend to the behaviour from
    before health checks existed, which is the right floor. The
    misconfiguration is logged once, naming the backend, the probe path and the
    status. A backend that has been unreachable since `pdbmux` started has not
    answered either, so it is not gated until it answers once — `reachable` on
    `/healthz` is what reports it in the meantime.
  - **Answered at least once** — the path works, so the probe is trusted and the
    ordinary thresholds below apply. A later run of *rejections* counts as
    failure, not `probe_unsupported`: a path that answered before and refuses
    now has moved or changed its authorization, which is logged loudly when the
    run starts.
- **Thresholds** — a healthy backend leaves the pool after
  `health_probe_failures` (default 3) **consecutive** unsuccessful probes; a down
  one comes back after `health_probe_successes` (default 2) consecutive
  successes, and the same failure threshold debounces the `probe_unsupported`
  warning. One blip cannot flap a backend out, and one lucky reply cannot flap
  it back in. The run counts every unsuccessful probe whatever its kind, so a
  backend that fails every probe in mixed ways — a `503`, then a `404`, then a
  timeout — still trips the threshold; only a success resets the run. Probes
  from before the latch was set do not count toward it. A down backend keeps
  being probed, so recovery is automatic.
- **Accepted trade-off: a probe path that is removed.** If a backend's probe
  path works and later goes away — an upgrade, a proxy change — the latch is
  already set, so the refusals count as failures and that backend is excluded
  even though it is serving queries fine. Global fail-open still covers the case
  where this happens to every backend, `/healthz` shows the state, and the log
  line names the probe path: fix `health_probe_path`, or set
  `health_probe_enabled: false`. There is no machinery to detect this
  automatically — any rule that readmits a backend on "no real failure lately"
  flaps a genuinely dead backend into service on a periodic failure pattern.
- **Fails open** — if the prober has marked **every** backend down, `pdbmux`
  queries them all anyway. A wrong `health_probe_path`, a broken prober or a
  partition that only the prober sees can therefore never black-hole traffic;
  the worst case is today's behaviour.
- **Serves immediately** — the listener never waits for a first probe round, and
  a backend nobody has probed yet counts as healthy, so a restart drops nothing.
- **Quiet** — only *transitions* are logged, never individual probes: up→down,
  down→up, a probe that has never answered reaching its failure threshold, the
  first answer after that, and the start of a rejection run on a probe that used
  to answer.
- **Partial responses stay partial.** Health state changes which backends are
  asked, never what a merged answer means: a response built from a subset is
  still served, as before. Every merged response carries `X-Backends:
  <contributed>/<configured>` naming how many backends' records went into it, so
  a client can tell a full answer from a partial one. On a cache hit the header
  describes the stored body, not the current backend count.
- **`/healthz`** gives each backend a `state` (`healthy`, `unhealthy`,
  `probe_unsupported`, `unprobed`, or `unmonitored` when probing is off),
  `consecutive_failures`,
  `consecutive_successes`, `last_probe` and `last_error`, alongside the
  `reachable` check `/healthz` runs itself — which always asks **every**
  backend, so a backend queries are skipping is still reported. Read the two
  together: `state` is the prober's verdict and `probe_unsupported` means "not
  being verified", *not* "well", so `reachable` is the field that says whether
  the backend is answering right now. A proxy that `404`s everything because the
  backend behind it is dead shows `state: probe_unsupported` with `reachable`
  carrying the query error, and the overall `status` drops to `degraded` or
  `down` accordingly. A `query` object reports the last merged fan-out:
  `partial`, `contributed`, `configured`, `partial_rounds` and `last_partial`.
- **`health_probe_enabled: false`** turns the whole thing off: no probing
  goroutines, no backend ever skipped, every backend queried on every request.
  `X-Backends` still reports how many answered.

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
  upstream fan-out; the rest wait for it and share the result. That fan-out runs
  on its own context, bounded by `timeout`, so a client that disconnects can
  neither cancel nor fail the requests sharing its flight; a waiter whose own
  client goes away leaves the flight running for the others. The flight is
  cancelled once its last participant leaves, so a lone client disconnecting
  releases the upstream connections straight away.
- **Response headers** — every response on a cached path carries `X-Cache`
  (`hit` served from a fresh entry, `miss` built by this request, `stale` the
  expired-entry fallback) and `Age` in whole seconds since the served copy was
  stored (`0` on a `miss`). Uncached paths carry neither.
- **Visibility** — `/healthz` carries a `cache` object: `backend`
  (`memory`/`none`), `ttl`, `entries`, `stale_entries`, `bytes`, `serving_stale`,
  `stale_served` and `last_stale_served`. `serving_stale` is `true` from the
  moment a stale fallback is served until the next response comes from a live
  fan-out or a fresh entry.
- **Provenance is stored, not re-applied** — what a cache entry holds is the
  fully merged body, `pdbmux_source` already injected and upstream records of
  that name already dropped. Attribution names the backend that supplied the
  data, which is a property of that fetch, so it stays correct for as long as the
  body does and ages out with it — `X-Cache` and `Age` say how old both are. Two
  requests can only share an entry when they share a key, and the key is path
  plus query, which is exactly what decides whether injection applies; a
  name-filtered `/facts` query and a plain one therefore cache separately and
  neither is ever served the other's shape. `source_fact` and
  `source_fact_enabled` are read once at startup, and the cache lives for the
  same process, so changing either cannot leave differently-shaped entries
  behind.

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
health_probe_enabled: true          # false queries every backend on every request
health_probe_path: /status/v1/services  # backend health endpoint
health_probe_interval: 10s          # how often each backend is probed
health_probe_timeout: 5s            # per-probe timeout
health_probe_failures: 3            # consecutive failures before a backend is skipped
health_probe_successes: 2           # consecutive successes before it is used again
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
| `PDBMUX_HEALTH_PROBE_ENABLED` | `health_probe_enabled` (default `true`) |
| `PDBMUX_HEALTH_PROBE_PATH` | `health_probe_path` (default `/status/v1/services`) |
| `PDBMUX_HEALTH_PROBE_INTERVAL` | `health_probe_interval` (Go duration) |
| `PDBMUX_HEALTH_PROBE_TIMEOUT` | `health_probe_timeout` (Go duration) |
| `PDBMUX_HEALTH_PROBE_FAILURES` | `health_probe_failures` (plain integer, minimum 1) |
| `PDBMUX_HEALTH_PROBE_SUCCESSES` | `health_probe_successes` (plain integer, minimum 1) |

Flags: `--config`, `--listen`, `--merge`, `--health-probe`.

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

## End-to-end tests

`make e2e` runs the suite against **real** PuppetDB backends: two openvoxdb
containers, each on its own PostgreSQL, loaded over the command API
(`replace facts` v5, `store report` v8, `replace catalog` v9, `deactivate node`
v3) and queried through `pdbmux`. Two real clients — Puppetboard and, when a
binary is available, `node-lookup` — are pointed at `pdbmux` and asserted on. It
needs a container runtime and takes a couple of minutes, so it sits behind the
`e2e` build tag and never runs as part of `make test` or `go test ./...`.

Commands are submitted with `secondsToWaitForCompletion`, so the harness waits
on PuppetDB actually processing each one rather than sleeping, and the fixture
load ends by polling `queue_depth` on `/status/v1/services` until both backends
have drained.

| Env var | Overrides |
|---|---|
| `PDBMUX_E2E_OPENVOXDB_IMAGE` | `ghcr.io/openvoxproject/openvoxdb:8.15.0` |
| `PDBMUX_E2E_POSTGRES_IMAGE` | `docker.io/library/postgres:17-alpine` |
| `PDBMUX_E2E_PUPPETBOARD_IMAGE` | `ghcr.io/voxpupuli/puppetboard:latest` |
| `PDBMUX_E2E_TUNNEL_IMAGE` | `docker.io/library/alpine:3` |
| `PDBMUX_E2E_NODE_LOOKUP` | path to a `node-lookup` binary (else `PATH`, else skipped) |

Two tests skip rather than assert, each naming a known gap and failing if that
gap closes: `/pdb/query/v4/facts/<name>` is served unmerged (so Puppetboard's
single-fact drilldown silently loses the other backend's nodes), and
`/fact-names` is likewise unmerged (so the facts overview lists only the first
backend's fact names).

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
