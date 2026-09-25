# Replication and failover across Caltech and UMN

Research spike: how the UMN instance can spin up and join the Caltech
instance as a replica, so the outside world sees one endpoint, survives the
loss of either site, and fails over the writer role from Caltech (primary)
to UMN (secondary).

Status: proposal, not implemented. Nothing here changes runtime behavior.

Core model: Caltech is the seed. Every UMN stateful service joins Caltech
on boot (MongoDB `rs.add`, Kafka replica assignment or MM2 replication,
API and frontend registration behind one load balancer). UMN serves reads
from its local replicas; only the active site writes.

## 1. Where we start

From `docs/deployment.md`, `docker-compose.yaml`, and `config.yaml`:

- Two independent single-node Compose stacks. Caltech (`kaboom`,
  `*.kaboom.caltech.edu`, automated deploy) is primary production. UMN (HPC
  cluster, manual deploy, `config/prod/umn/`) is an independent instance
  that can take over, not a hot standby.
- What diverges today: MongoDB contents (alerts, objects, crossmatch
  catalogs, filters), Kafka consumer offsets and backfill history, and
  per-site config (`overrides.yaml`). What already converges: user accounts
  via a recurring UMN-side sync.
- MongoDB: standalone `mongo:8.2` container, no replica set
  (`replica_set: null`), and `src/conf.rs` builds the URI with
  `directConnection=true`. Any replica-set design must change that URI
  construction and the `replica_set` config plumbing.
- Kafka: single combined broker+controller (`apache/kafka:4.3.1`,
  `KAFKA_PROCESS_ROLES=broker,controller`, `NODE_ID=1`,
  `OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`).
  Internal traffic is unauthenticated PLAINTEXT on `broker:29092`;
  external read-only access is SASL/SCRAM on `:9093` behind Traefik
  (see `docs/kafka-auth.md`). The broker is a single point of failure with
  no redundancy even within one site.
- API and frontend are already effectively stateless. The API reads config
  plus Mongo/Valkey/Kafka; the frontend is a per-deployment nginx build
  whose API origin and `VITE_*` values are baked in at build time. Each
  site has its own Traefik handling TLS and per-host routing.
- The pipeline is stateful at the edges: Valkey holds the alert,
  enrichment, and filter queues (ephemeral, per-site), while MongoDB is
  the system of record and Kafka holds the output topics
  (`ZTF_alerts_results`, `LSST_alerts_results`, `babamul.*`, etc.).
- Upstream ingestion (ZTF, LSST, WINTER) is per-site consumer groups, so
  both sites independently consume the same upstream nights today.

Goal: one public endpoint, redundant serving, and a practiced way to move
the writer role to UMN. Non-goal for this spike: active-active writes to
the same collections from both sites at once.

## 2. Target topology (recommended)

Active-passive where UMN joins Caltech on startup, with manual (later
automated) promotion of UMN to writer:

```text
                    upstream ZTF/LSST/WINTER
                               |
                 +-------------+-------------+
                 |                           |
          Caltech (main)              UMN (replica, joins on boot)
   API + frontend + workers      API + frontend (joins LB pool)
   Mongo PRIMARY                 Mongo SECONDARY ---- joins via rs.add
   Kafka leader partitions       Kafka follower replicas --- joins via
                                 replica assignment or MM2
   workers active                workers + upstream consumers paused
                 |                           |
                 +-------------+-------------+
                               |
                    single public endpoint
                    (load balancer + DNS failover)
```

- Boot: UMN containers start, reach Caltech over the WAN, and join as
  replicas. MongoDB does an initial sync, Kafka catches up follower
  replicas or MM2 consumer lag, then UMN serves reads locally.
- Normal operation: all writes land on Caltech. UMN's pipeline workers
  and upstream consumers stay stopped; UMN's API serves reads from its
  local secondary.
- Failover: promote the already-synced UMN replicas to writer (MongoDB
  election or forced reconfig, Kafka leader election, start UMN workers),
  then move the single public endpoint to UMN.
- Failback: Caltech rejoins as the replica and resyncs, then is promoted
  back. The join path must work in both directions.

## 3. MongoDB: UMN joins a stretched replica set

This is the load-bearing piece. If MongoDB is shared, filters, users,
alerts, and catalog ingests converge for free, which fixes the largest
source of Caltech/UMN divergence. Native replica-set replication is the
right fit here; no application-level sync can match it.

### 3.1 Recommended: one replica set across both sites, UMN joins on boot

- 3 voting members minimum; 2 members cannot survive the loss of either
  site because the survivor cannot reach majority. Standard layout:
  - Caltech: 2 data-bearing members (primary + secondary).
  - UMN: 1 data-bearing secondary with `priority: 0` (or `0.5`) and
    `votes: 1`, so it never wins an election while Caltech is healthy
    but stays a full copy.
  - Optional third location (tiny VM or arbiter) as tiebreaker. With only
    two datacenters, losing Caltech leaves 1 of 3 votes at UMN, which is
    read-only until an operator reconfigures. Plan for that
    reconfiguration rather than pretending automatic promotion works with
    two sites. MongoDB's own geographically-distributed replica set docs
    call this out explicitly.
- Join protocol (what "spin up and join" means concretely):
  1. UMN's `mongod` starts with the same `replication.replSetName`, a
     shared keyfile (`security.keyFile`), TLS, and a stable DNS hostname.
     It starts empty or with a recent filesystem snapshot to shorten the
     first sync.
  2. A join helper (one-shot container or entrypoint script, same pattern
     as `kafka-acl-init`) authenticates to the Caltech primary and runs
     `rs.add({ host: "<umn-host>:27017", priority: 0, tags: { dc: "umn" } })`.
     The operation is idempotent: if the member already exists it is a
     no-op, so restarts are safe.
  3. The UMN member enters `STARTUP2` and performs an initial sync over
     the WAN (full copy of alerts, objects, catalogs). After that it
     tails the oplog continuously.
  4. Both sites' BOOM configs set `database.replica_set` to the set name
     and list seed hosts spanning both sites, so either site's API can
     discover the current primary.
- Configuration prerequisites (repo changes):
  - Set `replication.replSetName` identically on all `mongod`s, add the
    keyfile, open `27017` between sites over TLS, use DNS hostnames
    (MongoDB 5.0+ rejects IP-only configs in several paths).
  - Give members `tags: { dc: caltech }` / `{ dc: umn }` and define a
    custom write concern (`getLastErrorModes: { multiDC: { dc: 2 } }`) so
    critical writes can require acknowledgment on both sites.
  - Default write concern should stay `majority` with a deadline; use the
    stricter `multiDC` concern only where durability across sites matters
    (filter definitions, user writes), not per-alert ingestion, or
    throughput collapses on WAN latency.
  - Remove `directConnection=true` from `src/conf.rs` when `replica_set`
    is set. That flag pins the driver to one node and defeats replica-set
    discovery and failover. The driver needs the seed list plus
    `replicaSet=<name>` and should use `readPreference=primaryPreferred`
    or `secondaryPreferred` per workload.
- Expected behavior: median election under ~12 seconds with defaults;
  WAN latency stretches this. Reads from UMN's secondary are local and
  fast; writes from UMN's API still go to the Caltech primary and pay one
  WAN round trip until promotion.
- Promotion to writer (the "UMN becomes main" step): graceful case is
  `rs.stepDown()` on Caltech plus raising UMN's `priority`, and the set
  elects UMN in seconds. Caltech-down case needs a forced reconfig at
  UMN (`rs.reconfig(newConf, { force: true })` dropping the unreachable
  Caltech members), which must be run by a documented operator step, not
  automatic, to avoid split brain when the link flaps.

### 3.2 What to verify before committing

- Round trip and bandwidth between `kaboom` and the UMN cluster. The
  ingestion path does large crossmatch reads (Gaia_DR3, LSDR10, NED with
  300 arcsecond radius) plus per-alert writes. A stretched set keeps
  working at 30-60 ms RTT but oplog application and `majority` writes
  feel every millisecond.
- Database size and oplog window. Catalog collections are large; initial
  sync of a UMN member copies everything over the WAN. Size the oplog
  (e.g. 5-10% of data volume or multi-hour window) so a network blip does
  not force a full resync.
- UMN HPC constraints: can we run a persistent `mongod` with a stable
  hostname, persistent volume, and open port? If the cluster only allows
  batch jobs, a stretched member is not viable and we fall back to 3.3.
- Journaling, backups, and point-in-time recovery need to cover the set,
  not one node. Keep per-site filesystem snapshots or `mongodump`
  schedules; a replica is not a backup.

### 3.3 Fallback if a stretched set is not viable: active-passive sync

- Keep two independent replica sets (each site can still run a local
  1-node set to get change streams and transactions).
- Replicate with one of: periodic `mongodump`/`mongorestore` of the
  mutable collections, a change-streams tailer that replays writes
  Caltech to UMN, or extending the existing user-sync pattern to filters
  and config. This is simpler operationally but is always lagging, needs
  conflict rules for anything written at UMN during an outage, and never
  gives single-endpoint read-your-write consistency.
- Treat this as Phase 0 (section 7), not the end state.

## 4. Kafka: UMN joins as a replica

Two ways to honor "use Kafka's replica configuration with Caltech as main
and UMN as replica". Option A is literal native replication; option B
keeps the same join-on-boot shape with MirrorMaker 2. Recommend B, with A
documented so the tradeoff is explicit.

### 4.1 Option A (literal): UMN broker joins the Caltech cluster

- UMN starts a broker with a unique `KAFKA_BROKER_ID`, `advertised.listeners`
  pointing at its own WAN-reachable hostname, and the same cluster ID as
  Caltech. A join helper adds it to the KRaft controller quorum voters
  (or as a broker-only node against a 3-controller quorum), then topic
  replication is widened so every output topic has a replica on UMN
  (`--replication-factor 3` reassignment, `min.insync.replicas=2`,
  `unclean.leader.election.enable=false`).
- On boot the UMN broker fetches its assigned partitions from the
  Caltech leaders until its log-end offset matches; from then on it
  follows the ISR like any in-site replica.
- Cost, and why it is not recommended: today's broker runs RF=1 with a
  single combined controller. A stretched version needs RF>=2 and 3 KRaft
  controllers spread across sites, and every `acks=all` produce then waits
  for a cross-WAN ISR acknowledgment. Filter-worker output and Babamul
  streams are produce-heavy, so throughput drops and tail latency tracks
  the WAN. A WAN partition can also deprive both sites of metadata quorum
  and halt all produces. Apache Kafka's own datacenter guidance is to
  deploy a local cluster per datacenter and mirror between them instead.

### 4.2 Option B (recommended): separate clusters, UMN mirrors Caltech via MM2

Same operational shape as A (UMN spins up, connects to Caltech, catches
up, serves local reads) without synchronous WAN writes:

- Run the existing Compose broker definition at each site as independent
  clusters `caltech` and `umn` (bump each to 3 brokers + 3 controllers
  locally when hardware allows; at minimum raise RF to the broker count
  for `__consumer_offsets`, transaction state, and the output topics).
- UMN runs an MM2 Connect cluster whose source is Caltech's bootstrap
  servers. On UMN boot it connects, discovers topics, and starts
  replicating: `MirrorSourceConnector` for topic data and config, plus
  `MirrorCheckpointConnector` so consumer-group offsets are translated
  and downstream consumers can resume near where they left off.
  `MirrorHeartbeatConnector` is not needed for disaster recovery and
  writes back to the source, so leave it off.
- Topic naming: use `IdentityReplicationPolicy` so `ZTF_alerts_results`
  on Caltech is also `ZTF_alerts_results` on UMN (no `caltech.` prefix).
  The default prefix policy is for active-active aggregation; it would
  force every consumer to switch topic names on failover.
- What gets mirrored: all output and `babamul.*` topics. Upstream input
  topics from ZTF/LSST do not need mirroring because each site can
  reconsume upstream after failover; what matters is not re-emitting or
  losing our own filtered output.
- Exactly-once does not span clusters under either option. Expect
  at-least-once on failover: some alerts may be re-emitted. Downstream
  consumers already tolerate redelivery better than gaps, but document it.

### 4.3 Kafka failover mechanics (the hard part)

Applies to either option; names differ (leader election vs. stop-mirror):

- Steady state: only Caltech's filter workers produce. Under A, UMN holds
  in-sync follower replicas; under B, MM2 copies Caltech topics to UMN.
  UMN's filter workers are stopped, and UMN's external `readonly`
  consumers read the local replicas or mirrored copies.
- Promotion: under A, trigger preferred-leader election to UMN and
  (if Caltech is gone) reconfigure ISR and controller quorum so UMN can
  form majority. Under B, stop MM2, verify UMN log-end offsets, start UMN
  workers and let them produce locally. In both cases repoint the public
  Kafka hostname (`KAFKA_EXTERNAL_HOST`, Traefik TCP router on `:9093`)
  to UMN. Consumers that use checkpoint-translated offsets resume; others
  replay from earliest or latest per their config.
- Fencing: never run both sites' filter workers against the same logical
  output at once, or downstream sees duplicates from two primaries. The
  deploy workflow or a small promotion script should stop one side before
  starting the other.
- Upstream consumers need the same fencing. Only the active site runs
  `kafka_consumer` against ZTF/LSST/WINTER. Both sites holding the same
  upstream group ID would split partitions; different group IDs would
  double-ingest into the same MongoDB set. So: one active consumer group,
  owned by the primary site, moved on failover.

## 5. API and frontend: replicas behind one load balancer

This is the easy layer because both are stateless. UMN's `api` and
`frontend` containers are configured as replicas of Caltech's: same image
tag, same JWT and OAuth expectations, but pointed at the local MongoDB
secondary and local Kafka replicas, and registered in one load-balancer
pool.

- Join protocol: UMN's API starts with the replica-set URI (section 3) so
  it finds the Caltech primary for writes and the local secondary for
  reads. Its `/` health endpoint already exists for Compose healthchecks;
  expose the same endpoint to the load balancer. The frontend needs no
  registration beyond being reachable; it proxies `/api/` to the public
  API origin baked in at build time.
- Single endpoint options, in increasing complexity:
  1. DNS failover (recommended first step): one hostname
     (`api.kaboom.caltech.edu` or a neutral name), health-checked A/AAAA
     records with low TTL (60-300 s), provided by Route 53, Cloudflare,
     or equivalent. UMN joins the record set on boot and is only returned
     while healthy. Dead-site detection is minutes, client caching adds
     more. Cheap and sufficient for our RTO.
  2. Global load balancer / reverse proxy in front of both Traefiks
     (Cloudflare Load Balancing, HAProxy, or a small VPS running Traefik
     or Caddy with active health checks against `/`). UMN registers on
     boot, gets weighted to zero or backup while Caltech is primary, and
     takes full traffic on promotion. Gives faster failover and
     active-active reads, at the cost of another component and another TLS
     termination point. This is the "load balancer" from the original
     idea, and it can front HTTP (API, frontend) and TCP (Kafka `:9093`)
     alike.
  3. Anycast / BGP. Overkill for two sites; skip.
- Requirements all options share:
  - Identical `BOOM_API__AUTH__SECRET_KEY` at both sites or JWTs minted
    at one site fail at the other. Same for the admin bootstrap and
    PostHog keys if we want continuous analytics identity.
  - OAuth: each provider registration pins one redirect URI
    (`{redirect_base_url}/babamul/oauth/{provider}/callback`). Either
    register both sites' callback URLs with Google/GitHub/ORCID, or
    better, put the public hostname behind the failover endpoint and set
    both sites' `redirect_base_url` to it, so the callback URL never
    changes on failover.
  - Frontend builds bake the API origin in. Build both sites' images with
    the public API hostname (not the per-site host), so a failed-over
    browser keeps working without a rebuild. The nginx `proxy_pass`
    already honors the runtime origin via the entrypoint substitution.
  - SMTP, Slack webhooks, and Grafana data sources are per-site config;
    alerts should page whoever owns the active site.
- Valkey needs no replication: queues are ephemeral. On failover, anything
  still in Caltech's Valkey queues is stuck there. Recovery is replay from
  Kafka offsets (upstream reconsume with
  `subscription_window_days`-bounded lookback), not Valkey failover.

## 6. Cutouts, catalogs, and workers

- Cutouts: if `cutouts_storage.type` stays `mongo`, cutouts replicate with
  the set for free. If a site uses S3/rustfs, the bucket must replicate
  too (bucket replication, `rclone sync`, or both sites pointed at one
  shared bucket). A stretched MongoDB plus an unreplicated cutout bucket
  gives alerts without images after failover.
- Catalogs: with a stretched set, ingest catalogs once at the primary;
  they replicate. Keep the `crossmatch.<survey>` map identical across
  `config/prod/caltech/` and `config/prod/umn/` so both sites plan the
  same queries (declaration order matters for `$unionWith`). With the
  fallback sync model, catalog ingest must run per site and drift is
  expected.
- Workers and schedulers: run only on the active site. GPU differences
  already exist (`device_ids: [0]` vs `[0, 1]`, different batch sizes);
  keep those per-site tunings in `overrides.yaml` and do not try to
  converge them. The promotion runbook starts UMN's
  `consumer-*`/`scheduler-*` services and stops Caltech's.

## 7. Suggested phasing

- Phase 0 (no architecture change): document the manual failover that
  exists today, automate offsite `mongodump` plus Kafka topic snapshots,
  extend the user sync to filters, and rehearse a UMN promotion. This
  bounds data loss even before replication exists.
- Phase 1 (this spike's recommendation): stretched 3-member MongoDB set
  (2 Caltech + 1 UMN, priority-weighted, `directConnection` removed) with
  a UMN join helper (`rs.add` on boot), per-site Kafka clusters with
  UMN-side MM2 and identity replication (native stretched Kafka evaluated
  and deferred), load balancer plus DNS failover for
  API/frontend/Kafka-external, single public OAuth callback via the public
  hostname, workers fenced to the active site.
- Phase 2 (later): faster or automatic failover (shorter TTLs or a global
  LB, MongoDB arbiter in a third location so UMN auto-promotes, Kafka
  consumer-offset automation), active-active API reads with
  `secondaryPreferred` at UMN, chaos drills.

## 8. Risks and open questions

1. WAN behavior Caltech to UMN (RTT, bandwidth, stability) under
   production write load. Measure before sizing oplog, RF, and write
   concerns.
2. UMN HPC fit: persistent volumes, stable hostnames, open ports for
   MongoDB (`27017`), Kafka inter-broker and MM2 traffic, and Traefik.
   Batch-only scheduling kills the stretched-set option.
3. Two-site majority: with no third location, loss of Caltech always
   needs a manual `rs.reconfig` at UMN to regain writes. Decide who can
   run it and practice it; consider a cheap arbiter VM elsewhere.
4. Split brain: DNS plus fencing must guarantee one active producer set.
   Two primaries means duplicate Kafka output and divergent MongoDB
   writes that do not heal on their own.
5. Secret and config parity: JWT secret, Kafka SASL passwords, OAuth
   client pairs, and SMTP/PostHog settings must match or failover
   visibly breaks auth and notifications. Keep the checklist in
   `docs/deployment.md` as the source of truth.
6. Testing: failover is only real if rehearsed. Minimum drill: kill
   Caltech Compose, reconfigure UMN MongoDB to primary, stop MM2, start
   UMN pipeline, flip DNS, verify API, frontend, and Kafka reads, then
   fail back and measure lag and duplicates.

## 9. References

- MongoDB: replica sets distributed across two or more data centers,
  geographically redundant self-managed replica set tutorial, and the
  multi-data-center deployment white paper (active/standby with lowered
  `priority` on the DR member, custom `getLastErrorModes` write
  concerns).
- Apache Kafka: `operations/datacenters` (local cluster per datacenter,
  mirror between them), KIP-382 MirrorMaker 2 design, and the Red Hat
  MM2 disaster-recovery guide (`MirrorSourceConnector` plus
  `MirrorCheckpointConnector`, skip the heartbeat connector for DR,
  `IdentityReplicationPolicy` when failover must preserve topic names).
- Current repo state: `docs/deployment.md` (instances table, Traefik and
  deploy workflow), `docs/kafka-auth.md` (listeners and ACLs),
  `docs/alert-processing.md` (pipeline and queues), `src/conf.rs`
  (`directConnection=true`, `replica_set` plumbing), `docker-compose.yaml`
  (single broker, RF=1), `frontend/Dockerfile` and
  `frontend/config/nginx.conf` (baked-in API origin with runtime
  substitution).
