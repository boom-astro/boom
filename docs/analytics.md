# Babamul usage analytics

How we measure who uses Babamul and how much of the stream they consume.

Two systems, deliberately split by the question they answer:

| Question | System | Why there |
| --- | --- | --- |
| Which *people* use Babamul, from what, how often, do they come back? | PostHog | Product analytics — persons, retention, funnels. |
| How much is being consumed *right now*, by which group, on which topic, with what lag? | Grafana | Ops view — live, per-topic, high resolution, alertable. |

Both are fed by the same sampling code in the API service, keyed on the same
Babamul user id, so you can pivot from a Grafana series to a PostHog person.

## The privacy stance

**The Babamul Python package contains no analytics SDK.** It runs on users'
own machines and we don't collect anything from there. Everything below is
measured server-side, from requests the user was already making to us and from
Kafka state we already hold.

The one thing the package contributes is a `User-Agent` describing the
*software*, not the user:

```
babamul-python/0.2.0 (Python/3.12.1; Linux)
```

Package version, Python version, OS name. No hostname, username, machine id,
architecture, OS version, locale, or timezone — see `get_user_agent` in
[`babamul/api.py`](https://github.com/boom-astro/babamul) and the tests that
pin it. The server never stores the raw string; `parse_user_agent` in
[`src/api/observability.rs`](../src/api/observability.rs) reduces unrecognized
agents to a bucket (`browser`, `httpx`, `requests`, `curl`, `other`) so an
unusual `User-Agent` can't become a fingerprint.

## Identity

`distinct_id` is the Babamul user `_id` — the same value the web app passes to
`posthog.identify` (`frontend/src/pages/Login.tsx`). Web activity, API calls
and Kafka consumption therefore merge into **one** PostHog person instead of
three.

Requests that aren't authenticated (signup, activation, the public stats
endpoints) are reported against a fixed `babamul-anonymous` id and carry
`$process_person_profile: false`, so they're counted without creating person
profiles.

## PostHog events

### `babamul_api_request`

One per request to any `/babamul/*` endpoint.

| Property | Notes |
| --- | --- |
| `endpoint` | Registered route *pattern* (e.g. `/babamul/surveys/{survey}/objects/{object_id}`), not the raw path — so object ids don't explode the property's value space. |
| `method`, `status_code`, `success`, `duration_ms` | |
| `authenticated` | Whether the request resolved to a user. |
| `auth_method` | `personal_access_token` (what the package uses), `jwt` (what the web app uses), or `none`. The cleanest programmatic-vs-browser signal, and it works even for clients that send no useful `User-Agent`. |
| `client` | `babamul-python`, `browser`, `httpx`, `requests`, `curl`, `other`, `unknown`. |
| `client_version`, `python_version`, `client_os` | Only present for the official package. |

**"How many people use the package?"** — unique users on
`babamul_api_request` where `client = babamul-python`.

**"Is anyone still on an old version?"** — break down by `client_version`. This
is what tells you when it's safe to drop support for a release.

### `babamul_stream_consumed`

One per user per sampling cycle (default 5 min), emitted **only** when that
user actually consumed messages in that cycle — so the event count is itself a
measure of active streaming.

| Property | Notes |
| --- | --- |
| `messages_consumed` | Delta since the previous cycle, summed across the user's groups and topics. Sum this in PostHog to get per-day or per-week consumption. |
| `lag` | Messages retained but not yet consumed, at sample time. |
| `topics`, `n_topics` | Which parts of the stream they read. |
| `credential_names`, `n_credentials` | The user's own names for their Kafka credentials. |
| `interval_seconds` | Sampling interval, so deltas stay interpretable if it's ever changed. |

Two person properties are also set: `babamul_last_streamed_at` and
`babamul_is_stream_consumer`, so "who is actively streaming" is answerable
without querying the event log.

## Grafana

The **Babamul Usage** dashboard
([`config/grafana/dashboards/babamul-usage.json`](../config/grafana/dashboards/babamul-usage.json))
covers stream consumption per user and per topic, Babamul API request rate
split by client, and the health of the analytics pipeline itself.

Metrics, all emitted by the API service:

| Metric | Type | Attributes |
| --- | --- | --- |
| `babamul_kafka_consumer_committed_offset` | gauge | `user_id`, `credential_id`, `group`, `topic` |
| `babamul_kafka_consumer_lag` | gauge | same |
| `babamul_kafka_consumer_consumed_fraction` | gauge | same |
| `babamul_kafka_active_consumer_groups` | gauge | — |
| `api_request_total` | counter | `api`, `method`, `status_code`, `client` |
| `api_analytics_event_sent_total` | counter | — |
| `api_analytics_event_dropped_total` | counter | `reason` |

## How Kafka consumption is attributed to users

Each Kafka credential gets a generated SCRAM username of
`babamul-{credential_id}`, and the Python package derives its consumer group
from that username (`{username}-{suffix}`). So the group name carries the
credential id, and the credential id lives on the user document — which is what
turns an anonymous-looking consumer group back into "this user consumed this
much".

Every cycle, [`src/api/consumption.rs`](../src/api/consumption.rs):

1. loads every user's Kafka credentials into a username → user map;
2. reads watermarks for all `babamul.*` partitions;
3. lists consumer groups and fetches each one's committed offsets;
4. emits the gauges above, and a per-user PostHog delta.

Offsets are fetched with a consumer that sets `group.id` but never subscribes.
That makes it a plain `OffsetFetch` — it cannot join the group, and cannot
trigger a rebalance for the user's real consumers.

### Caveats worth knowing

- **Deltas are in-memory.** A restart re-establishes the baseline and skips one
  cycle's delta per group. This under-counts slightly; it can never
  double-count.
- **Offset resets contribute zero,** not a negative or a huge number, so a user
  seeking back to `earliest` won't corrupt the totals.
- **`unattributed`.** The Kafka ACL only constrains groups to the `babamul-`
  prefix, so a user *can* pick a group id that maps to no known credential.
  Those land in an `unattributed` bucket rather than being dropped, and the
  dashboard has a stat panel for them. It should normally be 0.
- **Committed offsets, not delivered messages.** A consumer that reads without
  committing (`enable.auto.commit=false` and never calling `commit`) is
  invisible. The package commits by default.

## Configuration

Analytics are off unless a project key is set — no key, no capture, and the
client becomes a no-op that costs nothing.

```yaml
posthog:
  project_api_key: "" # BOOM_POSTHOG__PROJECT_API_KEY; empty disables analytics
  host: "https://us.i.posthog.com" # BOOM_POSTHOG__HOST
  flush_interval_seconds: 10
  queue_capacity: 10000
  consumption_interval_seconds: 300
```

Use the **same PostHog project** as the web app's `VITE_PUBLIC_POSTHOG_KEY`,
otherwise web and API activity land on different persons and the identity
merging described above doesn't happen.

Capture never blocks a request. Events go onto a bounded queue drained by a
background task; if PostHog is slow or down, events are dropped and counted
(`api_analytics_event_dropped_total`) rather than awaited. Analytics must not
be able to add latency to, or fail, a user's API call — so if the numbers ever
look low, check that counter before believing them.
