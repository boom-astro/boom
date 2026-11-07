# BOOM

BOOM is an astronomical alert broker written in Rust. It consumes alert streams
from ZTF, LSST, DECam and WINTER over Kafka, enriches each alert with archival
cross-matches and machine-learning classifications, runs user-defined filters
against it, and serves the results through the Babamul API and web interface.

These pages describe how a BOOM system is deployed and how each stage of the
pipeline works. The code lives at
[boom-astro/boom](https://github.com/boom-astro/boom); the README there covers
building and running it locally.

## Running a system

[Deployment](deployment.md) covers the compose stack, per-site configuration and
the images each service runs. [GPU and ONNX Runtime](gpu.md) covers hardware
acceleration for the classifiers, [Kafka authentication](kafka-auth.md) the SASL
and OAuth settings the consumers need, and [Observability](observability.md) the
tracing, metrics and dashboards.

## Alert pipeline

[Alert processing](alert-processing.md) follows an alert from the consumer
through enrichment and filtering. [Data model](data-model.md) describes the
MongoDB collections and what each document holds. [Task system](task-system.md)
covers the scheduled and on-demand jobs, and [Archival catalogs](catalogs.md)
how reference catalogs are ingested and cross-matched.

## Classification and fits

[AppleCiDER](applecider.md) describes the fusion classifier,
[Villar light-curve fits](villar-fit.md) the parametric light-curve model, and
[Milvus vector database](milvus.md) the embedding store used for similarity
search.

## Babamul

[Babamul](babamul.md) is the API and web interface served from a BOOM system;
[Usage analytics](analytics.md) describes how its usage is recorded and
reported.
