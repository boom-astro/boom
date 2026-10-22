-- Separate from the synthetic benchmark tables. Apply only in a demo database.
CREATE TABLE IF NOT EXISTS boom_photometry
(
    object_id String,
    kind Enum8('detection' = 1, 'upper_limit' = 2, 'forced' = 3),
    band LowCardinality(String),
    jd Float64 CODEC(DoubleDelta, ZSTD(1)),
    programid Int32,
    revision UInt64,
    flux Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    flux_err Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    -- Exact normalized BOOM record, including fields used by existing filters.
    payload String CODEC(ZSTD(3)),
    ts DateTime('UTC') MATERIALIZED toDateTime((jd - 2440587.5) * 86400, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    CONSTRAINT valid_jd CHECK isFinite(jd) AND jd >= 2440587.5 AND jd < 2440587.5 + 4294967296.0 / 86400,
    CONSTRAINT valid_flux CHECK isNull(flux) OR isFinite(flux),
    CONSTRAINT valid_error CHECK isNull(flux_err) OR (isFinite(flux_err) AND flux_err > 0),
    CONSTRAINT measured_error CHECK isNull(flux) OR isNotNull(flux_err),
    CONSTRAINT upper_limit_flux CHECK kind != 'upper_limit' OR isNull(flux)
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(ts)
ORDER BY (object_id, kind, band, jd, programid)
SETTINGS non_replicated_deduplication_window = 1000;
