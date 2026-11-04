-- Fixed-width UTC buckets from current, deduplicated normalized columns.
-- No payload parsing and no incremental sums over overlapping packet histories.
SELECT kind, band, programid,
       intDiv(toUnixTimestamp(ts), {cadence_seconds:UInt32}) * {cadence_seconds:UInt32} AS bucket_start_unix,
       count() AS n_records,
       countIf(valid_measurement) AS n_measured,
       if(n_measured > 0, sumIf(point_flux / (point_error * point_error), valid_measurement)
          / sumIf(1 / (point_error * point_error), valid_measurement), NULL) AS flux,
       if(n_measured > 0, 1 / sqrt(sumIf(1 / (point_error * point_error), valid_measurement)), NULL) AS flux_err,
       max(revision) AS revision
FROM
(
    SELECT kind, band, programid, ts, revision, flux AS point_flux, flux_err AS point_error,
           kind != 'upper_limit' AND isNotNull(flux) AND isNotNull(flux_err) AS valid_measurement
    FROM boom_photometry FINAL WHERE object_id = {object_id:String}
)
GROUP BY kind, band, programid, bucket_start_unix
ORDER BY bucket_start_unix, band, kind, programid
FORMAT JSONEachRow
