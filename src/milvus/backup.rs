//! A Valkey-backed holding area for embeddings Milvus could not accept, so an
//! outage costs queue space rather than the GPU time to recompute them.
//!
//! Keyed by `object_id`, holding only each object's newest alert. One object
//! can produce several alerts a night, and buffering every one would store
//! copies that [`super::insert`] discards on the way out anyway.
//!
//! Two Valkey keys back it: a hash of `object_id -> row`, and a sorted set
//! scored by `jd`. The sorted set makes "keep the newest" a single atomic
//! `ZADD ... GT` rather than a read-compare-write, and also orders the drain
//! and picks eviction victims. A Lua script keeps the pair consistent, since
//! a failure between the two writes would otherwise strand a payload with no
//! index entry or the reverse.
//!
//! Rows are stored as protobuf: JSON would inflate 384 floats by ~3x.

use prost::Message;
use redis::{aio::MultiplexedConnection, AsyncCommands, RedisError, Script};
use tracing::{debug, warn};

use super::insert::EmbeddingRow;

/// Store each row under its `object_id`, keeping whichever has the larger
/// `jd`, then evict the lowest-scored entries once over capacity.
///
/// `ZADD GT CH` reports 1 only when it inserted or raised a score, which is
/// exactly when the payload is worth writing.
const PUSH: &str = r#"
local hash, zset = KEYS[1], KEYS[2]
local max = tonumber(ARGV[1])
local i = 2
while i <= #ARGV do
  if redis.call('ZADD', zset, 'GT', 'CH', ARGV[i + 1], ARGV[i]) == 1 then
    redis.call('HSET', hash, ARGV[i], ARGV[i + 2])
  end
  i = i + 3
end
local over = redis.call('ZCARD', zset) - max
if over > 0 then
  local stale = redis.call('ZRANGE', zset, 0, over - 1)
  redis.call('ZREMRANGEBYRANK', zset, 0, over - 1)
  for j = 1, #stale, 1000 do
    redis.call('HDEL', hash, unpack(stale, j, math.min(j + 999, #stale)))
  end
end
return redis.call('ZCARD', zset)
"#;

/// Remove and return the `n` lowest-`jd` rows.
const TAKE: &str = r#"
local hash, zset = KEYS[1], KEYS[2]
local members = redis.call('ZRANGE', zset, 0, tonumber(ARGV[1]) - 1)
if #members == 0 then return {} end
local rows = redis.call('HMGET', hash, unpack(members))
redis.call('ZREMRANGEBYRANK', zset, 0, #members - 1)
redis.call('HDEL', hash, unpack(members))
return rows
"#;

/// Buffer of embeddings awaiting a healthy Milvus.
pub struct BackupQueue {
    con: MultiplexedConnection,
    /// Hash of `object_id -> encoded row`.
    key: String,
    /// Sorted set of `object_id` scored by `jd`.
    index: String,
    /// Past this many objects the lowest-`jd` entries are dropped.
    max_rows: usize,
}

impl BackupQueue {
    pub fn new(con: MultiplexedConnection, key: String, max_rows: usize) -> Self {
        let index = format!("{key}:jd");
        Self {
            con,
            key,
            index,
            max_rows,
        }
    }

    /// Buffer rows, keeping each object's newest alert and trimming to
    /// `max_rows`.
    ///
    /// Dropping the lowest `jd` is the right way round: those are the rows a
    /// later alert for the same object supersedes, so they would lose the `jd`
    /// comparison even if kept.
    pub async fn push(&mut self, rows: &[EmbeddingRow]) -> Result<(), RedisError> {
        let script = Script::new(PUSH);
        let mut invocation = script.prepare_invoke();
        invocation
            .key(&self.key)
            .key(&self.index)
            .arg(self.max_rows);

        // A NaN jd cannot be a sorted-set score and would fail the whole
        // script, so it is dropped here rather than poisoning the batch.
        let mut queued = 0usize;
        let mut rejected = 0usize;
        for row in rows {
            if row.jd.is_nan() {
                rejected += 1;
                continue;
            }
            invocation
                .arg(row.object_id.as_str())
                .arg(row.jd.to_string())
                .arg(row.encode_to_vec());
            queued += 1;
        }
        if rejected > 0 {
            warn!(rejected, "skipped buffering embeddings with a NaN jd");
        }
        if queued == 0 {
            return Ok(());
        }

        let held: usize = invocation.invoke_async(&mut self.con).await?;
        debug!(queued, held, "buffered embeddings for a later retry");
        Ok(())
    }

    /// Remove and return up to `max` rows, oldest `jd` first.
    ///
    /// Undecodable rows are dropped, so one corrupt entry cannot wedge the
    /// queue behind it.
    pub async fn take(&mut self, max: usize) -> Result<Vec<EmbeddingRow>, RedisError> {
        if max == 0 {
            return Ok(vec![]);
        }

        let raw: Vec<Option<Vec<u8>>> = Script::new(TAKE)
            .key(&self.key)
            .key(&self.index)
            .arg(max)
            .invoke_async(&mut self.con)
            .await?;

        let mut rows = Vec::with_capacity(raw.len());
        let mut undecodable = 0usize;
        for bytes in raw.iter().flatten() {
            match decode(bytes) {
                Some(row) => rows.push(row),
                None => undecodable += 1,
            }
        }
        if undecodable > 0 {
            warn!(
                dropped = undecodable,
                "discarded malformed rows from the milvus backup queue"
            );
        }

        if !rows.is_empty() {
            debug!(rows = rows.len(), "drained milvus backup queue");
        }
        Ok(rows)
    }

    /// How many objects are waiting.
    pub async fn pending(&mut self) -> Result<usize, RedisError> {
        self.con.zcard(&self.index).await
    }
}

/// Decode a stored row. `None` for anything malformed, so a bad entry is
/// dropped rather than panicking a worker.
///
/// Protobuf fills absent fields with defaults, so an entry truncated at a
/// field boundary (or an empty one) still decodes. A row without an id or an
/// embedding is rejected here rather than sent to Milvus, where it would fail
/// the whole upsert.
fn decode(bytes: &[u8]) -> Option<EmbeddingRow> {
    let row = EmbeddingRow::decode(bytes).ok()?;
    if row.object_id.is_empty() || row.embedding.is_empty() {
        return None;
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One alert for `object_id` at `jd`, with the payload varying by `jd` so
    /// tests can tell which copy survived.
    fn at(object_id: &str, jd: f64) -> EmbeddingRow {
        EmbeddingRow {
            object_id: object_id.to_string(),
            embedding: vec![jd as f32, 1.0],
            candid: jd as i64,
            jd,
        }
    }

    fn row(object_id: &str, dim: usize) -> EmbeddingRow {
        EmbeddingRow {
            object_id: object_id.to_string(),
            embedding: (0..dim).map(|i| i as f32 * 0.5).collect(),
            candid: 1234567890123,
            jd: 2400123.75,
        }
    }

    #[test]
    fn a_row_survives_a_round_trip() {
        let original = row("ZTF18abcdefg", 384);
        let decoded = decode(&original.encode_to_vec()).expect("must decode");

        assert_eq!(decoded.object_id, original.object_id);
        assert_eq!(decoded.candid, original.candid);
        assert_eq!(decoded.jd, original.jd);
        assert_eq!(decoded.embedding, original.embedding);
    }

    #[test]
    fn a_non_ascii_object_id_survives() {
        let decoded = decode(&row("ZTF_αβγ_✓", 4).encode_to_vec()).expect("must decode");
        assert_eq!(decoded.object_id, "ZTF_αβγ_✓");
    }

    /// Indistinguishable from an entry that lost its embedding.
    #[test]
    fn an_empty_embedding_is_rejected() {
        assert!(decode(&row("ZTF_A", 0).encode_to_vec()).is_none());
    }

    #[test]
    fn an_empty_object_id_is_rejected() {
        assert!(decode(&row("", 4).encode_to_vec()).is_none());
    }

    /// `jd` is compared with `total_cmp` downstream, so the bits matter.
    #[test]
    fn non_finite_floats_round_trip_bitwise() {
        let original = EmbeddingRow {
            object_id: "ZTF_A".into(),
            embedding: vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0],
            candid: -1,
            jd: f64::NAN,
        };
        let decoded = decode(&original.encode_to_vec()).expect("must decode");

        assert!(decoded.jd.is_nan());
        assert!(decoded.embedding[0].is_nan());
        assert_eq!(decoded.embedding[1], f32::INFINITY);
        assert_eq!(decoded.embedding[2], f32::NEG_INFINITY);
        assert!(decoded.embedding[3].is_sign_negative());
    }

    /// The case that would otherwise panic a worker on a corrupt entry.
    #[test]
    fn every_truncation_is_rejected() {
        let encoded = row("ZTF_A", 8).encode_to_vec();
        for n in 0..encoded.len() {
            assert!(
                decode(&encoded[..n]).is_none(),
                "truncating to {n} bytes should not decode"
            );
        }
        assert!(
            decode(&encoded).is_some(),
            "the whole entry should still decode"
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut encoded = row("ZTF_A", 4).encode_to_vec();
        encoded.push(0);
        assert!(decode(&encoded).is_none());
    }

    #[test]
    fn an_absurd_declared_length_is_rejected() {
        // The embedding is encoded last: its one-byte length, then 4 bytes.
        let mut encoded = row("ZTF_A", 1).encode_to_vec();
        let len_at = encoded.len() - 4 - 1;
        assert_eq!(encoded[len_at], 4);
        encoded[len_at] = 0x7f;

        assert!(decode(&encoded).is_none());
    }

    #[test]
    fn an_empty_buffer_is_rejected() {
        assert!(decode(&[]).is_none());
    }

    /// Real Valkey, or skip: these cover the Lua script's `ZADD GT` semantics
    /// and eviction arithmetic, which a fake would only restate. Keys are
    /// unique per run.
    async fn queue(name: &str, max_rows: usize) -> Option<BackupQueue> {
        let client = redis::Client::open("redis://localhost:6379/").ok()?;
        let mut con = client.get_multiplexed_async_connection().await.ok()?;

        let key = format!(
            "test_milvus_backup_{name}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let _: () = con.del(&key).await.ok()?;
        let _: () = con.del(format!("{key}:jd")).await.ok()?;

        Some(BackupQueue::new(con, key, max_rows))
    }

    async fn cleanup(q: &mut BackupQueue) {
        let (key, index) = (q.key.clone(), q.index.clone());
        let _: Result<(), _> = q.con.del(&key).await;
        let _: Result<(), _> = q.con.del(&index).await;
    }

    /// The point of keying by object: ten alerts for one object occupy one
    /// slot, not ten.
    #[tokio::test]
    async fn repeat_alerts_for_one_object_collapse_to_one_row() {
        let Some(mut q) = queue("collapse", 100).await else {
            eprintln!("skipping: no valkey on localhost:6379");
            return;
        };

        for i in 0..10 {
            q.push(&[at("ZTF_A", 2400000.0 + i as f64)]).await.unwrap();
        }
        assert_eq!(q.pending().await.unwrap(), 1);

        let drained = q.take(10).await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained[0].jd, 2400009.0,
            "the newest alert should be the survivor"
        );

        cleanup(&mut q).await;
    }

    /// An older alert arriving after a newer one must not replace it — the
    /// same rule the Milvus upsert applies, enforced here by `ZADD GT`.
    #[tokio::test]
    async fn an_older_alert_does_not_replace_a_newer_one() {
        let Some(mut q) = queue("older", 100).await else {
            return;
        };

        q.push(&[at("ZTF_A", 2400009.0)]).await.unwrap();
        q.push(&[at("ZTF_A", 2400001.0)]).await.unwrap();

        let drained = q.take(10).await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].jd, 2400009.0);

        cleanup(&mut q).await;
    }

    /// Two alerts for one object inside a single push must resolve the same
    /// way regardless of their order in the batch.
    #[tokio::test]
    async fn order_within_one_push_does_not_matter() {
        let Some(mut q) = queue("within", 100).await else {
            return;
        };
        q.push(&[at("ZTF_A", 2400009.0), at("ZTF_A", 2400001.0)])
            .await
            .unwrap();
        assert_eq!(q.take(10).await.unwrap()[0].jd, 2400009.0);
        cleanup(&mut q).await;

        let Some(mut r) = queue("within_rev", 100).await else {
            return;
        };
        r.push(&[at("ZTF_B", 2400001.0), at("ZTF_B", 2400009.0)])
            .await
            .unwrap();
        assert_eq!(r.take(10).await.unwrap()[0].jd, 2400009.0);
        cleanup(&mut r).await;
    }

    #[tokio::test]
    async fn distinct_objects_each_keep_a_slot() {
        let Some(mut q) = queue("distinct", 100).await else {
            return;
        };

        q.push(&[at("ZTF_A", 2400001.0), at("ZTF_B", 2400002.0)])
            .await
            .unwrap();
        q.push(&[at("ZTF_C", 2400003.0)]).await.unwrap();
        assert_eq!(q.pending().await.unwrap(), 3);

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn rows_drain_oldest_jd_first() {
        let Some(mut q) = queue("order", 100).await else {
            return;
        };

        q.push(&[
            at("ZTF_C", 2400003.0),
            at("ZTF_A", 2400001.0),
            at("ZTF_B", 2400002.0),
        ])
        .await
        .unwrap();

        let ids: Vec<String> = q
            .take(10)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.object_id)
            .collect();
        assert_eq!(ids, vec!["ZTF_A", "ZTF_B", "ZTF_C"]);
        assert_eq!(q.pending().await.unwrap(), 0);

        cleanup(&mut q).await;
    }

    /// The remainder must stay queued, not be consumed and dropped.
    #[tokio::test]
    async fn take_is_bounded_and_leaves_the_rest() {
        let Some(mut q) = queue("bounded", 100).await else {
            return;
        };

        let rows: Vec<EmbeddingRow> = (0..10)
            .map(|i| at(&format!("ZTF_{i}"), 2400000.0 + i as f64))
            .collect();
        q.push(&rows).await.unwrap();

        let first = q.take(4).await.unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(first[0].object_id, "ZTF_0");
        assert_eq!(q.pending().await.unwrap(), 6);

        let second = q.take(4).await.unwrap();
        assert_eq!(
            second[0].object_id, "ZTF_4",
            "should pick up where it left off"
        );

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn exceeding_the_cap_evicts_the_lowest_jd() {
        let Some(mut q) = queue("cap", 5).await else {
            return;
        };

        let rows: Vec<EmbeddingRow> = (0..8)
            .map(|i| at(&format!("ZTF_{i}"), 2400000.0 + i as f64))
            .collect();
        q.push(&rows).await.unwrap();

        assert_eq!(q.pending().await.unwrap(), 5);
        let ids: Vec<String> = q
            .take(10)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.object_id)
            .collect();
        assert_eq!(ids, vec!["ZTF_3", "ZTF_4", "ZTF_5", "ZTF_6", "ZTF_7"]);

        cleanup(&mut q).await;
    }

    /// Eviction must drop the payload too, or the hash grows unboundedly
    /// while the index stays capped.
    #[tokio::test]
    async fn eviction_removes_the_payload_as_well() {
        let Some(mut q) = queue("evict_payload", 2).await else {
            return;
        };

        let rows: Vec<EmbeddingRow> = (0..6)
            .map(|i| at(&format!("ZTF_{i}"), 2400000.0 + i as f64))
            .collect();
        q.push(&rows).await.unwrap();

        let key = q.key.clone();
        let stored: usize = q.con.hlen(&key).await.unwrap();
        assert_eq!(stored, 2, "hash and index must stay the same size");

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn the_cap_holds_across_repeated_pushes() {
        let Some(mut q) = queue("cap_repeat", 3).await else {
            return;
        };

        for i in 0..6 {
            q.push(&[at(&format!("ZTF_{i}"), 2400000.0 + i as f64)])
                .await
                .unwrap();
        }

        assert_eq!(q.pending().await.unwrap(), 3);
        let ids: Vec<String> = q
            .take(10)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.object_id)
            .collect();
        assert_eq!(ids, vec!["ZTF_3", "ZTF_4", "ZTF_5"]);

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn draining_an_empty_queue_is_not_an_error() {
        let Some(mut q) = queue("empty", 10).await else {
            return;
        };

        assert!(q.take(100).await.unwrap().is_empty());
        assert_eq!(q.pending().await.unwrap(), 0);

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn pushing_no_rows_is_a_no_op() {
        let Some(mut q) = queue("noop", 10).await else {
            return;
        };

        q.push(&[]).await.unwrap();
        assert_eq!(q.pending().await.unwrap(), 0);

        cleanup(&mut q).await;
    }

    /// A NaN jd cannot be a sorted-set score. It must be skipped rather than
    /// failing the script and losing the whole batch with it.
    #[tokio::test]
    async fn a_nan_jd_is_skipped_without_losing_the_batch() {
        let Some(mut q) = queue("nan", 10).await else {
            return;
        };

        let mut bad = at("ZTF_BAD", 2400001.0);
        bad.jd = f64::NAN;
        q.push(&[bad, at("ZTF_GOOD", 2400002.0)]).await.unwrap();

        let drained = q.take(10).await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].object_id, "ZTF_GOOD");

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn a_corrupt_entry_does_not_block_the_queue() {
        let Some(mut q) = queue("corrupt", 10).await else {
            return;
        };

        q.push(&[at("ZTF_A", 2400001.0), at("ZTF_B", 2400003.0)])
            .await
            .unwrap();

        // Overwrite one payload with garbage, leaving its index entry intact.
        let key = q.key.clone();
        let _: () = q.con.hset(&key, "ZTF_A", vec![0xffu8, 0x01]).await.unwrap();

        let drained = q.take(10).await.unwrap();
        let ids: Vec<&str> = drained.iter().map(|r| r.object_id.as_str()).collect();
        assert_eq!(ids, vec!["ZTF_B"], "the good row should still come back");
        assert_eq!(q.pending().await.unwrap(), 0, "both should be consumed");

        cleanup(&mut q).await;
    }
}
