//! A Valkey list holding embeddings Milvus could not accept, so an outage
//! costs queue space rather than the GPU time to recompute them.
//!
//! Order is not load-bearing — the `jd` guard in [`super::insert`] picks the
//! winner by data — which is what lets a failed drain push rows back on the
//! tail instead of restoring their position.
//!
//! Rows use a compact binary encoding: JSON would inflate 384 floats by ~2.5×.

use redis::{aio::MultiplexedConnection, AsyncCommands, RedisError};
use tracing::{debug, warn};

use super::insert::EmbeddingRow;

/// Buffer of embeddings awaiting a healthy Milvus.
pub struct BackupQueue {
    con: MultiplexedConnection,
    key: String,
    /// Past this many rows the oldest are dropped.
    max_rows: usize,
}

impl BackupQueue {
    pub fn new(con: MultiplexedConnection, key: String, max_rows: usize) -> Self {
        Self { con, key, max_rows }
    }

    /// Buffer rows that could not be uploaded, trimming to `max_rows`.
    ///
    /// Dropping the oldest is the right way round: a later alert for the same
    /// object supersedes them anyway, so they would lose the `jd` comparison
    /// even if kept.
    pub async fn push(&mut self, rows: &[EmbeddingRow]) -> Result<(), RedisError> {
        if rows.is_empty() {
            return Ok(());
        }

        let encoded: Vec<Vec<u8>> = rows.iter().map(encode).collect();
        let _: () = self.con.rpush(&self.key, encoded).await?;

        // Negative indices count from the tail, so this keeps the newest.
        let start = -(self.max_rows as isize);
        let _: () = self.con.ltrim(&self.key, start, -1).await?;

        Ok(())
    }

    /// Remove and return up to `max` of the oldest buffered rows.
    ///
    /// Undecodable rows are dropped, so one corrupt entry cannot wedge the
    /// queue behind it.
    pub async fn take(&mut self, max: usize) -> Result<Vec<EmbeddingRow>, RedisError> {
        if max == 0 {
            return Ok(vec![]);
        }

        let raw: Vec<Vec<u8>> = self
            .con
            .lpop(&self.key, std::num::NonZero::new(max))
            .await?;

        let mut rows = Vec::with_capacity(raw.len());
        let mut undecodable = 0usize;
        for bytes in &raw {
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

    /// How many rows are waiting.
    pub async fn pending(&mut self) -> Result<usize, RedisError> {
        self.con.llen(&self.key).await
    }
}

/// Length-prefixed `object_id`, then `candid`, `jd`, and a length-prefixed
/// embedding. Little-endian throughout.
fn encode(row: &EmbeddingRow) -> Vec<u8> {
    let id = row.object_id.as_bytes();
    let mut out = Vec::with_capacity(2 + id.len() + 8 + 8 + 4 + row.embedding.len() * 4);

    out.extend_from_slice(&(id.len() as u16).to_le_bytes());
    out.extend_from_slice(id);
    out.extend_from_slice(&row.candid.to_le_bytes());
    out.extend_from_slice(&row.jd.to_le_bytes());
    out.extend_from_slice(&(row.embedding.len() as u32).to_le_bytes());
    for value in &row.embedding {
        out.extend_from_slice(&value.to_le_bytes());
    }

    out
}

/// Inverse of [`encode`]. `None` for anything malformed, so a bad entry is
/// dropped rather than panicking a worker.
fn decode(bytes: &[u8]) -> Option<EmbeddingRow> {
    let mut cursor = Cursor { bytes, at: 0 };

    let id_len = u16::from_le_bytes(cursor.take::<2>()?) as usize;
    let object_id = String::from_utf8(cursor.take_slice(id_len)?.to_vec()).ok()?;
    let candid = i64::from_le_bytes(cursor.take::<8>()?);
    let jd = f64::from_le_bytes(cursor.take::<8>()?);

    let dim = u32::from_le_bytes(cursor.take::<4>()?) as usize;
    let mut embedding = Vec::with_capacity(dim.min(4096));
    for _ in 0..dim {
        embedding.push(f32::from_le_bytes(cursor.take::<4>()?));
    }

    // Trailing bytes mean it is not what it claims to be.
    if cursor.at != bytes.len() {
        return None;
    }

    Some(EmbeddingRow {
        object_id,
        embedding,
        candid,
        jd,
    })
}

/// Bounds-checked reader, so a truncated entry yields `None` instead of a panic.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let slice = self.take_slice(N)?;
        slice.try_into().ok()
    }

    fn take_slice(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let decoded = decode(&encode(&original)).expect("must decode");

        assert_eq!(decoded.object_id, original.object_id);
        assert_eq!(decoded.candid, original.candid);
        assert_eq!(decoded.jd, original.jd);
        assert_eq!(decoded.embedding, original.embedding);
    }

    #[test]
    fn a_non_ascii_object_id_survives() {
        let decoded = decode(&encode(&row("ZTF_αβγ_✓", 4))).expect("must decode");
        assert_eq!(decoded.object_id, "ZTF_αβγ_✓");
    }

    #[test]
    fn an_empty_embedding_survives() {
        let decoded = decode(&encode(&row("ZTF_A", 0))).expect("must decode");
        assert!(decoded.embedding.is_empty());
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
        let decoded = decode(&encode(&original)).expect("must decode");

        assert!(decoded.jd.is_nan());
        assert!(decoded.embedding[0].is_nan());
        assert_eq!(decoded.embedding[1], f32::INFINITY);
        assert_eq!(decoded.embedding[2], f32::NEG_INFINITY);
        assert!(decoded.embedding[3].is_sign_negative());
    }

    /// The case that would otherwise panic a worker on a corrupt entry.
    #[test]
    fn every_truncation_is_rejected() {
        let encoded = encode(&row("ZTF_A", 8));
        for n in 0..encoded.len() {
            assert!(
                decode(&encoded[..n]).is_none(),
                "truncating to {n} bytes should not decode"
            );
        }
        assert!(decode(&encoded).is_some(), "the whole entry still decodes");
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut encoded = encode(&row("ZTF_A", 4));
        encoded.push(0);
        assert!(decode(&encoded).is_none());
    }

    #[test]
    fn an_absurd_declared_length_is_rejected() {
        let mut encoded = encode(&row("ZTF_A", 1));
        let len_at = encoded.len() - 4 - 4;
        encoded[len_at..len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(decode(&encoded).is_none());
    }

    #[test]
    fn an_empty_buffer_is_rejected() {
        assert!(decode(&[]).is_none());
    }

    /// Real Valkey, or skip: these cover LTRIM's index arithmetic and LPOP's
    /// count form, which a fake would only restate. Key is unique per run.
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

        Some(BackupQueue::new(con, key, max_rows))
    }

    async fn cleanup(queue: &mut BackupQueue) {
        let key = queue.key.clone();
        let _: Result<(), _> = queue.con.del(&key).await;
    }

    #[tokio::test]
    async fn rows_come_back_oldest_first() {
        let Some(mut q) = queue("fifo", 100).await else {
            eprintln!("skipping: no valkey on localhost:6379");
            return;
        };

        q.push(&[row("ZTF_A", 2), row("ZTF_B", 2)]).await.unwrap();
        q.push(&[row("ZTF_C", 2)]).await.unwrap();
        assert_eq!(q.pending().await.unwrap(), 3);

        let drained = q.take(10).await.unwrap();
        let ids: Vec<&str> = drained.iter().map(|r| r.object_id.as_str()).collect();
        assert_eq!(ids, vec!["ZTF_A", "ZTF_B", "ZTF_C"]);
        assert_eq!(
            q.pending().await.unwrap(),
            0,
            "a full drain empties the queue"
        );

        cleanup(&mut q).await;
    }

    /// The remainder must stay queued, not be consumed and dropped.
    #[tokio::test]
    async fn take_is_bounded_and_leaves_the_rest() {
        let Some(mut q) = queue("bounded", 100).await else {
            return;
        };

        let rows: Vec<EmbeddingRow> = (0..10).map(|i| row(&format!("ZTF_{i}"), 2)).collect();
        q.push(&rows).await.unwrap();

        let first = q.take(4).await.unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(first[0].object_id, "ZTF_0");
        assert_eq!(q.pending().await.unwrap(), 6);

        let second = q.take(4).await.unwrap();
        assert_eq!(second[0].object_id, "ZTF_4", "picks up where it left off");

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn exceeding_the_cap_drops_the_oldest() {
        let Some(mut q) = queue("cap", 5).await else {
            return;
        };

        let rows: Vec<EmbeddingRow> = (0..8).map(|i| row(&format!("ZTF_{i}"), 2)).collect();
        q.push(&rows).await.unwrap();

        assert_eq!(q.pending().await.unwrap(), 5, "trimmed to the cap");
        let drained = q.take(10).await.unwrap();
        let ids: Vec<&str> = drained.iter().map(|r| r.object_id.as_str()).collect();
        assert_eq!(ids, vec!["ZTF_3", "ZTF_4", "ZTF_5", "ZTF_6", "ZTF_7"]);

        cleanup(&mut q).await;
    }

    #[tokio::test]
    async fn the_cap_holds_across_repeated_pushes() {
        let Some(mut q) = queue("cap_repeat", 3).await else {
            return;
        };

        for i in 0..6 {
            q.push(&[row(&format!("ZTF_{i}"), 2)]).await.unwrap();
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

    #[tokio::test]
    async fn a_corrupt_entry_does_not_block_the_queue() {
        let Some(mut q) = queue("corrupt", 10).await else {
            return;
        };

        q.push(&[row("ZTF_A", 2)]).await.unwrap();
        let key = q.key.clone();
        let _: () = q.con.rpush(&key, vec![vec![0xffu8, 0x01]]).await.unwrap();
        q.push(&[row("ZTF_B", 2)]).await.unwrap();

        let drained = q.take(10).await.unwrap();
        let ids: Vec<&str> = drained.iter().map(|r| r.object_id.as_str()).collect();
        assert_eq!(ids, vec!["ZTF_A", "ZTF_B"], "the good rows still come back");

        cleanup(&mut q).await;
    }
}
