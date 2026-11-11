use crate::{Claim, Error, now_ms};
use rusqlite::{Connection, params};
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    path::Path,
    time::Duration,
};

/// One bounded raw-result store shared with the experimental Bun replay adapter.
/// FULL WAL commits fsync starts before execution; interrupted starts remain observable.
pub struct Outbox {
    connection: Connection,
    store_id: String,
    epoch: String,
    _owner: File,
}

impl Outbox {
    /// Opens the fixture store with one native owner, FULL WAL commits and a page limit.
    /// Existing store/epoch metadata must match; this does not reset or discard old evidence.
    pub fn open(path: &Path, store_id: &str, epoch: &str) -> Result<Self, Error> {
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("owner"))?;
        owner.try_lock()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _created = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            // The experiment scheduler UID is distinct from web; only its shared replay group
            // can read/write this raw store. SQLite inherits this mode for WAL/SHM files.
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(2))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=64; PRAGMA journal_size_limit=1048576;
             PRAGMA max_page_count=8192;
             CREATE TABLE IF NOT EXISTS b2_identity (id INTEGER PRIMARY KEY CHECK(id=1), store_id TEXT NOT NULL, epoch TEXT NOT NULL, discarded INTEGER NOT NULL DEFAULT 0, acked_sequence INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE IF NOT EXISTS b2_outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL UNIQUE, store_id TEXT NOT NULL, claim TEXT NOT NULL, started_ms INTEGER NOT NULL, admitted_ms INTEGER, durable_ms INTEGER, completed_ms INTEGER, result TEXT);
             CREATE TABLE IF NOT EXISTS b2_status (id INTEGER PRIMARY KEY CHECK(id=1), pressure INTEGER NOT NULL DEFAULT 0, interrupted INTEGER NOT NULL DEFAULT 0, gaps INTEGER NOT NULL DEFAULT 0, resync_required INTEGER NOT NULL DEFAULT 0);
             INSERT OR IGNORE INTO b2_status(id) VALUES (1);",
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO b2_identity(id,store_id,epoch) VALUES(1,?1,?2)",
            params![store_id, epoch],
        )?;
        let identity: (String, String) = connection.query_row(
            "SELECT store_id,epoch FROM b2_identity WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if identity != (store_id.into(), epoch.into()) {
            return Err("B2 store identity changed; explicit restore reset required".into());
        }
        Ok(Self {
            connection,
            store_id: store_id.into(),
            epoch: epoch.into(),
            _owner: owner,
        })
    }

    /// Returns remaining record slots, closing admission on a sequence gap or aged backlog.
    /// Reports identity changes as errors and updates pressure without deleting evidence.
    pub fn available(&self, now: i64) -> Result<usize, Error> {
        self.check_identity()?;
        if !self.contiguous()? {
            return Ok(0);
        }
        let (count, oldest): (i64, Option<i64>) = self.connection.query_row(
            "SELECT count(*),min(started_ms) FROM b2_outbox",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let available = if oldest.is_some_and(|time| now.saturating_sub(time) >= 120000) {
            0
        } else {
            (4096 - count).max(0) as usize
        };
        self.connection.execute(
            "UPDATE b2_status SET pressure=?1 WHERE id=1 AND pressure<>?1",
            [i64::from(available == 0)],
        )?;
        Ok(available)
    }

    /// An allocated durable sequence cannot disappear without an exact terminal ACK.
    /// A detected gap latches admission off until a quiesced explicit store reset.
    fn contiguous(&self) -> Result<bool, Error> {
        let (acked, allocated, count, resync): (i64, i64, i64, i64) = self.connection.query_row(
            "SELECT identity.acked_sequence,coalesce((SELECT seq FROM sqlite_sequence WHERE name='b2_outbox'),0),(SELECT count(*) FROM b2_outbox WHERE sequence>identity.acked_sequence),status.resync_required FROM b2_identity identity,b2_status status WHERE identity.id=1 AND status.id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if resync != 0 {
            return Ok(false);
        }
        if acked < 0 || allocated.checked_sub(acked) != Some(count) {
            self.connection.execute(
                "UPDATE b2_status SET gaps=gaps+1,resync_required=1,pressure=1 WHERE id=1",
                [],
            )?;
            return Ok(false);
        }
        Ok(true)
    }

    fn check_identity(&self) -> Result<(), Error> {
        let valid: bool = self.connection.query_row(
            "SELECT store_id=?1 AND epoch=?2 FROM b2_identity WHERE id=1",
            params![self.store_id, self.epoch],
            |row| row.get(0),
        )?;
        if !valid {
            return Err("B2 restore fence changed".into());
        }
        Ok(())
    }

    /// Commits one bounded raw-start reservation before any probe can be admitted.
    /// Rejects invalid, duplicate or mismatched claims, changed identity and store pressure.
    pub fn start(&mut self, claim: &Claim, now: i64) -> Result<(), Error> {
        claim.validate(now)?;
        if !self.contiguous()? {
            return Err("B2 durable store sequence gap requires explicit reset".into());
        }
        let payload = serde_json::to_string(claim)?;
        if claim.store_id != self.store_id
            || claim.controller_epoch != self.epoch
            || payload.len() > 2048
        {
            return Err("B2 start identity or payload rejected".into());
        }
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (store, epoch): (String, String) = tx.query_row(
            "SELECT store_id,epoch FROM b2_identity WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if store != self.store_id || epoch != self.epoch {
            return Err("B2 restore fence changed".into());
        }
        let (count, oldest): (i64, Option<i64>) =
            tx.query_row("SELECT count(*),min(started_ms) FROM b2_outbox", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        // Every admitted start reserves 4096 bytes, including its later raw result.
        if count >= 4096 || oldest.is_some_and(|time| now.saturating_sub(time) >= 120000) {
            return Err("B2 durable store pressure".into());
        }
        tx.execute(
            "INSERT INTO b2_outbox(attempt_id,store_id,claim,started_ms) VALUES(?1,?2,?3,?4)",
            params![claim.attempt_id(), self.store_id, payload, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Completes the exact durable claim once, within its payload reservation and restore fence.
    /// The stored value is raw evidence; Bun replay owns validation and incident transitions.
    pub fn finish(&mut self, claim: &Claim, result: &Value, now: i64) -> Result<(), Error> {
        let payload = serde_json::to_string(result)?;
        let original_claim = serde_json::to_string(claim)?;
        if payload.len() + original_claim.len() > 4096 {
            return Err("B2 raw record exceeds reservation".into());
        }
        let changed = self.connection.execute("UPDATE b2_outbox SET result=?1,completed_ms=?2 WHERE attempt_id=?3 AND store_id=?4 AND result IS NULL AND claim=?6 AND EXISTS(SELECT 1 FROM b2_identity WHERE id=1 AND store_id=?4 AND epoch=?5)", params![payload,now,claim.attempt_id(),self.store_id,self.epoch,original_claim])?;
        if changed != 1 {
            return Err("B2 completion identity or restore fence rejected".into());
        }
        Ok(())
    }

    /// Commits the authority admission timestamp and local recording timestamp once.
    /// Requires the original unfinished claim and current store identity; no probe runs here.
    pub fn admitted(&mut self, claim: &Claim, now: i64) -> Result<(), Error> {
        let original_claim = serde_json::to_string(claim)?;
        let changed=self.connection.execute("UPDATE b2_outbox SET admitted_ms=?1,durable_ms=?5 WHERE attempt_id=?2 AND store_id=?3 AND result IS NULL AND admitted_ms IS NULL AND claim=?6 AND EXISTS(SELECT 1 FROM b2_identity WHERE id=1 AND store_id=?3 AND epoch=?4)",params![now,claim.attempt_id(),self.store_id,self.epoch,now_ms()?,original_claim])?;
        if changed != 1 {
            return Err("B2 durable admission fence rejected".into());
        }
        Ok(())
    }

    /// Recovery never fabricates a probe outcome for a durable start interrupted by a crash.
    /// Under the current identity fence, marks unfinished local records as interrupted atomically.
    pub fn recover_interrupted(&mut self, now: i64) -> Result<usize, Error> {
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let valid: bool = tx.query_row(
            "SELECT store_id=?1 AND epoch=?2 FROM b2_identity WHERE id=1",
            params![self.store_id, self.epoch],
            |row| row.get(0),
        )?;
        if !valid {
            return Err("B2 restore fence changed".into());
        }
        let changed = tx.execute(r#"UPDATE b2_outbox SET result='{"error":"interrupted"}',completed_ms=?1 WHERE result IS NULL AND store_id=?2"#, params![now,self.store_id])?;
        tx.execute(
            "UPDATE b2_status SET interrupted=interrupted+?1 WHERE id=1",
            [changed as i64],
        )?;
        tx.commit()?;
        Ok(changed)
    }

    /// Reads at most one store's bounded completed, admitted claims for execution-slot recovery.
    /// Checks the restore fence; PostgreSQL still decides whether each completion is current.
    pub fn completed_claims(&self) -> Result<Vec<Claim>, Error> {
        self.check_identity()?;
        let mut statement=self.connection.prepare("SELECT claim FROM b2_outbox WHERE result IS NOT NULL AND admitted_ms IS NOT NULL AND store_id=?1 AND EXISTS(SELECT 1 FROM b2_identity WHERE id=1 AND store_id=?1 AND epoch=?2) ORDER BY sequence LIMIT 4096")?;
        let rows = statement.query_map(params![self.store_id, self.epoch], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }
}
