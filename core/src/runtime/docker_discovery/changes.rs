use super::{
    Discovery,
    models::{Cursor, Error, Observation, Watch},
};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::Instant;

const MAX_GENERATION: u64 = 9_007_199_254_740_991;
pub(super) const WATCH_TIMEOUT: Duration = Duration::from_secs(25);

pub(super) fn new_cursor() -> Option<Cursor> {
    let mut bytes = [0_u8; 16];
    aws_lc_rs::rand::fill(&mut bytes).ok()?;
    Some(Cursor {
        runtime_id: bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
        generation: 0,
    })
}

impl Discovery {
    // Called only while holding snapshot: every semantic change is paired with one cursor.
    pub(super) fn advance_generation(&self) {
        self.changes.send_modify(|cursor| match cursor {
            Some(value) if value.generation < MAX_GENERATION => value.generation += 1,
            _ => *cursor = new_cursor(),
        });
    }

    /// Returns a complete snapshot on change/reset, or a cursor-only heartbeat after 25 seconds.
    /// Cursors are runtime-local hints; clients resync without a cursor after restarting.
    pub(crate) async fn watch(&self, request: Watch) -> Result<Observation, Error> {
        self.watch_until(request, Instant::now() + WATCH_TIMEOUT)
            .await
    }

    pub(super) async fn watch_until(
        &self,
        request: Watch,
        deadline: Instant,
    ) -> Result<Observation, Error> {
        if request.runtime_id.is_some() != request.generation.is_some()
            || request.runtime_id.as_ref().is_some_and(|id| {
                id.len() != 32
                    || !id
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
            || request
                .generation
                .is_some_and(|generation| generation > MAX_GENERATION)
        {
            return Err(Error::InvalidInput);
        }
        let _permit = self
            .subscribers
            .try_acquire()
            .map_err(|_| Error::EndpointUnavailable)?;
        // Subscribe before reading, including for changes published while persistent state is read.
        let mut changes = self.changes.subscribe();
        loop {
            if self.stopping.load(Ordering::SeqCst) {
                return Err(Error::Stopping);
            }
            let (snapshot, cursor) = self.observation().await?;
            let cursor = cursor.ok_or(Error::StoreUnavailable)?;
            if request.runtime_id.as_deref() != Some(&cursor.runtime_id)
                || request.generation != Some(cursor.generation)
            {
                return Ok(Observation {
                    cursor,
                    snapshot: Some(snapshot),
                });
            }
            if tokio::time::timeout_at(deadline, changes.changed())
                .await
                .is_err()
            {
                // Recheck once at the deadline; a notification racing timeout cannot be lost.
                if self.stopping.load(Ordering::SeqCst) {
                    return Err(Error::Stopping);
                }
                let (snapshot, current) = self.observation().await?;
                let current = current.ok_or(Error::StoreUnavailable)?;
                return Ok(Observation {
                    snapshot: (current != cursor).then_some(snapshot),
                    cursor: current,
                });
            }
        }
    }
}
