mod outbox;

pub use outbox::Outbox;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// No destinations or secrets cross this boundary: native execution resolves a configured host.
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claim {
    pub store_id: String,
    pub controller_epoch: String,
    pub generation: i64,
    pub policy_id: String,
    pub policy_version: String,
    pub lease_token: String,
    pub lease_expires_at_ms: i64,
    pub due_at_ms: i64,
    pub host_id: String,
    pub domain: String,
    pub timeout_ms: u64,
    pub before: Value,
}

impl Claim {
    pub fn attempt_id(&self) -> &str {
        &self.lease_token
    }

    /// Checks fixture claim shape, payload identifiers and the bounded reservation window.
    /// Live policy authority is checked separately through PostgreSQL before HTTP admission.
    pub fn validate(&self, now: i64) -> Result<(), Error> {
        let revision = self.before.get("revision").and_then(Value::as_str);
        let proof = self.before.get("proof").and_then(Value::as_str);
        let hash = |value: Option<&str>| {
            value.is_some_and(|v| {
                v.len() == 71
                    && v.starts_with("sha256:")
                    && v[7..]
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            })
        };
        if self.store_id.is_empty()
            || self.controller_epoch.is_empty()
            || self.generation < 1
            || !uuid(&self.policy_id)
            || !uuid(&self.policy_version)
            || !uuid(&self.lease_token)
            || !uuid(&self.host_id)
            || self.domain.is_empty()
            || self.domain.len() > 253
            || self
                .domain
                .bytes()
                .any(|b| !(b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-'))
            || !(1000..=10000).contains(&self.timeout_ms)
            || self.due_at_ms > now
            || self.lease_expires_at_ms <= now
            || self.lease_expires_at_ms > now.saturating_add(45000)
            || !hash(revision)
            || !hash(proof)
            || self.before.get("enabled") != Some(&Value::Bool(true))
            || self.before.get("availability").is_none()
        {
            return Err("Invalid B2 authority claim".into());
        }
        Ok(())
    }
}

fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit() && !b.is_ascii_uppercase()
            }
        })
}

pub fn now_ms() -> Result<i64, Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
