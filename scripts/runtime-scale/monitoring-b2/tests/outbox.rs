use rentnerproxy_monitoring_b2_experiment::{Claim, Outbox};
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn directory() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rentnerproxy-b2-unit-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn claim(index: u32) -> Claim {
    Claim {
        store_id: "store".into(),
        controller_epoch: "epoch".into(),
        generation: 1,
        policy_id: format!("00000000-0000-4000-8000-{index:012}"),
        policy_version: "00000000-0000-4000-8000-000000000001".into(),
        lease_token: format!("00000000-0000-4000-8000-{index:012}"),
        lease_expires_at_ms: 46000,
        due_at_ms: 1000,
        host_id: format!("00000000-0000-4000-8000-{index:012}"),
        domain: "fixture.example.test".into(),
        timeout_ms: 3000,
        before: json!({"enabled":true,"proof":format!("sha256:{}","a".repeat(64)),"revision":format!("sha256:{}","b".repeat(64)),"availability":{"proof":format!("sha256:{}","c".repeat(64)),"revision":format!("sha256:{}","d".repeat(64))}}),
    }
}

#[test]
fn restart_preserves_starts_and_recovers_interrupted_without_fabricating_results() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        assert!(Outbox::open(&path, "store", "epoch").is_err());
    }
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        assert_eq!(store.recover_interrupted(2000).unwrap(), 1);
        assert_eq!(store.recover_interrupted(2001).unwrap(), 0);
        assert!(
            store
                .finish(&claim(1), &json!({"success":true}), 2002)
                .is_err()
        );
    }
    assert!(Outbox::open(&path, "different", "epoch").is_err());
    assert!(Outbox::open(&path, "store", "different").is_err());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn bounded_store_blocks_admission_at_capacity_and_retention_without_deleting_evidence() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        for index in 1..=4096 {
            store.start(&claim(index), 1000).unwrap();
            store
                .finish(&claim(index), &json!({"error":"runtime_unavailable"}), 1001)
                .unwrap();
        }
        assert_eq!(store.available(2000).unwrap(), 0);
        assert!(store.start(&claim(4097), 1000).is_err());
    }
    {
        let store = Outbox::open(&path, "store", "epoch").unwrap();
        assert_eq!(store.available(121000).unwrap(), 0);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn restore_during_checks_cannot_complete_into_replaced_identity() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        let admin = rusqlite::Connection::open(&path).unwrap();
        admin
            .execute(
                "UPDATE b2_identity SET store_id='restored',epoch='restored'",
                [],
            )
            .unwrap();
        assert!(
            store
                .finish(&claim(1), &json!({"error":"runtime_unavailable"}), 1001)
                .is_err()
        );
        assert!(store.start(&claim(2), 1000).is_err());
        assert!(store.admitted(&claim(1), 1001).is_err());
        assert!(store.recover_interrupted(1002).is_err());
        assert!(store.completed_claims().is_err());
        assert!(store.available(1002).is_err());
        let unchanged: i64 = admin
            .query_row(
                "SELECT count(*) FROM b2_outbox WHERE result IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unchanged, 1);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn oversized_conflicting_or_expired_starts_cannot_mutate_the_durable_record() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        assert!(store.start(&claim(1), 1000).is_err());
        assert!(store.start(&claim(2), 46000).is_err());
        assert!(
            store
                .finish(&claim(1), &json!({"error":"x".repeat(4096)}), 1001)
                .is_err()
        );
        assert_eq!(store.available(2000).unwrap(), 4095);
        assert_eq!(store.recover_interrupted(2001).unwrap(), 1);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn missing_durable_sequence_latches_admission_off_across_reopen() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        store.start(&claim(2), 1000).unwrap();
        let admin = rusqlite::Connection::open(&path).unwrap();
        admin
            .execute("DELETE FROM b2_outbox WHERE sequence=1", [])
            .unwrap();
        assert_eq!(store.available(1001).unwrap(), 0);
        assert!(store.start(&claim(3), 1001).is_err());
        let status: (i64, i64) = admin
            .query_row(
                "SELECT gaps,resync_required FROM b2_status WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, (1, 1));
    }
    {
        let store = Outbox::open(&path, "store", "epoch").unwrap();
        assert_eq!(store.available(1002).unwrap(), 0);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn completion_and_admission_require_the_exact_durable_claim() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        let original = claim(1);
        store.start(&original, 1000).unwrap();
        let mut changed = original.clone();
        changed.generation += 1;
        assert!(store.admitted(&changed, 1001).is_err());
        assert!(
            store
                .finish(&changed, &json!({"error":"interrupted"}), 1002)
                .is_err()
        );
        changed = original.clone();
        changed.controller_epoch = "restored".into();
        assert!(store.admitted(&changed, 1001).is_err());
        assert!(
            store
                .finish(&changed, &json!({"error":"interrupted"}), 1002)
                .is_err()
        );
        store.admitted(&original, 1001).unwrap();
        assert!(store.admitted(&original, 1002).is_err());
        store
            .finish(&original, &json!({"error":"runtime_unavailable"}), 1003)
            .unwrap();
        let completed = store.completed_claims().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].attempt_id(), original.attempt_id());
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn terminal_ack_preserves_the_sequence_prefix_and_releases_capacity() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        store.start(&claim(2), 1000).unwrap();
        store
            .finish(&claim(1), &json!({"error":"runtime_unavailable"}), 1001)
            .unwrap();
        // The replay adapter owns terminal receipt verification; emulate its atomic local ACK.
        let mut replay = rusqlite::Connection::open(&path).unwrap();
        let tx = replay.transaction().unwrap();
        tx.execute("DELETE FROM b2_outbox WHERE sequence=1", [])
            .unwrap();
        tx.execute("UPDATE b2_identity SET acked_sequence=1 WHERE id=1", [])
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(store.available(1002).unwrap(), 4095);
        store.start(&claim(3), 1002).unwrap();
        assert_eq!(store.available(1003).unwrap(), 4094);
    }
    {
        let store = Outbox::open(&path, "store", "epoch").unwrap();
        assert_eq!(store.available(1004).unwrap(), 4094);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn invalid_boundary_timestamps_fail_without_overflowing() {
    let mut invalid = claim(1);
    invalid.lease_expires_at_ms = i64::MAX;
    assert!(invalid.validate(i64::MAX).is_err());
    assert!(invalid.validate(i64::MIN).is_err());
}

#[test]
fn corrupted_ack_cursor_latches_admission_off_without_overflowing() {
    let directory = directory();
    let path = directory.join("outbox.sqlite");
    {
        let mut store = Outbox::open(&path, "store", "epoch").unwrap();
        store.start(&claim(1), 1000).unwrap();
        let replay = rusqlite::Connection::open(&path).unwrap();
        replay
            .execute(
                "UPDATE b2_identity SET acked_sequence=?1 WHERE id=1",
                [i64::MIN],
            )
            .unwrap();
        assert_eq!(store.available(1001).unwrap(), 0);
        assert!(store.start(&claim(2), 1001).is_err());
    }
    fs::remove_dir_all(directory).unwrap();
}
