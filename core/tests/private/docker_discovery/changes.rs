use super::*;
use models::{Cursor, Error, Status, Watch};
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn wake_cancellation_drops_an_inflight_publication_without_recording_failure() {
    use crate::runtime::background::{Outcome, Worker};
    let root = directory();
    let discovery = Discovery::new(root.clone());
    let held = discovery.snapshot.lock().await;
    let pass = discovery.background.begin(Worker::Docker);
    let document = Document::default();
    {
        let publication = discovery.publish(&document, Some(Vec::new()), None);
        tokio::pin!(publication);
        tokio::select! {
            biased;
            _ = &mut publication => panic!("publication must wait for the held snapshot"),
            _ = tokio::task::yield_now() => {},
        }
    }
    pass.finish(Outcome::Cancelled);
    let value = serde_json::to_value(discovery.background.snapshot()).unwrap();
    let worker = &value["workers"][3];
    assert_eq!(worker["attempts"], 1);
    assert_eq!(worker["failures"], 0);
    assert_eq!(worker["cancellations"], 1);
    assert_eq!(worker["runningCount"], 0);
    drop(held);
    std::fs::remove_dir_all(root).unwrap();
}

fn after(cursor: &Cursor) -> Watch {
    Watch {
        runtime_id: Some(cursor.runtime_id.clone()),
        generation: Some(cursor.generation),
    }
}

#[tokio::test]
async fn watch_identity_resets_on_controller_restart_without_changing_legacy_status() {
    let root = directory();
    let first = Discovery::new(root.clone());
    let next = Discovery::new(root.clone());
    let initial = first.watch(Watch::default()).await.unwrap();
    assert_eq!(initial.cursor.generation, 0);
    assert_eq!(initial.cursor.runtime_id.len(), 32);
    let replacement = next.watch(after(&initial.cursor)).await.unwrap();
    assert_ne!(initial.cursor.runtime_id, replacement.cursor.runtime_id);
    assert_eq!(replacement.snapshot.unwrap().status, Status::Disabled);
    let serialized = serde_json::to_value(first.status().await.unwrap()).unwrap();
    assert!(serialized.get("cursor").is_none());
    assert!(serialized.get("runtimeId").is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn inventory_generations_advance_only_on_semantic_changes_and_reject_old_endpoints() {
    let root = directory();
    let discovery = Discovery::new(root.clone());
    discovery
        .save(save(Some("http://first:2375".into()), true, 0))
        .await
        .unwrap();
    let connecting = discovery.watch(Watch::default()).await.unwrap();
    let document = connecting.snapshot.unwrap().configuration;
    let inventory = vec![candidate(container('a'))];
    discovery
        .publish(&document, Some(inventory.clone()), None)
        .await;
    let ready = discovery.watch(after(&connecting.cursor)).await.unwrap();
    assert_eq!(ready.cursor.generation, connecting.cursor.generation + 1);
    assert_eq!(ready.snapshot.unwrap().status, Status::Ready);
    discovery
        .publish(&document, Some(inventory.clone()), None)
        .await;
    let unchanged = discovery
        .watch_until(
            after(&ready.cursor),
            Instant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap();
    assert!(unchanged.snapshot.is_none());
    assert_eq!(unchanged.cursor, ready.cursor);
    discovery
        .publish(&document, None, Some(Error::EndpointUnavailable))
        .await;
    let unavailable = discovery.watch(after(&ready.cursor)).await.unwrap();
    assert!(unavailable.snapshot.unwrap().stale);
    discovery
        .save(save(Some("http://replacement:2375".into()), true, 1))
        .await
        .unwrap();
    let replaced = discovery.watch(Watch::default()).await.unwrap();
    discovery.publish(&document, Some(inventory), None).await;
    let unchanged = discovery
        .watch_until(
            after(&replaced.cursor),
            Instant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap();
    assert!(unchanged.snapshot.is_none());
    assert_eq!(unchanged.cursor, replaced.cursor);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn watch_waiters_are_bounded_and_shutdown_wakes_every_subscriber() {
    let root = directory();
    let discovery = Discovery::new(root.clone());
    let cursor = discovery.watch(Watch::default()).await.unwrap().cursor;
    let mut waiters = Vec::new();
    for _ in 0..2 {
        let instance = discovery.clone();
        let request = after(&cursor);
        waiters.push(tokio::spawn(async move { instance.watch(request).await }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while discovery.subscribers.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        discovery.watch(Watch::default()).await,
        Err(Error::EndpointUnavailable)
    ));
    discovery.stop_accepting();
    for waiter in waiters {
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Stopping)
        ));
    }
    assert_eq!(discovery.subscribers.available_permits(), 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn a_change_during_a_watch_is_observed_and_abandoned_waiters_release_admission() {
    let root = directory();
    let discovery = Discovery::new(root.clone());
    let cursor = discovery.watch(Watch::default()).await.unwrap().cursor;
    let instance = discovery.clone();
    let request = after(&cursor);
    let waiting = tokio::spawn(async move { instance.watch(request).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while discovery.subscribers.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    discovery
        .save(save(Some("http://docker-proxy:2375".into()), true, 0))
        .await
        .unwrap();
    let changed = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(changed.cursor.generation > cursor.generation);
    assert_eq!(changed.snapshot.unwrap().configuration.revision, 1);
    let instance = discovery.clone();
    let waiting = tokio::spawn(async move { instance.watch(after(&changed.cursor)).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while discovery.subscribers.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiting.abort();
    let _ = waiting.await;
    assert_eq!(discovery.subscribers.available_permits(), 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn invalid_watch_cursors_fail_before_reading_persistent_state() {
    let discovery = Discovery::new(std::env::temp_dir().join("discovery-watch-do-not-create"));
    for request in [
        Watch {
            runtime_id: Some("a".repeat(32)),
            generation: None,
        },
        Watch {
            runtime_id: None,
            generation: Some(0),
        },
        Watch {
            runtime_id: Some("A".repeat(32)),
            generation: Some(0),
        },
        Watch {
            runtime_id: Some("a".repeat(32)),
            generation: Some(9_007_199_254_740_992),
        },
    ] {
        assert!(matches!(
            discovery.watch(request).await,
            Err(Error::InvalidInput)
        ));
    }
}

#[tokio::test]
async fn waiting_readers_and_publishers_cannot_replace_a_newer_ready_inventory() {
    for publishing in [false, true] {
        let root = directory();
        let discovery = Discovery::new(root.clone());
        let first = discovery
            .save(save(Some("http://first:2375".into()), true, 0))
            .await
            .unwrap();
        discovery
            .publish(
                &first.configuration,
                Some(vec![candidate(container('a'))]),
                None,
            )
            .await;
        let mut snapshot = discovery.snapshot.lock().await;
        let io = discovery.io.lock().await;
        let (entered, started) = tokio::sync::oneshot::channel();
        let instance = discovery.clone();
        let waiting = tokio::spawn(async move {
            entered.send(()).unwrap();
            if publishing {
                instance
                    .publish(
                        &first.configuration,
                        Some(vec![candidate(container('a'))]),
                        None,
                    )
                    .await;
                Ok(None)
            } else {
                instance.status().await.map(Some)
            }
        });
        started.await.unwrap();
        drop(io);
        // FIFO I/O admission lets any premature document read finish while snapshot
        // remains locked. Readers that correctly lock snapshot first have not read yet.
        drop(discovery.io.lock().await);
        let replacement = discovery
            .save(save(Some("http://replacement:2375".into()), true, 1))
            .await
            .unwrap();
        let mut ready = models::Snapshot::for_configuration(replacement.configuration, false);
        ready.status = Status::Ready;
        ready.stale = false;
        ready.candidates = vec![candidate(container('b'))];
        // Model a completed newer inventory publication before admitting the waiter.
        *snapshot = ready.clone();
        discovery.advance_generation();
        let cursor = discovery.changes.borrow().clone().unwrap();
        drop(snapshot);
        let returned = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(returned) = returned {
            assert!(
                returned == ready,
                "a waiting reader must return the newest ready inventory"
            );
        }
        let current = discovery.watch(Watch::default()).await.unwrap();
        assert!(
            current.snapshot.as_ref() == Some(&ready),
            "a waiting publisher must not restore its old endpoint inventory"
        );
        assert_eq!(current.cursor, cursor);
        std::fs::remove_dir_all(root).unwrap();
    }
}
