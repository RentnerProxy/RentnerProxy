use super::*;
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn cold_load_keeps_runtime_responsive_and_cancelled_load_serialized() {
    let store = Arc::new(Store::new(directory()));
    let (started, release) = pause_io(&store);
    let reader_store = Arc::clone(&store);
    let reader = tokio::spawn(async move { reader_store.list().await });
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .expect("cold registry load must not block the runtime")
        .unwrap();
    assert!(store.state.try_lock().is_err());
    // Cancellation drops only the waiter; the admitted read retains its guard.
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), store.list())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), store.list())
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_commit_retains_serialization_and_publishes_before_following_mutation() {
    let (store, server, _) = fixture().await;
    store.save(ID.into(), request(0)).await.unwrap();
    let store = Arc::new(store);
    let mut state = store.lock().await.unwrap();
    let mut next = Store::registry(&mut state).unwrap().clone();
    next.connections[0].metadata.revision = 2;
    next.connections[0].metadata.name = "Persisted before cancellation".into();
    let (started, release) = pause_io(&store);
    let commit_store = Arc::clone(&store);
    let commit = tokio::spawn(async move { commit_store.commit(state, next).await });
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .expect("registry commit must not block the runtime")
        .unwrap();
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    // Poll the reader first so the queued compare-and-replace follows it.
    let listed = store.list();
    let deleted = store.delete(ID, 2);
    tokio::pin!(listed, deleted);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut listed)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut deleted)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let metadata = listed.await.unwrap().remove(0);
    assert_eq!(metadata.revision, 2);
    assert_eq!(metadata.name, "Persisted before cancellation");
    // The queued compare-and-replace sees the completed revision, not stale cache.
    deleted.await.unwrap();
    let restarted = Store::new(store.path.clone());
    assert!(restarted.list().await.unwrap().is_empty());
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_uncertain_commit_forces_authoritative_reload() {
    let (store, server, _) = fixture().await;
    store.save(ID.into(), request(0)).await.unwrap();
    let store = Arc::new(store);
    let mut state = store.lock().await.unwrap();
    let mut next = Store::registry(&mut state).unwrap().clone();
    next.connections[0].metadata.revision = 2;
    let (started, release) = pause_io(&store);
    store.fail_after_persist.store(true, Ordering::SeqCst);
    let commit_store = Arc::clone(&store);
    let commit = tokio::spawn(async move { commit_store.commit(state, next).await });
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap()
        .unwrap();
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    // Wait for the blocking operation itself, independent of its aborted waiter.
    let state = tokio::time::timeout(Duration::from_secs(1), store.state.lock())
        .await
        .unwrap();
    assert!(state.is_none());
    drop(state);
    assert_eq!(store.list().await.unwrap()[0].revision, 2);
    assert_eq!(store.delete(ID, 1).await, Err(Error::ConnectionConflict));
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn key_read_keeps_runtime_responsive_and_cancellation_retains_admission() {
    let mut store = Store::new(directory());
    store.key = Some([7; 32]);
    let store = Arc::new(store);
    let (started, release) = pause_io(&store);
    let read_store = Arc::clone(&store);
    let read = tokio::spawn(async move { read_store.key().await });
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .expect("credential key read must not block the runtime")
        .unwrap();
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    assert!(store.key_io.try_lock().is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), store.key())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), store.key())
            .await
            .unwrap()
            .unwrap(),
        [7; 32]
    );
}
