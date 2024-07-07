use kube::Client;
use kube_lease_manager::LeaseManagerBuilder;
use std::time::Duration;

#[tokio::test]
#[ignore = "uses k8s current-context"]
async fn manual() {
    // Use default Kube client
    let client = Client::try_default().await.unwrap();
    // Create the simplest LeaseManager with reasonable defaults using convenient builder.
    // It uses Lease resource called `test-watch-lease`.
    let manager = LeaseManagerBuilder::new(client, "test-manual-lease")
        .build()
        .await
        .unwrap();

    // Try to get a lock on resource
    let state = manager.changed().await.unwrap();
    assert!(state);

    tokio::select! {
        // Ensure `changed()` is running to refresh lease lock
        lock_state = manager.changed() => {
            if let Ok(state) = lock_state {
                println!("Looks like lock state was changed to {state}");
                assert!(!state);
            } else {
                println!("Something wrong happened: {lock_state:?}")
            }
        }
        // Do everything you need with locked resource
        _ = async {
            tokio::time::sleep(Duration::from_secs(1)).await
        } => {
            println!("We've done our heavy work.");
            // Release lock after finish
            manager.release().await.unwrap();
            // And ensure state was changed
            assert!(!manager.changed().await.unwrap());
        }
    }
}
