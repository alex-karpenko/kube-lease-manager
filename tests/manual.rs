use kube::Client;
use kube_lease_manager::{LeaseManagerBuilder, Result};
use std::time::Duration;

#[tokio::test]
#[ignore = "uses k8s current-context"]
async fn manual() -> Result<()> {
    // Use the default Kube client
    let client = Client::try_default().await?;
    // Create the simplest LeaseManager with reasonable defaults using a convenient builder.
    // It uses a Lease resource called `test-watch-lease`.
    let manager = LeaseManagerBuilder::new(client, "test-manual-lease").build().await?;

    // Try to get a lock on resource
    let state = manager.changed().await?;
    assert!(state);

    // Let's run two branches:
    // - the first one watches on state to ensure we don't work with lost lease and refreshes lock
    // - the second one does actual work
    tokio::select! {
        // Ensure `changed()` is running to refresh the lease lock
        lock_state = manager.changed() => {
            if let Ok(state) = lock_state {
                println!("Looks like lock state was changed to {state} before we finished.");
                assert!(!state);
            } else {
                println!("Something wrong happened: {lock_state:?}.")
            }
        }
        // Do everything you need with a locked resource
        _ = async {
            println!("We got a lease lock! Lets do out heady work...");
            // Do something useful here
            tokio::time::sleep(Duration::from_secs(1)).await
        } => {
            println!("We've done our heavy work.");
            // Release lock after finish
            manager.release().await?;
            // And ensure the state was changed
            assert!(!manager.changed().await?);
        }
    }

    Ok(())
}
