use kube::Client;
use kube_lease_manager::LeaseManagerBuilder;
use std::time::Duration;

#[tokio::test]
#[ignore = "uses k8s current-context"]
async fn auto() {
    // Use default Kube client
    let client = Client::try_default().await.unwrap();
    // Create the simplest LeaseManager with reasonable defaults using convenient builder.
    // It uses Lease resource called `test-watch-lease`.
    let manager = LeaseManagerBuilder::new(client, "test-auto-lease")
        .build()
        .await
        .unwrap();

    let (mut channel, task) = manager.watch().await;
    // Watch on the channel for lock state changes
    tokio::select! {
        _ = channel.changed() => {
            let lock_state = *channel.borrow_and_update();

            if lock_state {
                // Do something useful as a leader
                println!("Got a luck!");
            }
        }
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            println!("Unable get lock during 10s");
        }
    }

    // Explicitly close the control channel
    drop(channel);
    // Wait for the finish of the manager and get it back
    let _manager = tokio::join!(task).0.unwrap().unwrap();
}
