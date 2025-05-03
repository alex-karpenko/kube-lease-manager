# kube-lease-manager

Ergonomic and reliable leader election using Kubernetes Lease API.

<p>
<a href="https://github.com/alex-karpenko/kube-lease-manager/actions/workflows/ci.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/kube-lease-manager/ci.yaml?label=ci" alt="CI status"></a>
<a href="https://github.com/alex-karpenko/kube-lease-manager/actions/workflows/audit.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/kube-lease-manager/audit.yaml?label=audit" alt="Audit status"></a>
<a href="https://github.com/alex-karpenko/kube-lease-manager/actions/workflows/publish.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/kube-lease-manager/publish.yaml?label=publish" alt="Crates.io publishing status"></a>
<a href="https://docs.rs/kube-lease-manager" rel="nofollow"><img src="https://img.shields.io/docsrs/kube-lease-manager" alt="docs.rs status"></a>
<a href="https://crates.io/crates/kube-lease-manager" rel="nofollow"><img src="https://img.shields.io/crates/v/kube-lease-manager" alt="Version at Crates.io"></a>
<a href="https://github.com/alex-karpenko/kube-lease-manager/blob/HEAD/LICENSE" rel="nofollow"><img src="https://img.shields.io/crates/l/kube-lease-manager" alt="License"></a>
</p>

`kube-lease-manager` is a high-level helper to facilitate leader election using
[Lease Kubernetes resource](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/).
It ensures that only a single instance of the lease managers holds the lock at any moment of time.

Some of the typical use cases:
* automatic coordination of leader election between several instances (Pods) of Kubernetes controllers;
* ensure only a single instance of concurrent jobs is running right now;
* exclusive acquiring of shared resource.

## Features

* `LeaseManager` is a central part of the crate.
  This is a convenient wrapper around a Kubernetes `Lease` resource to manage all aspects of a leader election process.
* Provides two different high-level approaches to lock and release lease:
  fully automated or partially manual lock control.
* Uses [Server-Side-Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
  approach to update lease state that facilitates conflict detection and resolution
  and makes impossible concurrent locking.
* Tolerates configurable time skew between nodes of the Kubernetes cluster.
* Behavioral parameters of the lease manager are easily and flexibly configurable.
* Uses well-known and highly appreciated [kube](https://crates.io/crates/kube)
  and [Tokio](https://crates.io/crates/tokio)
  crates to access Kubernetes API and coordinate asynchronous tasks execution.
* You don't need to use low-level Kubernetes API.
* Uses Tokio [tracing](https://crates.io/crates/tracing) crate to provide event logs.

Please visit [crate's documentation](https://docs.rs/kube-lease-manager/) to get details and more examples.

---

As mentioned above, `kube-lease-manager` provides two possible ways to manage lease lock:
1. _Fully automated_: you create `LeaseManager` instance and run its `watch()` method.
   It returns [Tokio watch channel](https://docs.rs/tokio/1.38.0/tokio/sync/watch/index.html) to watch on state changes.
   Besides that, it runs an unattended background task
   which permanently tries to lock lease if it's free and publish changed state to the channel.
   The task finishes if the channel is closed.
2. _Partially manual_: you create `LeaseManager`
   instance and use its `changed()`
   and `release()` methods to control lock.
   `changed()` tries to lock lease as soon as it becomes free and returns actual lock state when it's changed.
   Your responsibilities are:
   - to keep `changed()` running (it's a `Future`) to ensure lock is refreshing while it's in use;
   - to call `release()` when you don't need the lock and want to make it free for others.

The first way ensures that the lease is locked (has a holder) at any moment of time.
Second makes it possible to acquire and release a lock when you need it.

## Example

The simplest example using the first locking approach:
```rust
use kube::Client;
use kube_lease_manager::{LeaseManagerBuilder, Result};
use std::time::Duration;

#[tokio::main]
async fn main() {
   // Use the default Kube client
   let client = Client::try_default().await?;
   // Create the simplest LeaseManager with reasonable defaults using a convenient builder.
   // It uses a Lease resource called `test-watch-lease`.
   let manager = LeaseManagerBuilder::new(client, "test-watch-lease")
           .build()
           .await?;

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
            println!("Unable to get lock during 10s");
        }
    }

   // Explicitly close the control channel
   drop(channel);
   // Wait for the finish of the manager and get it back
   let _manager = tokio::join!(task).0.unwrap()?;

   Ok(())
}
```

Please visit [crate's documentation](https://docs.rs/kube-lease-manager/) to get more examples and usage details.

## Credits

The author was inspired on this piece of work by two other crates that provide similar functionality:
[kubert](https://crates.io/crates/kubert) and [kube-leader-election](https://crates.io/crates/kube-leader-election).
Both of them are great, thanks to the authors.
But both have something missing for one of my projects.
So it was a reason to create this one.

## License

This project is licensed under the [MIT license](LICENSE).
