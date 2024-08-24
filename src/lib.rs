#![deny(unsafe_code, warnings, missing_docs)]
//! Ergonomic and reliable leader election using Kubernetes Lease API.
//!
//! `kube-lease-manager` is a high-level helper to facilitate leader election using
//! [Lease Kubernetes resource](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/).
//! It ensures that only a single instance of the lease managers holds the lock at any moment of time.
//!
//! Some of the typical use cases:
//! * automatic coordination of leader election between several instances (Pods) of Kubernetes controllers;
//! * ensure only a single instance of concurrent jobs is running right now;
//! * exclusive acquiring of shared resource.
//!
//! ## Features
//!
//! * [`LeaseManager`] is a central part of the crate.
//!   This is a convenient wrapper around a Kubernetes `Lease` resource to manage all aspects of leader election process.
//! * Provides two different high-level approaches to lock and release lease:
//!   fully automated or partially manual lock control.
//! * Uses [Server-Side-Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
//!   approach to update lease state that facilitates conflict detection and resolution
//!   and makes impossible concurrent locking.
//! * Tolerate configurable time skew between nodes of the Kubernetes cluster.
//! * Behavioral parameters of the lease manager are easily and flexibly configurable.
//! * Uses well-known and highly appreciated [kube](https://crates.io/crates/kube)
//!   and [Tokio](https://crates.io/crates/tokio)
//!   crates to access Kubernetes API and coordinate asynchronous tasks execution.
//! * You don't need to use low-level Kubernetes API.
//! * Uses Tokio [tracing](https://crates.io/crates/tracing) carte to provide event logs.
//!
//! ---
//!
//! As mentioned above, `kube-lease-manager` provides two possible ways to manage lease lock:
//! 1. _Fully automated_: you create [`LeaseManager`] instance and run its [`watch()`](LeaseManager::watch()) method.
//!    It returns [Tokio watch channel](https://docs.rs/tokio/latest/tokio/sync/watch/index.html) to watch on state changes
//!    Besides that it runs an unattended background task
//!    which permanently tries to lock lease if it's free and publish changed state to the channel.
//!    The task finishes if the channel is closed.
//! 2. _Partially manual_: you create [`LeaseManager`]
//!    instance and use its [`changed()`](LeaseManager::changed())
//!    and [`release()`](LeaseManager::release()) methods to control lock.
//!    `changed()` tries to lock lease as soon as it becomes free and returns actual lock state when it's changed.
//!    Your responsibilities are:
//!    - to keep `changed()` running (it's a `Future`) to ensure lock is refreshing while it's in use;
//!    - to call `release()` when you don't need the lock and want to make it free for others.
//!
//! First way ensures that lease is locked (has a holder) at any moment of time.
//! Second makes possible to acquire and release lock when you need it.
//!
//! ## LeaseManager config
//!
//! The main config of the [`LeaseManager`] is a [`LeaseParams`] structure, which
//! describes several parameters that affect managers' behavior.
//! Those parameters are:
//! * _identity_: unique string identifier of the lease manager among all other instances.
//!   Usually this is some randomly generated string (UUID, for example).
//!   `LeaseParams` can provide default value by generating random 32-symbol alphanumeric string,
//!   or you can explicitly specify identity string while creating `LeaseParams`
//!   or directly via [`LeaseManagerBuilder`].
//! * _duration_: this is a maximum duration (in seconds) of lock validity after last renewal (confirmation) of the lock.
//!   In other words, the current lock holder is obliged to renew (re-confirm) its lock during this time after last renewal.
//!   If the holder didn't re-confirm lock, any other `LeaseManager` instance is permitted to grab the lock.
//!   The default value provided by `LeaseParams` is 30 seconds and may be configured.
//! * _grace_: to avoid flapping losses of lock, the actual leaseholder tries to re-confirm (renew) lock earlier,
//!   before it expires (before the end of the `duration` interval).
//!   This parameter defines an interval (in seconds) before lock expiration when the lock holder has to renew its lock.
//!   As a side effect,
//!   this interval can be considered as a maximum allowed time synchronization skew between nodes in the Kubernetes cluster
//!   to avoid overlying locking.
//!   The default value provided by `LeaseParams` is 5 seconds and may be configured.
//! * _field_manager_:
//!   identifier of the Kubernetes [field manager](https://kubernetes.io/docs/reference/using-api/server-side-apply/#managers)
//!   to authorize changes of the Lease resources.
//!   It should be unique among other managers.
//!   Usually you don't need to specify it explicitly because of LeaseParams generates it concatenating crates'
//!   name (kube-lease-manager) and `identity` string.
//!   But it's possible to specify field manger directly via `LeaseParams`
//!   [with_field_manager()](LeaseParams::with_field_manager) method or using [`LeaseManagerBuilder`].
//!
//! The next config option is a [`LeaseCreateMode`]
//! which defines the behavior how [`LeaseManager`] manages Lease Kubernetes resource during startup.
//! The default behavior is [`AutoCreate`](LeaseCreateMode::AutoCreate):
//! create resource if it doesn't exist or use existing one if it's already present.
//!
//! The last significant parameter of the [`LeaseManager`] is a Lease resource name.
//! Obviously, it has no defaults and should be specified explicitly via LeaseManager [`new()`](LeaseManager::new)
//! constructor of using [`LeaseManagerBuilder`].
//!
//! ## Examples
//!
//! Create [`LeaseManager`] with reasonable defaults using convenient ['LeaseManagerBuilder'] and use ["watch"](LeaseManager::watch)
//! approach to get notified about lock holder changes.
//!
//! ```no_run
//! use kube::Client;
//! use kube_lease_manager::{LeaseManagerBuilder, Result};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!    // Use default Kube client
//!    let client = Client::try_default().await?;
//!
//!    // Create the simplest LeaseManager with reasonable defaults using convenient builder.
//!    // It uses Lease resource called `test-lease-name`.
//!    // With default auto-create mode Lease will be created if it doesn't exist,
//!    // or existing one will be used otherwise.
//!    // The default lease duration is 30 seconds with grace period 5 seconds.
//!    let manager = LeaseManagerBuilder::new(client, "test-lease-name")
//!            .build()
//!            .await?;
//!
//!    // Start manager in watching mode and get back status channel and task handler.
//!    let (mut channel, task) = manager.watch().await;
//!
//!    // Watch on the channel for lock state changes.
//!    tokio::select! {
//!         _ = channel.changed() => {
//!             let lock_state = *channel.borrow_and_update();
//!
//!             if lock_state {
//!                 // Do something useful as a leader
//!                 println!("Got a luck!");
//!             }
//!         }
//!         _ = tokio::time::sleep(Duration::from_secs(10)) => {
//!             println!("Unable to get lock during 10s");
//!         }
//!     }
//!
//!    // Explicitly close the control channel
//!    drop(channel);
//!
//!    // Wait for the finish of the manager and get it back
//!    let _manager = tokio::join!(task).0.unwrap()?;
//!
//!     Ok(())
//! }
//! ```
//!
//! More examples with detailed explanations can be found in the corresponding documentation chapters about [`LeaseManager`],
//! [`LeaseManagerBuilder`], [`LeaseParams`] and [`LeaseCreateMode`],
//! as well as an explanation of the specific errors provided by [`LeaseManagerError`].
//!
//! ## Notes about dependencies
//!
//! 1. We depend on [`kube`](https://crates.io/crates/kube) as on the Kubernetes API library.
//!    In the majority of possible use cases, interaction with Kubernetes API requires TLS,
//!    so one of the relevant features of the `kube`
//!    crate should be added to your dependencies: `rustls-tls` or `openssl-tls`.
//!    Please consult with [`kube` crate documentation](https://crates.io/crates/kube).
//!    We deliberately don't include such a dependency into `kube-lease-manager`
//!    to don't force you to use a particular dependency,
//!    it's up to you.
//! 2. We use [`k8s-openapi`](https://crates.io/crates/k8s-openapi), it provides bindings for the Kubernetes client API.
//!    If you use `kube-lease-manager`
//!    in a binary crate, you have to add this dependency as well with one of the needed features enabled
//!    to reflect a minimal version of Kubernetes API of your application.
//!    For example, `v1_26` or `latest`.
//!    Please consult with [`k8s-openapi` crate documentation](https://crates.io/crates/k8s-openapi).
//!
mod backoff;
mod error;
mod manager;
mod state;

pub use error::*;
pub use manager::*;
