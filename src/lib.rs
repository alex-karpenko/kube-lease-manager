#![deny(unsafe_code)]
//! Ergonomic and durable leader election using Kubernetes Lease API.
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
//! * Uses Tokio [`tracing`](https://crates.io/crates/tracing) carte to provide event logs.
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
//! which defines the behavior how LeaseManager manages Lease Kubernetes resource during startup.
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
//!             println!("Unable get lock during 10s");
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

mod backoff;
mod state;

use backoff::{BackoffSleep, DurationFloat};
use kube::Client;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use state::{LeaseLockOpts, LeaseState, LeaseStateError};
use std::{
    fmt::Debug,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};
use tokio::{select, sync::RwLock, task::JoinHandle};
use tracing::{debug, error, trace};

type DurationMillis = u64;

/// Since all durations related to Lease resource are in seconds, this alias is useful.
pub type DurationSeconds = u64;
/// Convenient alias for `Result`. Uses [`LeaseManagerError`] as an Error type.
pub type Result<T, E = LeaseManagerError> = std::result::Result<T, E>;

/// Default value of the `duration` parameter.
pub const DEFAULT_LEASE_DURATION_SECONDS: DurationSeconds = 30;
/// Default value of the `grace` parameter.
pub const DEFAULT_LEASE_GRACE_SECONDS: DurationSeconds = 5;

/// Length of the random identity string generated by [`default`](LeaseParams::default) [`LeaseParams`] constructor.
pub const DEFAULT_RANDOM_IDENTITY_LEN: usize = 32;
const DEFAULT_FIELD_MANAGER_PREFIX: &str = env!("CARGO_PKG_NAME");

const MIN_RELEASE_WAITING_MILLIS: DurationMillis = 100;
const MAX_RELEASE_WAITING_MILLIS: DurationMillis = 1000;

const MIN_CONFLICT_BACKOFF_TIME: DurationFloat = 0.1;
const MAX_CONFLICT_BACKOFF_TIME: DurationFloat = 5.0;
const CONFLICT_BACKOFF_MULT: DurationFloat = 2.0;

const MIN_WATCHER_BACKOFF_TIME: DurationFloat = 1.0;
const MAX_WATCHER_BACKOFF_TIME: DurationFloat = 30.0;
const WATCHER_BACKOFF_MULT: DurationFloat = 2.0;

/// Represents `kube-lease-manager` specific errors.
#[derive(thiserror::Error, Debug)]
pub enum LeaseManagerError {
    /// Error originated from the Kubernetes.
    #[error("Kube error: {0}")]
    KubeError(
        #[source]
        #[from]
        kube::Error,
    ),

    /// Internal state inconsistency detected.
    ///
    /// Usually the root cause is a bug.
    #[error("inconsistent LeaseManager state detected: {0}")]
    InconsistentState(String),

    /// Try to create a new `Lease` resource, but it already exists.
    #[error("Lease resource `{0}` already exists")]
    LeaseAlreadyExists(String),

    /// Try to use non-existent `Lease` resource.
    #[error("Lease resource `{0}` doesn't exist")]
    NonexistentLease(String),

    /// `LeaseManager` unable to send state to the control channel.
    #[error("Control channel error: {0}")]
    ControlChannelError(
        #[source]
        #[from]
        tokio::sync::watch::error::SendError<bool>,
    ),
}

/// Parameters of [`LeaseManager`].
///
/// The structure describes several parameters that affect [`LeaseManager`] behavior:
/// * _identity_: unique string identifier of the lease manager among all other instances.
///   Usually this is some randomly generated string (UUID, for example).
///   `LeaseParams` can provide default value by generating random 32-symbol alphanumeric string,
///   or you can explicitly specify identity string while creating `LeaseParams` with [`new()`](LeaseParams::new)
///   constructor or via [`LeaseManagerBuilder`].
/// * _duration_: this is a maximum duration (in seconds) of lock validity after last renewal (confirmation) of the lock.
///   In other words, the current lock holder is obliged to renew (re-confirm) its lock during this time after last renewal.
///   If the holder didn't re-confirm lock, any other `LeaseManager` instance is permitted to grab the lock.
///   The default value provided by `LeaseParams` is 30 seconds and may be configured.
/// * _grace_: to avoid flapping losses of lock, the actual leaseholder tries to re-confirm (renew) lock earlier,
///   before it expires (before the end of the `duration` interval).
///   This parameter defines an interval (in seconds) before lock expiration when the lock holder has to renew its lock.
///   As a side effect,
///   this interval can be considered as a maximum allowed time synchronization skew between nodes in the Kubernetes cluster
///   to avoid overlying locking.
///   The default value provided by `LeaseParams` is 5 seconds and may be configured.
/// * _field_manager_:
///   identifier of the Kubernetes [field manager](https://kubernetes.io/docs/reference/using-api/server-side-apply/#managers)
///   to authorize changes of the Lease resources.
///   It should be unique among other managers.
///   Usually you don't need to specify it explicitly because of LeaseParams generates it concatenating crates'
///   name (kube-lease-manager) and `identity` string.
///   But it's possible to specify field manger directly via `LeaseParams`
///   [with_field_manager()](LeaseParams::with_field_manager) method or using [`LeaseManagerBuilder`].
///
/// `LeaseParams` may be created using [`default()`](LeaseParams::default)
/// constructor, [`new()`](LeaseParams::new) constructor,
/// or you can specify any non-default parameter value by creating `LeaseManager` using [`LeaseManagerBuilder`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseParams {
    /// Lease holder identity.
    identity: String,
    /// Duration of lease lock.
    duration: DurationSeconds,
    /// Period of tme to renew lease lock before it expires.
    grace: DurationSeconds,
    /// Field manager.
    field_manager: String,
}

/// Lease Kubernetes resource creation mode.
///
/// To coordinate leader election,
/// [`LeaseManager`] uses [`Lease`](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/)
/// Kubernetes resource.
/// You specify `Lease` resource name during creation of the `LeaseManager` instance.
/// So to use that object, it should exist.
///
/// `LeaseCreateMode` describes is `Lease` resource should be created and how,
/// or it should exist before `LeaseManager` constructing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum LeaseCreateMode {
    /// If a specified Lease object is already present in the Kubernetes cluster, then use it.
    /// Otherwise, try to create a new object and use it.
    /// This is a default behavior.
    #[default]
    AutoCreate,
    /// Use only a newly created Lease object.
    /// If it's already present in the cluster,
    /// LeaseManager constructor fails with [`LeaseAlreadyExists`](LeaseManagerError::LeaseAlreadyExists) error.
    CreateNew,
    /// Lease object should be existent before call of the `LeaseManager` constructor.
    /// If it doesn't exist, construct fails with [`NonexistentLease`](LeaseManagerError::NonexistentLease) error.
    UseExistent,
    #[cfg(test)]
    Ignore,
}

impl From<LeaseStateError> for LeaseManagerError {
    fn from(value: LeaseStateError) -> Self {
        match value {
            LeaseStateError::LockConflict => unreachable!("this branch is unreachable, looks like a BUG!"),
            LeaseStateError::KubeError(err) => LeaseManagerError::KubeError(err),
            LeaseStateError::LeaseAlreadyExists(lease) => LeaseManagerError::LeaseAlreadyExists(lease),
            LeaseStateError::NonexistentLease(lease) => LeaseManagerError::NonexistentLease(lease),
            LeaseStateError::InconsistentState(err) => LeaseManagerError::InconsistentState(err),
        }
    }
}

/// Wrapper around a Kubernetes Lease resource to manage all aspects of leader election process.
///
/// There are two possible ways to manage lease lock:
/// fully automated using managers' [`watch()`](LeaseManager::watch)
/// method and partially manual using [`changed()`](LeaseManager::changed)
/// and [`release()`](LeaseManager::release) methods.
///
/// ## Fully automated way
///
/// You create [`LeaseManager`] instance and run its [`watch()`](LeaseManager::watch()) method.
/// It consumes manager instance (to prevent using other methods)
/// and returns [Tokio watch channel](https://docs.rs/tokio/latest/tokio/sync/watch/index.html)
/// to watch on state changes,
/// and Tokio [`task handler`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html).
///
/// It runs a detached background [`Tokio task`](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html)
/// which permanently tries to lock lease if it's free and publish changed state to the channel.
/// The task finishes if the channel is closed or in case of channel error.
/// So this approach ensures that `Lease` is locked by some manager at any moment of time.
///
/// `Lease` lock state is a `bool` value,
/// wrapped into the [`Receiver`](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html) structure,
/// it's `true` if the manager got the lease lock, and `false` if it lost the lease.
/// Refer to the [`tokio::sync::watch`](https://docs.rs/tokio/latest/tokio/sync/watch/index.html)
/// documentation for details about using it.
/// But the most convenient way is to use receivers'
/// [`changed()`](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html#method.changed)
/// method to get notified when channels' value was changed,
/// and follow it by the [`borrow_and_update()`](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html#method.borrow_and_update)
/// method call to read the actual state value.
///
/// When the channel is closed (if all receivers went out of their scopes and deleted, or by explicit `drop()` call),
/// watching task finishes,
/// and you can (preferably you should) call [`tokio::join`](https://docs.rs/tokio/latest/tokio/macro.join.html)
/// to get its result.
/// It returns the previously consumed `LeaseManager` instance,
/// so you can use it next time, but the new status channel and the new watching task will be created.
///
/// `watch()` tolerates (hides) all errors except watch channel related ones.
/// In case of error, it tries to recover state repeating calls to Kubernetes API with an increasing backoff interval.
///
/// ### Example
///
/// ```no_run
/// ```
///
/// ## Partially manual approach
///
/// The same, you create [`LeaseManager`] instance but use its [`changed()`](LeaseManager::changed())
/// and [`release()`](LeaseManager::release()) methods to control lock.
/// `changed()` tries to lock lease as soon as it becomes free and returns actual lock state when it's changed.
/// It finishes and returns changed state each time the managers' lock state changes.
/// It stays running until the state of this particular manager is the same as before.
///
/// At the same time, if the manager holds lock then `changed()` method keeps it held by permanently refreshing lease lock.
/// So to avoid loss of the lock, you're responsible to keep `changed()` running (it's a `Future`)
/// to ensure refreshing of the lock,
/// and to ensure this particular manager is a holder of the lease until you need it locked.
///
/// When you don't need the lease lock anymore, you have to call `release()`
/// method and make lock free to be acquired by any other manager.
///
/// In contrast to the previous approach, `changed()` method returns any errors related to interacting with Kubernetes API.
/// So it's up to you how to respond to errors.
///
/// ### Example
///
/// ```no_run
/// ```
#[derive(Debug)]
pub struct LeaseManager {
    /// Parameters of the desired lock.
    params: LeaseParams,
    /// Current state.
    state: RwLock<LeaseState>,
    /// Is current identity set as leader now.
    is_leader: AtomicBool,
}

impl LeaseParams {
    /// Constructs instances of `LeaseParams`.
    ///
    /// Parameters should satisfy all conditions below, otherwise method panics:
    /// * duration > 0;
    /// * grace > 0;
    /// * grace < duration.
    pub fn new(identity: impl Into<String>, duration: DurationSeconds, grace: DurationSeconds) -> Self {
        let duration: DurationSeconds = duration;
        let grace: DurationSeconds = grace;

        if duration == 0 || grace == 0 {
            panic!("duration and grace period should be greater than zero");
        } else if grace >= duration {
            panic!("grace period should be less than lease lock duration");
        }

        let identity = identity.into();
        let field_manager = format!("{DEFAULT_FIELD_MANAGER_PREFIX}-{}", identity);
        Self {
            identity,
            duration,
            grace,
            field_manager,
        }
    }

    /// Set the specified field manager value instead of the default one.
    ///
    /// Default is `identity` string prefixed by `kube-lease-manager-`, and usually you don't need to change it, by can.
    ///
    /// It has to be unique among other field managers withing the scope of the `Lease` resource.
    /// So if you change it, your responsibility is to ensure its uniqueness.
    pub fn with_field_manager(self, field_manager: impl Into<String>) -> Self {
        Self {
            field_manager: field_manager.into(),
            ..self
        }
    }

    /// Return current filed_manager value to use in [`patch()`](LeaseState::patch).
    fn field_manager(&self) -> String {
        self.field_manager.clone()
    }
}

impl Default for LeaseParams {
    /// Creates parameters instance with reasonable defaults:
    ///   - random alphanumeric identity string of [`DEFAULT_RANDOM_IDENTITY_LEN`] length;
    ///   - the lease duration is [`DEFAULT_LEASE_DURATION_SECONDS`];
    ///   - the lease grace period is [`DEFAULT_LEASE_GRACE_SECONDS`].
    fn default() -> Self {
        Self::new(
            random_string(DEFAULT_RANDOM_IDENTITY_LEN),
            DEFAULT_LEASE_DURATION_SECONDS,
            DEFAULT_LEASE_GRACE_SECONDS,
        )
    }
}

/// Convenient builder of [`LeaseManager`].
///
/// It facilitates creating of the manager instance with reasonable defaults.
///
/// Please refer to [`LeaseParams`] and [`LeaseCreateMode`] for detailed explanation of each parameter.
///
/// ## Examples
///
/// To build default [`LeaseManager`]:
///
/// ```no_run
/// use kube::Client;
/// use kube_lease_manager::{LeaseManagerBuilder, LeaseManager, Result};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let client = Client::try_default().await?;
///     let manager = LeaseManagerBuilder::new(client, "my-unique-test-lease")
///         .build()
///         .await?;
///     // ...
///
///     Ok(())
/// }
/// ```
///
/// Redefine everything possible:
///
/// ```no_run
/// use kube::Client;
/// use kube_lease_manager::{LeaseManagerBuilder, LeaseManager, Result, LeaseCreateMode};
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let identity = Uuid::new_v4();
///     let client = Client::try_default().await?;
///
///     let manager = LeaseManagerBuilder::new(client, "existing-test-lease")
///         .with_namespace("lease-manager-test-ns")
///         .with_create_mode(LeaseCreateMode::UseExistent)
///         .with_field_manager(format!("custom-field-manager-{identity}"))
///         .with_identity(identity)
///         .with_duration(128)
///         .with_grace(16)
///         .build()
///         .await?;
///     // ...
///
///     Ok(())
/// }
/// ```
pub struct LeaseManagerBuilder {
    client: Client,
    lease_name: String,
    namespace: String,
    params: LeaseParams,
    create_mode: LeaseCreateMode,
}

impl LeaseManagerBuilder {
    /// Constructs minimal builder instance with all other parameters set to default values.
    ///
    /// See each builders' method for details about defaults and restrictions.
    pub fn new(client: Client, lease_name: impl Into<String>) -> Self {
        Self {
            client,
            lease_name: lease_name.into(),
            namespace: String::from("default"),
            params: LeaseParams::default(),
            create_mode: LeaseCreateMode::default(),
        }
    }

    /// Builds [`LeaseManager`] from the builder instance using [`LeaseManager::new()`] constructor.
    ///
    /// This method is async since [`LeaseManager::new()`] is also async,
    /// because constructor uses Kubernetes API to ensure the existence of the Lease resource.
    ///
    /// May return [`LeaseManagerError`] in case of issues with interacting with Kubernetes cluster using provided `client`.
    pub async fn build(self) -> Result<LeaseManager> {
        LeaseManager::new(
            self.client,
            &self.lease_name,
            &self.namespace,
            self.params,
            self.create_mode,
        )
        .await
    }

    /// Use the specified `Lease` name instead of the one provided to [`LeaseManagerBuilder::new()`].
    pub fn with_lease_name(self, lease_name: impl Into<String>) -> Self {
        Self {
            lease_name: lease_name.into(),
            ..self
        }
    }

    /// Updates namespace which is used for `Lease` resource.
    ///
    /// Default value is `"default"`.
    pub fn with_namespace(self, namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ..self
        }
    }

    /// Updates [`LeaseCreateMode`] of the manager.
    ///
    /// The default value is [`AutoCreate`](LeaseCreateMode::AutoCreate).
    pub fn with_create_mode(self, create_mode: LeaseCreateMode) -> Self {
        Self { create_mode, ..self }
    }

    /// Updates whole [`LeaseParams`] instance of the manager.
    ///
    /// There four additional methods to set each parameters' value individually.
    pub fn with_parameters(self, params: LeaseParams) -> Self {
        Self { params, ..self }
    }

    /// Updates `Lease` identity value.
    ///
    /// The default value is a random alphanumeric string of [`DEFAULT_RANDOM_IDENTITY_LEN`] length.
    pub fn with_identity(self, identity: impl Into<String>) -> Self {
        Self {
            params: LeaseParams {
                identity: identity.into(),
                ..self.params
            },
            ..self
        }
    }

    /// Updates `Lease` duration parameter.
    ///
    /// Default is [30 seconds](DEFAULT_LEASE_DURATION_SECONDS).
    ///
    /// May panic if duration is less than 0, or if duration is less or equal to the current grace value.
    pub fn with_duration(self, duration: DurationSeconds) -> Self {
        Self {
            params: LeaseParams {
                duration,
                ..self.params
            },
            ..self
        }
    }

    /// Updates lease grace period parameter.
    ///
    /// Default is [5 seconds](DEFAULT_LEASE_GRACE_SECONDS).
    ///
    /// May panic if grace is less than 0, or if grace is greater or equal to the current duration value.
    pub fn with_grace(self, grace: DurationSeconds) -> Self {
        Self {
            params: LeaseParams { grace, ..self.params },
            ..self
        }
    }

    /// Updates `Lease` field manager value.
    ///
    /// Default is `identity` string prefixed by `kube-lease-manager-`.
    ///
    /// Make sure that if you change, it has to be unique among other field managers withing the scope of the `Lease` resource.
    pub fn with_field_manager(self, field_manager: impl Into<String>) -> Self {
        Self {
            params: self.params.with_field_manager(field_manager.into()),
            ..self
        }
    }
}

impl LeaseManager {
    /// Basic constructor of the `LeaseManager` instance.
    /// See [`LeaseManagerBuilder`] for another way to make it.
    ///
    /// Besides constructing of the `LeaseManager,` it ensures `Lease` resource with respect to specified `crate_mode`:
    /// creates `Lease` resource or verifies its existence with specified name in the provided namespace.
    ///
    /// It may return [`LeaseManagerError`] in case of issues during interaction with Kubernetes cluster
    /// using provided `client`'
    pub async fn new(
        client: Client,
        lease_name: impl Into<String>,
        namespace: impl Into<String>,
        params: LeaseParams,
        create_mode: LeaseCreateMode,
    ) -> Result<Self> {
        let manager = Self {
            params,
            state: RwLock::new(LeaseState::new(client, lease_name.into(), &namespace.into(), create_mode).await?),
            is_leader: AtomicBool::new(false),
        };

        trace!(manager = ?manager, "constructed new LeaseManager");
        Ok(manager)
    }

    /// Spawns a [`Tokio`](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html) task and watch on leader changes permanently.
    ///
    /// If self-state changes (became a leader or lost the lock), it sends actual lock state to the channel.
    /// Exits if the channel is closed (all receivers are gone) or in case of any sending error.
    ///
    /// See [`more details and examples`](LeaseManager#fully-automated-way) about this method and approach.
    pub async fn watch(self) -> (tokio::sync::watch::Receiver<bool>, JoinHandle<Result<LeaseManager>>) {
        let (sender, receiver) = tokio::sync::watch::channel(self.is_leader.load(Ordering::Relaxed));
        let watcher = async move {
            debug!(params = ?self.params, "starting watch loop");

            let mut backoff =
                BackoffSleep::new(MIN_WATCHER_BACKOFF_TIME, MAX_WATCHER_BACKOFF_TIME, WATCHER_BACKOFF_MULT);

            loop {
                select! {
                    biased;
                    _ = sender.closed() => {
                        // Consumer has closed all receivers - release the lock and exit
                        debug!(identity = %self.params.identity, "control channel has been closed");
                        let result = self.release().await;
                        return match result {
                            Ok(_) => Ok(self),
                            Err(err) => Err(err),
                        };
                    }
                    result = self.changed() => {
                        debug!(identity = %self.params.identity, "lease state has been changed");
                        match result {
                            Ok(state) => {
                                let result = sender.send(state);
                                match result {
                                    Ok(_) => backoff.reset(),
                                    Err(err) => {
                                        let _ = self.release().await;
                                        return Err(LeaseManagerError::ControlChannelError(err));
                                    }
                                };
                            }
                            Err(e) => {
                                error!(error = %e, "LeaseManager watcher error");
                                if sender.is_closed() {
                                    let result = self.release().await;
                                    return match result {
                                        Ok(_) => Ok(self),
                                        Err(err) => Err(err),
                                    };
                                } else {
                                    backoff.sleep().await;
                                }
                            }
                        }

                    }
                }
            }
        };

        let handler = tokio::spawn(watcher);

        (receiver, handler)
    }

    /// Try to lock lease as soon as it's free and renew it periodically to prevent expiration.
    ///
    /// Returns own status of the leader lock as soon as it was changed (acquired or released).
    ///
    /// See [`detailed explanation and examples`](LeaseManager#partially-manual-approach).
    ///
    /// May return [`LeaseManagerError`] in case of issues during interaction with Kubernetes API.
    pub async fn changed(&self) -> Result<bool> {
        let mut backoff = BackoffSleep::new(
            MIN_CONFLICT_BACKOFF_TIME,
            MAX_CONFLICT_BACKOFF_TIME,
            CONFLICT_BACKOFF_MULT,
        );

        loop {
            // re-sync state if needed
            self.state.write().await.sync(LeaseLockOpts::Soft).await?;

            // Is leader changed iteration?
            let is_holder = self.state.read().await.is_holder(&self.params.identity);
            if self.is_leader.load(Ordering::Acquire) != is_holder {
                debug!(identity = %self.params.identity, is_leader = %is_holder, "lease lock state has been changed");
                self.is_leader.store(is_holder, Ordering::Release);

                return Ok(is_holder);
            }

            // Make a single iteration, if no changes so far.
            match self.watcher_step().await {
                Ok(_) => {
                    // reset backoff and continue
                    backoff.reset();
                }
                Err(LeaseStateError::LockConflict) => {
                    // Wait for a backoff interval and continue.
                    backoff.sleep().await;
                }
                Err(err) => return Err(LeaseManagerError::from(err)),
            }
        }
    }

    /// Release self-lock if it's set, or do nothing if the lease is locked by another identity.
    ///
    /// It's useful to call this method to free locked lease gracefully before exit/shutdown
    /// if you use [`LeaseManager::changed()`](LeaseManager::changed)
    /// directly instead of using [`LeaseManager::watch()`](LeaseManager::watch).
    ///
    /// It's safe to call it even if this particular manager isn't a leader.
    ///
    /// May return [`LeaseManagerError`] in case of issues during interaction with Kubernetes API.
    pub async fn release(&self) -> Result<()> {
        self.state
            .write()
            .await
            .release(&self.params, LeaseLockOpts::Soft)
            .await
            .map_err(LeaseManagerError::from)
    }

    async fn watcher_step(&self) -> Result<(), LeaseStateError> {
        if self.is_holder().await {
            // if we're the holder of the lock - sleep up to the next refresh time,
            // and renew lock (lock it softly)
            tokio::time::sleep(self.grace_sleep_duration(self.expiry().await, self.params.grace)).await;

            debug!(identity = %self.params.identity, "renew own lease lock");
            self.state.write().await.lock(&self.params, LeaseLockOpts::Soft).await
        } else if !self.is_locked().await {
            // Lease isn't locket yet
            debug!(identity = %self.params.identity, "try to lock orphaned lease");
            self.state.write().await.lock(&self.params, LeaseLockOpts::Soft).await
        } else if self.is_locked().await && self.is_expired().await {
            // It's locked by someone else but lock is already expired.
            // Release it by force and try to lock on the next loop cycle
            debug!(identity = %self.params.identity, "release expired lease lock");
            let res = self
                .state
                .write()
                .await
                .release(&self.params, LeaseLockOpts::Force)
                .await;

            // Sleep some random time (up to 1000ms) to minimize collision probability
            tokio::time::sleep(random_duration(MIN_RELEASE_WAITING_MILLIS, MAX_RELEASE_WAITING_MILLIS)).await;
            res
        } else if self.is_locked().await && !self.is_expired().await {
            // It's locked by someone else and the lock is actual.
            // Sleep up to the expiration time of the lock.
            let holder = self.holder().await.unwrap();
            debug!(identity = %self.params.identity, %holder,"lease is actually locked by other identity");
            tokio::time::sleep(self.grace_sleep_duration(self.expiry().await, 0)).await;
            Ok(())
        } else {
            // Something wrong happened
            error!(?self, "unreachable branch in LeaseManager watcher, looks like a BUG!");
            Err(LeaseStateError::InconsistentState(
                "unreachable branch, looks like a BUG!".into(),
            ))
        }
    }

    async fn is_expired(&self) -> bool {
        self.state.read().await.is_expired()
    }

    async fn is_locked(&self) -> bool {
        self.state.read().await.is_locked()
    }

    async fn is_holder(&self) -> bool {
        self.state.read().await.is_holder(&self.params.identity)
    }

    async fn expiry(&self) -> SystemTime {
        self.state.read().await.expiry
    }

    async fn holder(&self) -> Option<String> {
        self.state.read().await.holder.clone()
    }

    fn grace_sleep_duration(&self, expiry: SystemTime, grace: DurationSeconds) -> Duration {
        let grace = Duration::from_secs(grace);
        expiry
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
            .saturating_sub(grace)
    }
}

fn random_duration(min_millis: DurationMillis, max_millis: DurationMillis) -> Duration {
    Duration::from_millis(thread_rng().gen_range(min_millis..max_millis))
}

fn random_string(len: usize) -> String {
    let rand: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    rand
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::select_all;
    use k8s_openapi::api::{coordination::v1::Lease, core::v1::Namespace};
    use kube::{
        api::{DeleteParams, PostParams},
        Api, Resource as _,
    };
    use std::{collections::HashSet, sync::OnceLock};
    use tokio::{join, select, sync::OnceCell};

    pub(crate) const TEST_NAMESPACE: &str = "kube-lease-test";

    static INITIALIZED: OnceCell<bool> = OnceCell::const_new();
    static TRACING: OnceLock<()> = OnceLock::new();

    pub(crate) async fn init() -> Client {
        let client = Client::try_default().await.unwrap();
        INITIALIZED
            .get_or_init(|| async {
                create_namespace(client.clone()).await;
                init_tracing();
                true
            })
            .await;

        client
    }

    pub(crate) fn init_tracing() {
        TRACING.get_or_init(|| {
            tracing_subscriber::fmt::init();
        });
    }

    /// Unattended namespace creation
    async fn create_namespace(client: Client) {
        let api = Api::<Namespace>::all(client);
        let pp = PostParams::default();

        let mut data = Namespace::default();
        data.meta_mut().name = Some(String::from(TEST_NAMESPACE));

        api.create(&pp, &data).await.unwrap_or_default();
    }

    async fn setup_simple_managers_vec(lease_name: &str, count: usize) -> Vec<LeaseManager> {
        const LEASE_DURATION_SECONDS: DurationSeconds = 2;
        const LEASE_GRACE_SECONDS: DurationSeconds = 1;

        let client = init().await;
        let mut managers = vec![];

        for i in 0..count {
            let param = LeaseParams::new(format!("leader-{i}"), LEASE_DURATION_SECONDS, LEASE_GRACE_SECONDS);
            let manager = LeaseManager::new(
                client.clone(),
                lease_name,
                TEST_NAMESPACE,
                param,
                LeaseCreateMode::Ignore,
            )
            .await
            .unwrap();
            managers.push(manager);
        }

        // Create lease
        let _ = managers[0]
            .state
            .read()
            .await
            .create(LeaseCreateMode::CreateNew)
            .await
            .unwrap();
        managers
    }

    pub(crate) async fn sleep_secs(seconds: DurationSeconds) {
        tokio::time::sleep(Duration::from_secs(seconds)).await
    }

    #[test]
    fn lease_params_default_constructor() {
        let params = LeaseParams::default();
        assert_eq!(params.identity.len(), DEFAULT_RANDOM_IDENTITY_LEN);
        assert_eq!(params.duration, DEFAULT_LEASE_DURATION_SECONDS);
        assert_eq!(params.grace, DEFAULT_LEASE_GRACE_SECONDS);
    }

    #[test]
    #[should_panic = "duration and grace period should be greater than zero"]
    fn incorrect_lease_params_duration_0() {
        let _params = LeaseParams::new(random_string(10), 0, 0);
    }

    #[test]
    #[should_panic = "duration and grace period should be greater than zero"]
    fn incorrect_lease_params_grace_0() {
        let _params = LeaseParams::new(random_string(10), 2, 0);
    }

    #[test]
    #[should_panic = "grace period should be less than lease lock duration"]
    fn incorrect_lease_params_duration_equal_grace() {
        let _params = LeaseParams::new(random_string(10), 2, 2);
    }

    #[test]
    #[should_panic = "grace period should be less than lease lock duration"]
    fn incorrect_lease_params_duration_less_than_grace() {
        let _params = LeaseParams::new(random_string(10), 2, 3);
    }

    #[test]
    fn random_100_000_8ch_strings() {
        const SET_LEN: usize = 100_000;

        let mut set = HashSet::new();
        for _ in 0..SET_LEN {
            set.insert(random_string(8));
        }

        assert_eq!(set.len(), SET_LEN);
    }

    #[test]
    fn random_100_intervals() {
        const SET_LEN: usize = 100;

        let mut set = HashSet::new();
        for _ in 0..SET_LEN {
            set.insert(random_duration(MIN_RELEASE_WAITING_MILLIS, MAX_RELEASE_WAITING_MILLIS));
        }

        assert!(
            set.len() >= SET_LEN * 8 / 10,
            "at least 80% of randoms should be unique, but got {}%",
            set.len()
        );
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn grace_sleep_duration() {
        let client = Client::try_default().await.unwrap();
        let manager = LeaseManager::new(
            client,
            "lease_name",
            "namespace",
            LeaseParams::default(),
            LeaseCreateMode::Ignore,
        )
        .await
        .unwrap();

        // Already expired - always ZERO
        let expiry = SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap();
        manager.state.write().await.expiry = expiry;
        assert_eq!(
            manager.grace_sleep_duration(expiry, 0),
            Duration::ZERO,
            "should be ZERO since it's already expired"
        );
        assert_eq!(
            manager.grace_sleep_duration(expiry, 1),
            Duration::ZERO,
            "should be ZERO since it's already expired"
        );

        // Expires in 10s
        let expiry = SystemTime::now().checked_add(Duration::from_secs(10)).unwrap();
        manager.state.write().await.expiry = expiry;
        let duration = manager.grace_sleep_duration(expiry, 0);
        assert!(
            duration >= Duration::from_millis(9_900) && duration <= Duration::from_millis(10_000),
            "should be around 10s"
        );
        let duration = manager.grace_sleep_duration(expiry, 1);
        assert!(
            duration >= Duration::from_millis(8_900) && duration <= Duration::from_millis(9_000),
            "should be around 9s"
        );
        assert_eq!(
            manager.grace_sleep_duration(expiry, 10),
            Duration::ZERO,
            "should be around ZERO"
        );
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn single_manager_watcher_step() {
        const LEASE_NAME: &str = "single-manager-watcher-step-test";

        let managers = setup_simple_managers_vec(LEASE_NAME, 1).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(!managers[0].state.read().await.is_holder(&managers[0].params.identity));
        assert!(!managers[0].state.read().await.is_locked());

        managers[0].watcher_step().await.unwrap();
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(managers[0].state.read().await.is_holder(&managers[0].params.identity));
        assert!(managers[0].state.read().await.is_locked());
        assert!(!managers[0].state.read().await.is_expired());

        // Expire
        sleep_secs(managers[0].params.duration).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(managers[0].state.read().await.is_holder(&managers[0].params.identity));
        assert!(managers[0].state.read().await.is_locked());
        assert!(managers[0].state.read().await.is_expired());

        // Clean up
        managers[0].state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn single_manager_changed_loop() {
        const LEASE_NAME: &str = "single-manager-changed-loop-test";

        let managers = setup_simple_managers_vec(LEASE_NAME, 1).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));

        let is_leader = managers[0].changed().await.unwrap();
        assert!(is_leader);
        assert!(managers[0].is_leader.load(Ordering::Relaxed));

        let long_duration = Duration::from_secs(managers[0].params.duration * 2);
        select! {
             _ = managers[0].changed() => {
                unreachable!("unreachable branch since `changed` loop should last forever")
             },
             _ = tokio::time::sleep(long_duration) => {
                assert!(managers[0].is_leader.load(Ordering::Relaxed))
            }
        }
        assert!(managers[0].is_leader.load(Ordering::Relaxed));

        // Clean up
        managers[0].state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn two_managers_1st_expires_then_2nd_locks() {
        const LEASE_NAME: &str = "two-managers-1st-expires-then-2nd-locks-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 2).await;
        let manager0 = managers.pop().unwrap();
        let manager1 = managers.pop().unwrap();

        assert!(!manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Lock by 1st
        let is_leader = manager0.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Try to hold lock by 1st
        let long_duration = Duration::from_secs(manager0.params.duration * 2);
        select! {
            biased;
             _ = manager0.changed() => {
                unreachable!("unreachable branch since `changed` loop should last forever")
             },
             _ = manager1.changed() => {
                unreachable!("unreachable branch since `changed` loop should last forever")
             },
             _ = tokio::time::sleep(long_duration) => {
                assert!(manager0.is_leader.load(Ordering::Relaxed));
                assert!(!manager1.is_leader.load(Ordering::Relaxed));
            }
        }
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Expire and try to re-lock by 2nd
        sleep_secs(manager0.params.duration).await;
        let is_leader = manager1.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager1.is_leader.load(Ordering::Relaxed));

        let is_leader = manager0.changed().await.unwrap();
        assert!(!is_leader);
        assert!(!manager0.is_leader.load(Ordering::Relaxed));

        // Clean up
        manager0.state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn many_managers_1st_expires_then_someone_locks() {
        const LEASE_NAME: &str = "many-managers-1st-expires-then-someone-locks-test";
        const MANAGERS: usize = 100;

        let mut managers = setup_simple_managers_vec(LEASE_NAME, MANAGERS).await;

        for manager in &managers {
            assert!(!manager.is_leader.load(Ordering::Relaxed));
        }

        // Lock by 1st
        let is_leader = managers[0].changed().await.unwrap();
        assert!(is_leader);
        for (i, manager) in managers.iter().enumerate() {
            assert_eq!(
                i == 0,
                manager.is_leader.load(Ordering::Relaxed),
                "locked by incorrect manager"
            );
        }

        // Try to hold lock by 1st
        let long_duration = Duration::from_secs(managers[0].params.duration * 2);
        {
            let managers_fut: Vec<_> = managers
                .iter_mut()
                .map(|m| {
                    let c = async {
                        let r = m.changed().await;
                        tokio::time::sleep(random_duration(0, 500)).await;
                        r
                    };
                    Box::pin(c)
                })
                .collect();
            select! {
                _ = tokio::time::sleep(long_duration) => {},
                _ = select_all(managers_fut) => {
                    unreachable!("unreachable branch since `changed` loop should last forever")
                }
            }
            for (i, manager) in managers.iter().enumerate() {
                assert_eq!(i == 0, manager.is_leader.load(Ordering::Relaxed));
            }
        }
        // Expire it
        tokio::time::sleep(
            Duration::from_secs(managers[0].params.duration)
                .checked_add(Duration::from_millis(100))
                .unwrap(),
        )
        .await;
        // Try to re-lock by someone else (exclude 1st from loop)
        {
            let managers_fut: Vec<_> = managers
                .iter_mut()
                .skip(1)
                .map(|m| {
                    let c = async {
                        let r = m.changed().await;
                        tokio::time::sleep(random_duration(0, 500)).await;
                        r
                    };
                    Box::pin(c)
                })
                .collect();
            let (result, index, _) = select_all(managers_fut).await;
            assert!(result.unwrap());
            // Assert that only one holds lock
            for (i, manager) in managers.iter().skip(1).enumerate() {
                assert_eq!(
                    i == index,
                    manager.is_leader.load(Ordering::Relaxed),
                    "locked by incorrect manager"
                );
            }
            // Assert that 1st lost lock
            assert!(!managers[0].changed().await.unwrap());
        }

        // Clean up
        managers[0].state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn create_lease() {
        const LEASE_NAME: &str = "create-lease-test";

        let client = init().await;
        let dp = DeleteParams::default();
        let api = Api::<Lease>::namespaced(client.clone(), TEST_NAMESPACE);

        let _ = api.delete(LEASE_NAME, &dp).await;

        assert!(
            matches!(
                LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::UseExistent).await,
                Err(LeaseStateError::NonexistentLease(_))
            ),
            "lease exists but shouldn't"
        );

        assert!(
            LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::AutoCreate)
                .await
                .is_ok(),
            "lease wasn't created"
        );

        assert!(
            LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::UseExistent)
                .await
                .is_ok(),
            "lease wasn't created"
        );

        assert!(
            LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::AutoCreate)
                .await
                .is_ok(),
            "lease wasn't created"
        );

        assert!(
            matches!(
                LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::CreateNew).await,
                Err(LeaseStateError::LeaseAlreadyExists(_))
            ),
            "lease should exist"
        );

        api.delete(LEASE_NAME, &dp).await.unwrap();

        assert!(
            LeaseState::new(client.clone(), LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::CreateNew)
                .await
                .is_ok(),
            "lease shouldn't exist"
        );

        api.delete(LEASE_NAME, &dp).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn two_managers_1st_releases_then_2nd_locks() {
        const LEASE_NAME: &str = "two-managers-1st-releases-then-2nd-locks-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 2).await;
        let manager0 = managers.pop().unwrap();
        let manager1 = managers.pop().unwrap();

        assert!(!manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Lock by 1st
        let is_leader = manager0.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Try to hold lock by 1st
        let long_duration = Duration::from_secs(manager0.params.duration * 2);
        select! {
            biased;
             _ = manager0.changed() => {
                unreachable!("unreachable branch since `changed` loop should last forever")
             },
             _ = manager1.changed() => {
                unreachable!("unreachable branch since `changed` loop should last forever")
             },
             _ = tokio::time::sleep(long_duration) => {
                assert!(manager0.is_leader.load(Ordering::Relaxed));
                assert!(!manager1.is_leader.load(Ordering::Relaxed));
            }
        }
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Release and try to re-lock by 2nd
        manager0.release().await.unwrap();
        let is_leader = manager1.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager1.is_leader.load(Ordering::Relaxed));

        let is_leader = manager0.changed().await.unwrap();
        assert!(!is_leader);
        assert!(!manager0.is_leader.load(Ordering::Relaxed));

        // Clean up
        manager0.state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn single_watch_managers_handles_own_channel() {
        const LEASE_NAME: &str = "single-watch-managers-handle-channel-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 1).await;
        let manager0 = managers.pop().unwrap();

        assert!(!manager0.is_leader.load(Ordering::Relaxed));

        // Run watcher
        let (mut channel, handler) = manager0.watch().await;
        // It had no time to lock
        assert!(!*channel.borrow_and_update());

        // Wait to lock lease
        channel.changed().await.unwrap();
        assert!(*channel.borrow_and_update());

        // Try to hold lock for 3 seconds
        select! {
            _ = channel.changed() => {
                unreachable!("unreachable branch since lock state has not be changed")
            },
            _ = sleep_secs(3) => {
                assert!(*channel.borrow());
            }
        }
        assert!(*channel.borrow());

        // Close the control channel and expect released lock and finished watcher
        drop(channel);
        let manager0 = join!(handler).0.unwrap().unwrap();
        manager0.state.write().await.sync(LeaseLockOpts::Force).await.unwrap();
        assert!(!manager0.is_holder().await);

        // Clean up
        manager0.state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn two_managers_1st_uses_changed_2nd_watch() {
        const LEASE_NAME: &str = "two-managers-1st-uses-changed-2nd-watch-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 2).await;
        let manager0 = managers.remove(0);
        let manager1 = managers.remove(0);

        assert!(!manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Lock by 1st
        let is_leader = manager0.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        let (mut channel, handler) = manager1.watch().await;
        assert!(!*channel.borrow_and_update());

        // Hold lock by 1st for 3 seconds
        select! {
            _ = sleep_secs(3) => {
                assert!(manager0.is_leader.load(Ordering::Relaxed));
                assert!(!*channel.borrow_and_update());
            }
            _changed = manager0.changed() => {
                unreachable!("unreachable branch since lock state has not be changed");
            }
            _watch = channel.changed() => {
                unreachable!("unreachable branch since lock state has not be changed");
            }
        }
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!*channel.borrow_and_update());

        // Don't touch 1st to make it expired, 2nd has to lock it
        select! {
            _ = sleep_secs(4) => {
                unreachable!("unreachable branch since lock state has not be changed");

            }
            _watch = channel.changed() => {
                assert!(*channel.borrow_and_update());
                manager0.changed().await.unwrap();
                assert!(!manager0.is_leader.load(Ordering::Relaxed));
            }
        }

        // Drop channel to release 2nd and wait for 1st is changed
        drop(channel);
        assert!(manager0.changed().await.unwrap());
        // And 2nd has to finish
        let res = join!(handler).0.unwrap().unwrap();
        assert!(!res.is_holder().await);

        // Clean up
        manager0.state.read().await.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn many_managers_watch_one_by_one() {
        const LEASE_NAME: &str = "many-managers-watch-one-by-one-test";
        const NUMBER_OF_MANAGERS: usize = 10;

        let managers = setup_simple_managers_vec(LEASE_NAME, NUMBER_OF_MANAGERS).await;

        // ensure there is no holders
        assert_eq!(
            managers.iter().filter(|m| m.is_leader.load(Ordering::Relaxed)).count(),
            0
        );

        // run watchers
        let mut handlers = vec![];
        let mut channels = vec![];
        for m in managers {
            let w = m.watch().await;
            channels.push(w.0);
            handlers.push(w.1);
        }
        // wait for at least one locked lease
        // assert that at least one watcher get changed, and only one holds lock
        sleep_secs(3).await;
        let changed_vec: Vec<_> = channels.iter_mut().map(|ch| Box::pin(ch.changed())).collect();
        let (_result, index, _) = select_all(changed_vec).await;
        assert!(*channels[index].borrow_and_update());
        assert_eq!(
            channels
                .iter_mut()
                .map(|ch| *ch.borrow_and_update())
                .filter(|r| *r)
                .count(),
            1
        );

        // drop watchers one by one and assert that single lock only
        let mut prev_index = index;
        let mut managers = vec![];
        while !channels.is_empty() {
            let prev_channel = channels.remove(prev_index);
            let prev_handler = handlers.remove(prev_index);
            drop(prev_channel);
            let manager = join!(prev_handler).0.unwrap().unwrap();
            managers.push(manager);

            if !channels.is_empty() {
                // wait for new lock
                let changed_vec: Vec<_> = channels.iter_mut().map(|ch| Box::pin(ch.changed())).collect();
                let (_result, index, _) = select_all(changed_vec).await;
                assert!(*channels[index].borrow_and_update());
                assert_eq!(
                    channels
                        .iter_mut()
                        .map(|ch| *ch.borrow_and_update())
                        .filter(|r| *r)
                        .count(),
                    1
                );

                prev_index = index;
            }
        }

        // assert the expected number of the lease transitions
        assert_eq!(
            managers[0]
                .state
                .read()
                .await
                .get()
                .await
                .unwrap()
                .spec
                .unwrap()
                .lease_transitions
                .unwrap(),
            NUMBER_OF_MANAGERS as i32
        );

        // Clean up
        managers[0].state.read().await.delete().await.unwrap();
    }

    #[test]
    fn lease_params_constructor_with_field_manager() {
        const CUSTOM_FIELD_MANAGER: &str = "custom-field-manager";

        let fm = String::from(CUSTOM_FIELD_MANAGER);

        let params = LeaseParams::default();
        assert_ne!(params.field_manager(), CUSTOM_FIELD_MANAGER);

        let params = params.with_field_manager(fm);
        assert_eq!(params.field_manager(), CUSTOM_FIELD_MANAGER);
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn lease_manager_builder() {
        const LEASE_NAME0: &str = "lease-manager-builder-test-0";
        const LEASE_NAME: &str = "lease-manager-builder-test";
        const CUSTOM_FIELD_MANAGER0: &str = "custom-field-manager-0";
        const CUSTOM_FIELD_MANAGER: &str = "custom-field-manager";
        const CUSTOM_IDENTITY0: &str = "custom-lease-manager-identity-0";
        const CUSTOM_IDENTITY: &str = "custom-lease-manager-identity";

        let client = init().await;
        let builder = LeaseManagerBuilder::new(client, LEASE_NAME0);

        assert_eq!(builder.lease_name, LEASE_NAME0);
        let builder = builder.with_lease_name(LEASE_NAME);
        assert_eq!(builder.lease_name, LEASE_NAME);

        assert_eq!(builder.namespace, "default");
        let builder = builder.with_namespace(TEST_NAMESPACE);
        assert_eq!(builder.namespace, TEST_NAMESPACE);

        assert_eq!(builder.create_mode, LeaseCreateMode::default());
        assert_ne!(LeaseCreateMode::default(), LeaseCreateMode::CreateNew);
        let builder = builder.with_create_mode(LeaseCreateMode::CreateNew);
        assert_eq!(builder.create_mode, LeaseCreateMode::CreateNew);

        assert_ne!(builder.params.identity, CUSTOM_IDENTITY0);
        let builder = builder.with_identity(CUSTOM_IDENTITY0);
        assert_eq!(builder.params.identity, CUSTOM_IDENTITY0);

        assert_ne!(builder.params.field_manager, CUSTOM_FIELD_MANAGER0);
        let builder = builder.with_field_manager(CUSTOM_FIELD_MANAGER0);
        assert_eq!(builder.params.field_manager(), CUSTOM_FIELD_MANAGER0);

        assert_ne!(builder.params.duration, 111);
        assert_eq!(builder.params.duration, LeaseParams::default().duration);
        let builder = builder.with_duration(111);
        assert_eq!(builder.params.duration, 111);

        assert_ne!(builder.params.grace, 11);
        assert_eq!(builder.params.grace, LeaseParams::default().grace);
        let builder = builder.with_grace(11);
        assert_eq!(builder.params.grace, 11);

        let params = LeaseParams {
            identity: CUSTOM_IDENTITY.into(),
            duration: 222,
            grace: 22,
            field_manager: CUSTOM_FIELD_MANAGER.into(),
        };
        let builder = builder.with_parameters(params);
        assert_eq!(builder.params.identity, CUSTOM_IDENTITY);
        assert_eq!(builder.params.duration, 222);
        assert_eq!(builder.params.grace, 22);
        assert_eq!(builder.params.field_manager(), CUSTOM_FIELD_MANAGER);

        let manager = builder.build().await.unwrap();
        manager.changed().await.unwrap();

        // Clean up
        manager.state.read().await.delete().await.unwrap();
    }
}
