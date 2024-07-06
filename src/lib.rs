#![deny(unsafe_code)]

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
use tracing::{debug, error};

type DurationMillis = u64;

/// Since all Durations related to Lease resource are in seconds, this alias is useful.
pub type DurationSeconds = u64;
/// Convenient alias for `Result`.
pub type Result<T, E = LeaseManagerError> = std::result::Result<T, E>;

pub const DEFAULT_LEASE_DURATION_SECONDS: DurationSeconds = 30;
pub const DEFAULT_LEASE_GRACE_SECONDS: DurationSeconds = 5;

const DEFAULT_RANDOM_IDENTITY_LEN: usize = 32;
const DEFAULT_FIELD_MANAGER_PREFIX: &str = env!("CARGO_PKG_NAME");

const MIN_RELEASE_WAITING_MILLIS: DurationMillis = 100;
const MAX_RELEASE_WAITING_MILLIS: DurationMillis = 1000;

const MIN_CONFLICT_BACKOFF_TIME: DurationFloat = 0.1;
const MAX_CONFLICT_BACKOFF_TIME: DurationFloat = 5.0;
const CONFLICT_BACKOFF_MULT: DurationFloat = 2.0;

const MIN_WATCHER_BACKOFF_TIME: DurationFloat = 1.0;
const MAX_WATCHER_BACKOFF_TIME: DurationFloat = 30.0;
const WATCHER_BACKOFF_MULT: DurationFloat = 2.0;

/// Represents `kube-lease` specific errors.
#[derive(thiserror::Error, Debug)]
pub enum LeaseManagerError {
    /// Error originated from the Kubernetes.
    #[error("Kube error: {0}")]
    KubeError(
        #[source]
        #[from]
        kube::Error,
    ),

    /// Internal lease state inconsistency detected.
    ///
    /// Usually root cause is a bug.
    #[error("inconsistent LeaseManager state detected: {0}")]
    InconsistentState(String),

    /// Try to create Lease but it already exists.
    #[error("Lease resource `{0}` already exists")]
    LeaseAlreadyExists(String),

    /// Try to use non-existent Lease resource.
    #[error("Lease resource `{0}` doesn't exist")]
    NonexistentLease(String),

    /// LeaseManager unable to send state to the control channel
    #[error("Control channel error: {0}")]
    ControlCannelError(
        #[source]
        #[from]
        tokio::sync::watch::error::SendError<bool>,
    ),
}

/// Parameters of LeaseManager.
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

/// Lease resource creation mode.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum LeaseCreateMode {
    #[default]
    AutoCreate,
    CreateNew,
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

/// Lease lock manager.
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
    /// Lease lock parameters constructor.
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

    /// Use specified field manager value instead of the default one.
    pub fn with_field_manager(self, field_manager: String) -> Self {
        Self { field_manager, ..self }
    }

    fn field_manager(&self) -> String {
        self.field_manager.clone()
    }
}

impl Default for LeaseParams {
    /// Creates parameters instance with reasonable defaults:
    ///   - random alpha-numeric identity string;
    ///   - lease duration is [`DEFAULT_LEASE_DURATION_SECONDS`];
    ///   - lease grace period is [`DEFAULT_LEASE_GRACE_SECONDS`].
    fn default() -> Self {
        Self::new(
            random_string(DEFAULT_RANDOM_IDENTITY_LEN),
            DEFAULT_LEASE_DURATION_SECONDS,
            DEFAULT_LEASE_GRACE_SECONDS,
        )
    }
}

/// Builder of [`LeaseManager`].
///
/// It facilitates creating of the manager instance with reasonable defaults.
pub struct LeaseManagerBuilder {
    client: Client,
    lease_name: String,
    namespace: String,
    params: LeaseParams,
    create_mode: LeaseCreateMode,
}

impl LeaseManagerBuilder {
    /// Constructs minimal builder instance with all other parameters set to default values.
    pub fn new(client: Client, lease_name: impl Into<String>) -> Self {
        Self {
            client,
            lease_name: lease_name.into(),
            namespace: String::from("default"),
            params: LeaseParams::default(),
            create_mode: LeaseCreateMode::default(),
        }
    }

    /// Builds [`LeaseManager`] from the builder instance.
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

    /// Change current Lease name instead of the one provided to [`LeaseManagerBuilder::new()`].
    pub fn with_lease_name(self, lease_name: impl Into<String>) -> Self {
        Self {
            lease_name: lease_name.into(),
            ..self
        }
    }

    /// Updates namespace to create Lease resource onto.
    pub fn with_namespace(self, namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ..self
        }
    }

    pub fn with_create_mode(self, create_mode: LeaseCreateMode) -> Self {
        Self { create_mode, ..self }
    }

    /// Update whole LeaseParameters instance.
    ///
    /// There four additional methods to set each parameters' value individually.
    pub fn with_parameters(self, params: LeaseParams) -> Self {
        Self { params, ..self }
    }

    /// Updates parameters field manager value.
    pub fn with_field_manager(self, field_manager: impl Into<String>) -> Self {
        Self {
            params: self.params.with_field_manager(field_manager.into()),
            ..self
        }
    }

    /// Updates Lease duration parameter.
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
    pub fn with_grace(self, grace: DurationSeconds) -> Self {
        Self {
            params: LeaseParams { grace, ..self.params },
            ..self
        }
    }

    /// Updates lease identity value.
    pub fn with_identity(self, identity: impl Into<String>) -> Self {
        Self {
            params: LeaseParams {
                identity: identity.into(),
                ..self.params
            },
            ..self
        }
    }
}

impl LeaseManager {
    /// Constructor
    pub async fn new(
        client: Client,
        lease_name: &str,
        namespace: &str,
        params: LeaseParams,
        create_mode: LeaseCreateMode,
    ) -> Result<Self> {
        Ok(Self {
            params,
            state: RwLock::new(LeaseState::new(client, lease_name, namespace, create_mode).await?),
            is_leader: AtomicBool::new(false),
        })
    }

    /// Spawns Tokio task and watch on leader changes permanently.
    /// If self state changes (became a leader or lost the lock) it sends actual lock state to the channel.
    /// Exits if channel is closed (all receivers are gone) or in case of any sending error.
    ///
    pub async fn watch(self) -> (tokio::sync::watch::Receiver<bool>, JoinHandle<Result<LeaseManager>>) {
        let (sender, receiver) = tokio::sync::watch::channel(self.is_leader.load(Ordering::Relaxed));
        let watcher = async move {
            debug!("starting watch loop");

            let mut backoff =
                BackoffSleep::new(MIN_WATCHER_BACKOFF_TIME, MAX_WATCHER_BACKOFF_TIME, WATCHER_BACKOFF_MULT);

            loop {
                select! {
                    biased;
                    _ = sender.closed() => {
                        // Consumer closed all receivers - release lock and exit
                        debug!("control channel has been closed");
                        let result = self.release().await;
                        return match result {
                            Ok(_) => Ok(self),
                            Err(err) => Err(err),
                        };
                    }
                    result = self.changed() => {
                        match result {
                            Ok(state) => {
                                let result = sender.send(state);
                                match result {
                                    Ok(_) => backoff.reset(),
                                    Err(err) => {
                                        let _ = self.release().await;
                                        return Err(LeaseManagerError::ControlCannelError(err));
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

    /// Try to lock lease and renew it periodically to prevent expiration.
    ///
    /// Returns self leader lock status as soon as it was changed (acquired or released).
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

            // Make single iteration, if no changes so far
            match self.watcher_step().await {
                Ok(_) => {
                    // reset backoff and continue
                    backoff.reset();
                }
                Err(LeaseStateError::LockConflict) => {
                    // Wait for backoff interval and continue
                    backoff.sleep().await;
                }
                Err(err) => return Err(LeaseManagerError::from(err)),
            }
        }
    }

    /// Release self-lock if it's set, or do nothing if lease is locked by other identity.
    ///
    /// It's useful to call this method to free locked lease gracefully before exit/shutdown if you use [`LeaseManager::changed`] directly instead of the using [`LeaseManager::watch`].
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
            // if we're holder of the lock - sleep up to the next refresh time,
            // and renew lock (lock it softly)
            tokio::time::sleep(self.grace_sleep_duration(self.expiry().await, self.params.grace)).await;

            debug!(identity = %self.params.identity, "renew own lease lock");
            self.state.write().await.lock(&self.params, LeaseLockOpts::Soft).await
        } else if !self.is_locked().await {
            // Lease isn't locket yet
            debug!(identity = %self.params.identity, "try to lock lease");
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

            // Sleep some random time (up to 500ms) to minimize collisions probability
            tokio::time::sleep(random_duration(MIN_RELEASE_WAITING_MILLIS, MAX_RELEASE_WAITING_MILLIS)).await;
            res
        } else if self.is_locked().await && !self.is_expired().await {
            // It's locked by someone else and lock is actual.
            // Sleep up to the expiration time of the lock.
            let holder = self.holder().await.unwrap();
            debug!(identity = %self.params.identity, %holder,"lease is actually locked by other identity");
            tokio::time::sleep(self.grace_sleep_duration(self.expiry().await, 0)).await;
            Ok(())
        } else {
            // Something wrong happened
            error!(?self, "unreachable branch, looks like a BUG!");
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

        // Close control channel and expect released lock and finished watcher
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
        const NUMBER_OF_MANAGERS: usize = 100;

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
        // assert that at least one watcher get changed and only one holds lock
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

        // assert expected number of the lease transitions
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
