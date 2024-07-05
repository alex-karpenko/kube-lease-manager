#![deny(unsafe_code)]

mod backoff;
mod state;

use backoff::{BackoffSleep, DurationFloat};
use kube::Client;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use state::{LeaseLockOpts, LeaseState};
use std::{
    fmt::Debug,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};
use tokio::{sync::RwLock, task::JoinHandle};
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

    /// Conflict detected during locking attempt.
    #[error("lock conflict detected")]
    LockConflict,

    /// Internal lease state inconsistency detected.
    ///
    /// Usually root cause is a bug.
    #[error("inconsistent kube-lease state detected: {0}")]
    InconsistentState(String),

    /// Try to create Lease but it already exists.
    #[error("Lease resource `{0}` already exists")]
    LeaseAlreadyExists(String),

    /// Try to use non-existent Lease resource.
    #[error("Lease resource `{0}` doesn't exist")]
    NonexistentLease(String),
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

/// Lease lock manager.
#[derive(Debug)]
pub struct LeaseManager {
    /// Parameters of the desired lock.
    params: LeaseParams,
    /// Current state.
    state: RwLock<LeaseState>,
    /// Is current identity marked as leader now
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

        Self {
            identity: identity.into(),
            duration,
            grace,
        }
    }

    fn field_manager(&self) -> String {
        format!("{DEFAULT_FIELD_MANAGER_PREFIX}-{}", self.identity)
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

    pub async fn watch(self) -> (tokio::sync::watch::Receiver<bool>, JoinHandle<()>) {
        let (sender, receiver) = tokio::sync::watch::channel(self.is_leader.load(Ordering::Relaxed));
        let watcher = async move {
            let mut backoff =
                BackoffSleep::new(MIN_WATCHER_BACKOFF_TIME, MAX_WATCHER_BACKOFF_TIME, WATCHER_BACKOFF_MULT);
            loop {
                if sender.is_closed() {
                    return;
                }
                match self.changed().await {
                    Ok(state) => {
                        let result = sender.send(state);
                        if result.is_err() {
                            return;
                        }
                        backoff.reset();
                    }
                    Err(e) => {
                        error!(error = %e, "LeaseManager watcher error");
                        if sender.is_closed() {
                            return;
                        } else {
                            backoff.sleep().await;
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
                Err(LeaseManagerError::LockConflict) => {
                    // Wait for backoff interval and continue
                    backoff.sleep().await;
                }
                Err(err) => return Err(err),
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
    }

    async fn watcher_step(&self) -> Result<()> {
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
            Err(LeaseManagerError::InconsistentState(
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
    use tokio::{select, sync::OnceCell};

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
                Err(LeaseManagerError::NonexistentLease(_))
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
                Err(LeaseManagerError::LeaseAlreadyExists(_))
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
}
