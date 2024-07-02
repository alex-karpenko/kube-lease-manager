#![deny(unsafe_code)]

use k8s_openapi::{
    api::coordination::v1::Lease,
    apimachinery::pkg::apis::meta::v1::MicroTime,
    chrono::{DateTime, Utc},
    serde::Serialize,
    serde_json,
};
use kube::{
    api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams},
    Api, Client,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    fmt::Debug,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
};
use tracing::{debug, error};

/// Convenient alias for `Result`.
pub type Result<T, E = LeaseError> = std::result::Result<T, E>;

pub const DEFAULT_LEASE_DURATION_SECONDS: u64 = 30;
pub const DEFAULT_LEASE_GRACE_SECONDS: u64 = 5;

const DEFAULT_RANDOM_IDENTITY_LEN: usize = 32;
const DEFAULT_FIELD_MANAGER_PREFIX: &str = "kube-lease-";

const DEFAULT_MIN_RANDOM_RELEASE_WAITING_MILLIS: u64 = 100;
const DEFAULT_MAX_RANDOM_RELEASE_WAITING_MILLIS: u64 = 1000;

/// Represents `kube-lease` specific errors.
#[derive(thiserror::Error, Debug)]
pub enum LeaseError {
    #[error("Kube error: {0}")]
    KubeError(
        #[source]
        #[from]
        kube::Error,
    ),

    #[error("lock conflict detected")]
    LockConflict,

    #[error("inconsistent kube-lease state detected: {0}")]
    InconsistentState(String),
}

/// Parameters of LeaseManager.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseParams {
    /// Lease holder identity.
    identity: String,
    /// Duration of lease lock.
    duration: Duration,
    /// Period of tme to renew lease lock before it expires.
    grace: Duration,
}

/// Represents actual Lease state.
#[derive(Debug, Clone)]
struct LeaseState {
    api: Api<Lease>,
    /// Name of the Lease object.
    lease_name: String,
    /// Identity of the current Lease holder, if it's locket now.
    holder: Option<String>,
    /// Time of the potential state expiration.
    expiry: SystemTime,
    /// Transitions count.
    transitions: i32,
}

/// Lease lock manager.
#[derive(Debug)]
pub struct LeaseManager {
    /// Parameters of the desired lock.
    params: LeaseParams,
    /// Current state.
    state: LeaseState,
    /// Is current identity marked as leader now
    is_leader: AtomicBool,
}

/// Options to use for operations with lock.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
enum LeaseLockOpts {
    #[default]
    Soft,
    Force,
}

impl Default for LeaseParams {
    fn default() -> Self {
        Self::new(
            random_string(DEFAULT_RANDOM_IDENTITY_LEN),
            Duration::from_secs(DEFAULT_LEASE_DURATION_SECONDS),
            Duration::from_secs(DEFAULT_LEASE_GRACE_SECONDS),
        )
    }
}

impl LeaseParams {
    /// Lease lock parameters constructor.
    pub fn new(identity: impl Into<String>, duration: impl Into<Duration>, grace: impl Into<Duration>) -> Self {
        let duration: Duration = duration.into();
        let grace: Duration = grace.into();

        if duration == Duration::ZERO || grace == Duration::ZERO {
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
        format!("{DEFAULT_FIELD_MANAGER_PREFIX}{}", self.identity)
    }
}

impl LeaseState {
    fn new(client: Client, lease_name: impl Into<String>, namespace: &str) -> Self {
        let api = Api::<Lease>::namespaced(client, namespace);

        Self {
            api,
            lease_name: lease_name.into(),
            holder: None,
            expiry: SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap(),
            transitions: 0,
        }
    }

    /// Check if current state is still valid.
    fn is_expired(&self) -> bool {
        SystemTime::now() > self.expiry
    }

    /// Check if current holder is the same as `other` parameter.
    fn is_holder(&self, other: &str) -> bool {
        if let Some(holder) = &self.holder {
            holder == other
        } else {
            false
        }
    }

    /// Check if current holder is set to something.
    fn is_locked(&self) -> bool {
        self.holder.is_some()
    }

    /// Retrieve actual state from the cluster.
    async fn sync(&mut self, opts: LeaseLockOpts) -> Result<()> {
        if opts == LeaseLockOpts::Force || self.is_expired() {
            let result = self.api.get(&self.lease_name).await;

            // If Lease doesn't exist - clear state before exiting
            if let Err(kube::Error::Api(err)) = &result {
                if err.code == 404 {
                    self.holder = None;
                    self.transitions = 0;
                    self.expiry = SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap();

                    return Err(LeaseError::from(result.err().unwrap()));
                }
            }

            // If success or non-404 - try to unwrap spec and do sync
            let result = result?;
            if let Some(lease) = result.spec {
                self.holder = lease.holder_identity;
                self.transitions = lease.lease_transitions.unwrap_or(0);
                self.expiry = {
                    let renew = lease.renew_time;
                    let duration = lease.lease_duration_seconds.map(|d| Duration::from_secs(d as u64));

                    if renew.is_some() && duration.is_some() {
                        let renew: SystemTime = renew.unwrap().0.into();
                        let duration: Duration = duration.unwrap();

                        renew + duration
                    } else {
                        SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap()
                    }
                };
            } else {
                // Empty spec in the Lease
                self.holder = None;
                self.transitions = 0;
                self.expiry = SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap();
            }
        }

        Ok(())
    }

    async fn lock(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await?;

        let lease_duration_seconds = params.duration.as_secs();
        let now: DateTime<Utc> = SystemTime::now().into();

        // if we're holder - just refresh lease
        let patch = if self.is_holder(&params.identity) {
            // if we're holder - just refresh lease
            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "spec": {
                    "renewTime": MicroTime(now),
                    "leaseDurationSeconds": lease_duration_seconds,
                },
            });
            Patch::Strategic(patch)
        } else if !self.is_locked() {
            // if lock is orphan - try to lock it softly
            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "spec": {
                    "acquireTime": MicroTime(now),
                    "renewTime": MicroTime(now),
                    "holderIdentity": params.identity,
                    "leaseDurationSeconds": lease_duration_seconds,
                    "leaseTransitions": self.transitions + 1,
                },
            });
            Patch::Apply(patch)
        } else if opts == LeaseLockOpts::Force {
            // if it's locked by someone else but force is requested - try to lock it with force
            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "spec": {
                    "acquireTime": MicroTime(now),
                    "renewTime": MicroTime(now),
                    "holderIdentity": params.identity,
                    "leaseDurationSeconds": lease_duration_seconds,
                    "leaseTransitions": self.transitions + 1,
                },
            });
            Patch::Strategic(patch)
        } else {
            return Ok(());
        };

        self.patch(params, &patch).await?;
        self.sync(LeaseLockOpts::Force).await
    }

    async fn release(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await?;

        if self.is_holder(&params.identity) || self.is_expired() || opts == LeaseLockOpts::Force {
            debug!(?params, ?opts, "release lock");

            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "spec": {
                    "acquireTime": Option::<()>::None,
                    "renewTime": Option::<()>::None,
                    "holderIdentity": Option::<()>::None,
                    "leaseDurationSeconds": Option::<()>::None,
                }
            });

            let patch = Patch::Strategic(patch);
            self.patch(params, &patch).await?;
        }

        self.sync(LeaseLockOpts::Force).await
    }

    async fn patch<P>(&self, params: &LeaseParams, patch: &Patch<P>) -> Result<()>
    where
        P: Serialize + Debug,
    {
        debug!(?patch);

        let params = PatchParams {
            field_manager: Some(params.field_manager()),
            force: matches!(patch, Patch::Apply(_)),
            ..Default::default()
        };

        match self.api.patch(&self.lease_name, &params, patch).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.reason == "Conflict" && err.code == 409 => Err(LeaseError::LockConflict),
            Err(err) => Err(LeaseError::KubeError(err)),
        }
    }

    #[allow(dead_code)]
    async fn create(&self) -> Result<Lease> {
        let pp = PostParams::default();
        let data = Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                ..Default::default()
            },
            spec: Default::default(),
        };

        Ok(self.api.create(&pp, &data).await?)
    }

    #[allow(dead_code)]
    async fn delete(&self) -> Result<()> {
        let dp = DeleteParams::default();
        let _ = self.api.delete(&self.lease_name, &dp).await?;

        Ok(())
    }
}

impl LeaseManager {
    pub fn new(client: Client, lease_name: &str, namespace: &str, params: LeaseParams) -> Self {
        Self {
            params,
            state: LeaseState::new(client, lease_name, namespace),
            is_leader: AtomicBool::new(false),
        }
    }

    /// Try to lock lease and renew it periodically. Returns leader status as soon as it was changed.
    pub async fn changed(&mut self) -> Result<bool> {
        loop {
            // Is leader changed after this iteration?
            let is_holder = self.state.is_holder(&self.params.identity);
            if self.is_leader.load(Ordering::Acquire) != is_holder {
                debug!(identity = %self.params.identity, is_leader = is_holder, "lease lock state has been changed");
                self.is_leader.store(is_holder, Ordering::Release);

                return Ok(is_holder);
            }

            // Make single iteration, if no changes so far
            self.watcher_step().await?;
        }
    }

    async fn watcher_step(&mut self) -> Result<()> {
        // re-sync state if needed
        self.state.sync(LeaseLockOpts::Soft).await?;

        if self.state.is_holder(&self.params.identity) {
            // if we're holder of the lock - sleep up to the next refresh time,
            // and renew lock (lock it softly)
            tokio::time::sleep(self.grace_sleep_duration(self.params.grace)).await;

            debug!(identity = %self.params.identity, "renew own lease lock");
            self.state.lock(&self.params, LeaseLockOpts::Soft).await
        } else if !self.state.is_locked() {
            // Lease isn't locket yet
            debug!(identity = %self.params.identity, "try to lock lease");
            self.state.lock(&self.params, LeaseLockOpts::Soft).await
        } else if self.state.is_locked() && self.state.is_expired() {
            // It's locked by someone else but lock is already expired.
            // Release it by force and try to lock on the next loop cycle
            debug!(identity = %self.params.identity, "release expired lease lock");
            let res = self.state.release(&self.params, LeaseLockOpts::Force).await;

            // Sleep some random time (up to 500ms) to minimize collisions probability
            tokio::time::sleep(random_duration(
                DEFAULT_MIN_RANDOM_RELEASE_WAITING_MILLIS,
                DEFAULT_MAX_RANDOM_RELEASE_WAITING_MILLIS,
            ))
            .await;
            res
        } else if self.state.is_locked() && !self.state.is_expired() {
            // It's locked by someone else and lock is actual.
            // Sleep up to the expiration time of the lock.
            debug!(
                identity = %self.params.identity, holder = self.state.holder.as_ref().unwrap(),
                "lease is actually locked by other identity"
            );
            tokio::time::sleep(self.grace_sleep_duration(Duration::ZERO)).await;
            Ok(())
        } else {
            // Something wrong happened
            error!(?self, "unreachable branch, looks like a BUG!");
            Err(LeaseError::InconsistentState(
                "unreachable branch, looks like a BUG!".into(),
            ))
        }
    }

    fn grace_sleep_duration(&self, grace: Duration) -> Duration {
        self.state
            .expiry
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
            .saturating_sub(grace)
    }
}

fn random_duration(min_millis: u64, max_millis: u64) -> Duration {
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
    use k8s_openapi::api::{coordination::v1::LeaseSpec, core::v1::Namespace};
    use kube::Resource as _;
    use std::collections::HashSet;
    use tokio::{select, sync::OnceCell};

    const TEST_NAMESPACE: &str = "kube-lease-test";

    static INITIALIZED: OnceCell<bool> = OnceCell::const_new();

    async fn init() -> Client {
        let client = Client::try_default().await.unwrap();
        INITIALIZED
            .get_or_init(|| async {
                create_namespace(client.clone()).await;
                tracing_subscriber::fmt::init();
                true
            })
            .await;

        client
    }

    /// Unattended namespace creation
    async fn create_namespace(client: Client) {
        let api = Api::<Namespace>::all(client);
        let pp = PostParams::default();

        let mut data = Namespace::default();
        data.meta_mut().name = Some(String::from(TEST_NAMESPACE));

        api.create(&pp, &data).await.unwrap_or_default();
    }

    async fn setup_simple_leaders_vec(lease_name: &str, count: usize) -> (Vec<LeaseParams>, Vec<LeaseState>) {
        const LEASE_DURATION_SECONDS: u64 = 2;
        const LEASE_GRACE_SECONDS: u64 = 1;

        let client = init().await;
        let mut params = vec![];
        let mut states = vec![];

        for i in 0..count {
            let param = LeaseParams::new(
                format!("leader-{i}"),
                Duration::from_secs(LEASE_DURATION_SECONDS),
                Duration::from_secs(LEASE_GRACE_SECONDS),
            );
            let state = LeaseState::new(client.clone(), lease_name, TEST_NAMESPACE);

            params.push(param);
            states.push(state);
        }

        // Create lease
        let _ = states[0].create().await.unwrap();

        (params, states)
    }

    async fn setup_simple_managers_vec(lease_name: &str, count: usize) -> Vec<LeaseManager> {
        const LEASE_DURATION_SECONDS: u64 = 2;
        const LEASE_GRACE_SECONDS: u64 = 1;

        let client = init().await;
        let mut managers = vec![];

        for i in 0..count {
            let param = LeaseParams::new(
                format!("leader-{i}"),
                Duration::from_secs(LEASE_DURATION_SECONDS),
                Duration::from_secs(LEASE_GRACE_SECONDS),
            );
            let manager = LeaseManager::new(client.clone(), lease_name, TEST_NAMESPACE, param);
            managers.push(manager);
        }

        // Create lease
        let _ = managers[0].state.create().await.unwrap();
        managers
    }

    #[test]
    fn lease_params_default_constructor() {
        let params = LeaseParams::default();
        assert_eq!(params.identity.len(), DEFAULT_RANDOM_IDENTITY_LEN);
        assert_eq!(params.duration.as_secs(), DEFAULT_LEASE_DURATION_SECONDS);
        assert_eq!(params.grace.as_secs(), DEFAULT_LEASE_GRACE_SECONDS);
    }

    #[test]
    #[should_panic = "duration and grace period should be greater than zero"]
    fn incorrect_lease_params_duration_0() {
        let _params = LeaseParams::new(random_string(10), Duration::from_secs(0), Duration::from_secs(0));
    }

    #[test]
    #[should_panic = "duration and grace period should be greater than zero"]
    fn incorrect_lease_params_grace_0() {
        let _params = LeaseParams::new(random_string(10), Duration::from_secs(2), Duration::from_secs(0));
    }

    #[test]
    #[should_panic = "grace period should be less than lease lock duration"]
    fn incorrect_lease_params_duration_equal_grace() {
        let _params = LeaseParams::new(random_string(10), Duration::from_secs(2), Duration::from_secs(2));
    }

    #[test]
    #[should_panic = "grace period should be less than lease lock duration"]
    fn incorrect_lease_params_duration_less_than_grace() {
        let _params = LeaseParams::new(random_string(10), Duration::from_secs(2), Duration::from_secs(3));
    }

    #[tokio::test]
    async fn create_delete() {
        const LEASE_NAME: &str = "create-delete-test";

        let client = init().await;
        let state = LeaseState::new(client, LEASE_NAME, TEST_NAMESPACE);

        let lease = state.create().await.unwrap();
        assert!(lease.spec.is_some());
        assert_eq!(lease.spec.unwrap(), LeaseSpec::default());

        state.delete().await.unwrap();
    }

    #[tokio::test]
    async fn simple_soft_lock_soft_release() {
        const LEASE_NAME: &str = "simple-soft-lock-soft-release-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 1).await;

        // Lock
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        // Expire
        tokio::time::sleep(params[0].duration).await;
        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(states[0].is_expired());

        // Release
        states[0].release(&params[0], LeaseLockOpts::Soft).await.unwrap();
        assert!(!states[0].is_locked());
        assert!(!states[0].is_holder(&params[0].identity));
        assert!(states[0].is_expired());

        states[0].delete().await.unwrap();
    }

    #[tokio::test]
    async fn soft_lock_1st_soft_release_2nd() {
        const LEASE_NAME: &str = "soft-lock-1st-soft-release-2nd-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock by 1st
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Try to release by 2nd, unsuccessfully
        states[1].release(&params[1], LeaseLockOpts::Soft).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Expire
        tokio::time::sleep(params[0].duration).await;
        assert!(states[0].is_expired());
        assert!(states[1].is_expired());

        // Try to release by 2nd, successfully
        states[1].release(&params[1], LeaseLockOpts::Soft).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(!states[0].is_locked());
        assert!(!states[0].is_holder(&params[0].identity));
        assert!(states[0].is_expired());

        assert!(!states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(!states[1].is_holder(&params[0].identity));
        assert!(states[1].is_expired());

        states[0].delete().await.unwrap();
    }

    #[tokio::test]
    async fn soft_lock_1st_force_release_2nd() {
        const LEASE_NAME: &str = "soft-lock-1st-force-release-2nd-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock by 1st
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Try to release by 2nd, successfully
        states[1].release(&params[1], LeaseLockOpts::Force).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(!states[0].is_locked());
        assert!(!states[0].is_holder(&params[0].identity));
        assert!(states[0].is_expired());

        assert!(!states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(!states[1].is_holder(&params[0].identity));
        assert!(states[1].is_expired());

        states[0].delete().await.unwrap();
    }

    #[tokio::test]
    async fn soft_lock_1st_soft_lock_2nd() {
        const LEASE_NAME: &str = "soft-lock-1st-soft-lock-2nd-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock by 1st
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Try to lock by 2nd, unsuccessfully
        states[1].lock(&params[1], LeaseLockOpts::Soft).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Expire
        tokio::time::sleep(params[0].duration).await;
        assert!(states[0].is_expired());
        assert!(states[1].is_expired());

        // Try to lock by 2nd, unsuccessfully
        states[1].lock(&params[1], LeaseLockOpts::Soft).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(states[1].is_expired());

        states[0].delete().await.unwrap();
    }

    #[tokio::test]
    async fn unattended_soft_lock_1st_soft_lock_2nd() {
        const LEASE_NAME: &str = "unattended-soft-lock-1st-soft-lock-2nd-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock by 1st and 2nd
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].lock(&params[1], LeaseLockOpts::Soft).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Expire
        tokio::time::sleep(params[0].duration).await;
        assert!(states[0].is_expired());
        assert!(states[1].is_expired());

        // Try to lock by 2nd and 1st
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].lock(&params[1], LeaseLockOpts::Soft).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        states[0].delete().await.unwrap();
    }

    #[tokio::test]
    async fn unattended_soft_lock_1st_force_lock_2nd() {
        const LEASE_NAME: &str = "unattended-soft-lock-1st-force-lock-2nd-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock by 1st and 2nd
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].lock(&params[1], LeaseLockOpts::Soft).await.unwrap();

        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(!states[1].is_holder(&params[1].identity));
        assert!(states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        // Don't expire
        tokio::time::sleep(params[0].grace).await;
        assert!(!states[0].is_expired());
        assert!(!states[1].is_expired());

        // Try to lock by 2nd and 1st
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].lock(&params[1], LeaseLockOpts::Force).await.unwrap();
        states[0].sync(LeaseLockOpts::Force).await.unwrap();

        assert!(states[0].is_locked());
        assert!(!states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        assert!(states[1].is_locked());
        assert!(states[1].is_holder(&params[1].identity));
        assert!(!states[1].is_holder(&params[0].identity));
        assert!(!states[1].is_expired());

        states[0].delete().await.unwrap();
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
            set.insert(random_duration(
                DEFAULT_MIN_RANDOM_RELEASE_WAITING_MILLIS,
                DEFAULT_MAX_RANDOM_RELEASE_WAITING_MILLIS,
            ));
        }

        assert!(
            set.len() >= SET_LEN * 8 / 10,
            "at least 80% of randoms should be unique, but got {}%",
            set.len()
        );
    }

    #[tokio::test]
    async fn deleted_lease_state() {
        const LEASE_NAME: &str = "deleted-lease-state-test";

        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 1).await;

        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[0].delete().await.unwrap();

        let result = states[0].sync(LeaseLockOpts::Force).await;
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), LeaseError::KubeError(kube::Error::Api(err)) if err.code==404));
        assert!(states[0].holder.is_none());
        assert_eq!(states[0].transitions, 0);
        assert!(states[0].is_expired());
    }

    #[tokio::test]
    async fn grace_sleep_duration() {
        let client = Client::try_default().await.unwrap();
        let mut manager = LeaseManager::new(client, "lease_name", "namespace", LeaseParams::default());

        // Already expired - always ZERO
        manager.state.expiry = SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap();
        assert_eq!(
            manager.grace_sleep_duration(Duration::ZERO),
            Duration::ZERO,
            "should be ZERO since it's already expired"
        );
        assert_eq!(
            manager.grace_sleep_duration(Duration::from_secs(1)),
            Duration::ZERO,
            "should be ZERO since it's already expired"
        );

        // Expires in 10s
        manager.state.expiry = SystemTime::now().checked_add(Duration::from_secs(10)).unwrap();
        let duration = manager.grace_sleep_duration(Duration::ZERO);
        assert!(
            duration >= Duration::from_millis(9_900) && duration <= Duration::from_millis(10_000),
            "should be around 10s"
        );
        let duration = manager.grace_sleep_duration(Duration::from_secs(1));
        assert!(
            duration >= Duration::from_millis(8_900) && duration <= Duration::from_millis(9_000),
            "should be around 9s"
        );
        assert_eq!(
            manager.grace_sleep_duration(Duration::from_secs(10)),
            Duration::ZERO,
            "should be around ZERO"
        );
    }

    #[tokio::test]
    async fn single_manager_watcher_step() {
        const LEASE_NAME: &str = "single-manager-watcher-step-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 1).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(!managers[0].state.is_holder(&managers[0].params.identity));
        assert!(!managers[0].state.is_locked());

        managers[0].watcher_step().await.unwrap();
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(managers[0].state.is_holder(&managers[0].params.identity));
        assert!(managers[0].state.is_locked());
        assert!(!managers[0].state.is_expired());

        // Expire
        tokio::time::sleep(managers[0].params.duration).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));
        assert!(managers[0].state.is_holder(&managers[0].params.identity));
        assert!(managers[0].state.is_locked());
        assert!(managers[0].state.is_expired());

        // Clean up
        managers[0].state.delete().await.unwrap();
    }

    #[tokio::test]
    async fn single_manager_changed_loop() {
        const LEASE_NAME: &str = "single-manager-changed-loop-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 1).await;
        assert!(!managers[0].is_leader.load(Ordering::Relaxed));

        let is_leader = managers[0].changed().await.unwrap();
        assert!(is_leader);
        assert!(managers[0].is_leader.load(Ordering::Relaxed));

        let long_duration = managers[0].params.duration.checked_mul(2).unwrap();
        select! {
             _ = managers[0].changed() => {
                unreachable!("unreachable branch since `changed` loop should lasts forever")
             },
             _ = tokio::time::sleep(long_duration) => {
                assert!(managers[0].is_leader.load(Ordering::Relaxed))
            }
        }
        assert!(managers[0].is_leader.load(Ordering::Relaxed));

        // Clean up
        managers[0].state.delete().await.unwrap();
    }

    #[tokio::test]
    async fn two_managers_1st_expire_then_2nd_lock() {
        const LEASE_NAME: &str = "two-managers-1st-expire-then-2nd-lock-test";

        let mut managers = setup_simple_managers_vec(LEASE_NAME, 2).await;
        let mut manager0 = managers.pop().unwrap();
        let mut manager1 = managers.pop().unwrap();

        assert!(!manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Lock by 1st
        let is_leader = manager0.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Try to hold lock by 1st
        let long_duration = manager0.params.duration.checked_mul(2).unwrap();
        select! {
            biased;
             _ = manager0.changed() => {
                unreachable!("unreachable branch since `changed` loop should lasts forever")
             },
             _ = manager1.changed() => {
                unreachable!("unreachable branch since `changed` loop should lasts forever")
             },
             _ = tokio::time::sleep(long_duration) => {
                assert!(manager0.is_leader.load(Ordering::Relaxed));
                assert!(!manager1.is_leader.load(Ordering::Relaxed));
            }
        }
        assert!(manager0.is_leader.load(Ordering::Relaxed));
        assert!(!manager1.is_leader.load(Ordering::Relaxed));

        // Expire and try to re-lock by 2nd
        tokio::time::sleep(manager0.params.duration).await;
        let is_leader = manager1.changed().await.unwrap();
        assert!(is_leader);
        assert!(manager1.is_leader.load(Ordering::Relaxed));

        let is_leader = manager0.changed().await.unwrap();
        assert!(!is_leader);
        assert!(!manager0.is_leader.load(Ordering::Relaxed));

        // Clean up
        manager0.state.delete().await.unwrap();
    }
}
