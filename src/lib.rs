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

/// Represents `kube-lease` specific errors.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LeaseError {}

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
    pub fn new(
        identity: impl Into<String>,
        duration: impl Into<Duration>,
        grace: impl Into<Duration>,
    ) -> Self {
        let duration: Duration = duration.into();
        let grace: Duration = grace.into();

        if grace >= duration {
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
            expiry: SystemTime::now()
                .checked_sub(Duration::from_nanos(1))
                .unwrap(),
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
            let lease = self.api.get(&self.lease_name).await.unwrap().spec.unwrap();

            self.holder = lease.holder_identity;
            self.transitions = lease.lease_transitions.unwrap_or(0);
            self.expiry = {
                let renew = lease.renew_time;
                let duration = lease
                    .lease_duration_seconds
                    .map(|d| Duration::from_secs(d as u64));

                if renew.is_some() && duration.is_some() {
                    let renew: SystemTime = renew.unwrap().0.into();
                    let duration: Duration = duration.unwrap();

                    renew + duration
                } else {
                    SystemTime::now()
                        .checked_sub(Duration::from_nanos(1))
                        .unwrap()
                }
            };
        }

        Ok(())
    }

    async fn lock(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await.unwrap();

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
            Patch::Strategic(patch)
        } else if opts == LeaseLockOpts::Force {
            // if it's locked by someone else - try to lock it with force
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
        } else {
            return Ok(());
        };

        self.patch(params, &patch).await.unwrap();
        self.sync(LeaseLockOpts::Force).await
    }

    async fn release(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await.unwrap();

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
            self.patch(params, &patch).await.unwrap();
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

        self.api
            .patch(&self.lease_name, &params, patch)
            .await
            .unwrap();

        Ok(())
    }

    async fn create(&self) -> Result<Lease> {
        let pp = PostParams::default();
        let data = Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                ..Default::default()
            },
            spec: Default::default(),
        };

        let lease = self.api.create(&pp, &data).await.unwrap();
        Ok(lease)
    }

    async fn delete(&self) -> Result<()> {
        let dp = DeleteParams::default();
        let _ = self.api.delete(&self.lease_name, &dp).await.unwrap();

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

    /// Task which tries to lock lease and renews it periodically.
    pub async fn watch(&mut self) {
        loop {
            // re-sync state if needed
            self.state.sync(LeaseLockOpts::Soft).await.unwrap();

            if self.state.is_holder(&self.params.identity) {
                // if we're holder of the lock - sleep up to the next refresh time,
                // and renew lock (lock it softly)
                self.sleep_with_grace(self.params.grace).await;

                debug!("renew own lease lock");
                self.state
                    .lock(&self.params, LeaseLockOpts::Soft)
                    .await
                    .unwrap();
            } else if !self.state.is_locked() {
                // Lease isn't locket yet
                debug!("try to lock lease");
                self.state
                    .lock(&self.params, LeaseLockOpts::Soft)
                    .await
                    .unwrap();
            } else if self.state.is_locked() && self.state.is_expired() {
                // It's locked by someone else but lock is already expired.
                // Release it by force and try to lock on the next loop cycle
                debug!("release expired lease lock");
                self.state
                    .release(&self.params, LeaseLockOpts::Force)
                    .await
                    .unwrap();

                // Sleep some random time (up to 500ms) to minimize collisions probability
                tokio::time::sleep(random_duration(1, 500)).await;
            } else if self.state.is_locked() && !self.state.is_expired() {
                // It's locked by someone else and lock is actual.
                // Sleep up to the expiration time of the lock.
                debug!(
                    holder = self.state.holder.as_ref().unwrap(),
                    "lease is actually locked by other identity"
                );
                self.sleep_with_grace(Duration::ZERO).await;
            } else {
                // Something wrong happened
                error!(?self, "unreachable branch, looks like a BUG!");
                unreachable!("it's impossible to reach this branch, looks like a BUG!");
            }

            // Is leader changed after this iteration?
            let is_holder = self.state.is_holder(&self.params.identity);
            if self.is_leader.load(Ordering::Acquire) != is_holder {
                debug!(leader = is_holder, "lease lock state has been changed");
                self.is_leader.store(is_holder, Ordering::Release);
                // TODO: inform around using watch channel or/and callback
            }
        }
    }

    async fn sleep_with_grace(&self, grace: Duration) {
        let wait_for = self
            .state
            .expiry
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
            .saturating_sub(grace);

        tokio::time::sleep(wait_for).await;
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

