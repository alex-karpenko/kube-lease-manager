use crate::{DurationSeconds, LeaseCreateMode, LeaseManagerError, LeaseParams, Result};
use k8s_openapi::{
    api::coordination::v1::Lease,
    apimachinery::pkg::apis::meta::v1::MicroTime,
    chrono::{DateTime, Utc},
    serde::Serialize,
    serde_json,
};
use kube::{
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    Api, Client,
};
use std::{
    fmt::Debug,
    time::{Duration, SystemTime},
};
use tracing::debug;

/// Represents actual Lease state.
#[derive(Debug)]
pub(crate) struct LeaseState {
    /// namespaced kube::Api
    api: Api<Lease>,
    /// Name of the Lease object.
    lease_name: String,
    /// Identity of the current Lease holder, if it's locked now.
    pub(crate) holder: Option<String>,
    /// Time of the potential state expiration.
    pub(crate) expiry: SystemTime,
    /// Transitions count.
    transitions: i32,
}

/// Options to use for operations with lock.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LeaseLockOpts {
    #[default]
    Soft,
    Force,
}

impl LeaseState {
    /// Constructor
    pub(crate) async fn new(
        client: Client,
        lease_name: impl Into<String>,
        namespace: &str,
        create_mode: LeaseCreateMode,
    ) -> Result<Self> {
        let api = Api::<Lease>::namespaced(client, namespace);

        let state = Self {
            api,
            lease_name: lease_name.into(),
            holder: None,
            expiry: SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap(),
            transitions: 0,
        };

        state.create(create_mode).await?;
        Ok(state)
    }

    /// Check if current state is still valid.
    pub(crate) fn is_expired(&self) -> bool {
        SystemTime::now() > self.expiry
    }

    /// Check if current holder is the same as `other` parameter.
    pub(crate) fn is_holder(&self, other: &str) -> bool {
        if let Some(holder) = &self.holder {
            holder == other
        } else {
            false
        }
    }

    /// Check if current holder is set to something.
    pub(crate) fn is_locked(&self) -> bool {
        self.holder.is_some()
    }

    /// Retrieve actual state from the cluster.
    pub(crate) async fn sync(&mut self, opts: LeaseLockOpts) -> Result<()> {
        if opts == LeaseLockOpts::Force || self.is_expired() {
            let result = self.get().await;

            // If Lease doesn't exist - clear state before exiting
            if let Err(LeaseManagerError::NonexistentLease(_)) = &result {
                self.holder = None;
                self.transitions = 0;
                self.expiry = SystemTime::now().checked_sub(Duration::from_nanos(1)).unwrap();

                return Err(result.err().unwrap());
            }

            // If success or non-404 - try to unwrap spec and do sync
            let result = result?;
            if let Some(lease) = result.spec {
                self.holder = lease.holder_identity;
                self.transitions = lease.lease_transitions.unwrap_or(0);
                self.expiry = {
                    let renew = lease.renew_time;
                    let duration = lease
                        .lease_duration_seconds
                        .map(|d| Duration::from_secs(d as DurationSeconds));

                    if renew.is_some() && duration.is_some() {
                        let renew: SystemTime = renew.unwrap().0.into();
                        let duration: Duration = duration.unwrap();

                        renew.checked_add(duration).unwrap()
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

    pub(crate) async fn lock(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await?;

        let lease_duration_seconds = params.duration;
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
                },
            });
            let patch = Patch::Apply(patch);
            self.patch(params, &patch).await?;

            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "spec": {
                    "leaseTransitions": self.transitions + 1,
                },
            });
            Patch::Strategic(patch)
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

    pub(crate) async fn release(&mut self, params: &LeaseParams, opts: LeaseLockOpts) -> Result<()> {
        self.sync(LeaseLockOpts::Soft).await?;

        if self.is_holder(&params.identity) || self.is_expired() || opts == LeaseLockOpts::Force {
            debug!(?params, ?opts, "release lock");

            let patch = serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                // "metadata": {
                //     "managedFields": [{}]
                // },
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

    pub(crate) async fn patch<P>(&self, params: &LeaseParams, patch: &Patch<P>) -> Result<()>
    where
        P: Serialize + Debug,
    {
        debug!(?patch);

        let params = PatchParams {
            field_manager: Some(params.field_manager()),
            // force: matches!(patch, Patch::Apply(_)),
            ..Default::default()
        };

        match self.api.patch(&self.lease_name, &params, patch).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.reason == "Conflict" && err.code == 409 => {
                debug!(error = ?err, "patch conflict detected");
                Err(LeaseManagerError::LockConflict)
            }
            Err(err) => Err(LeaseManagerError::KubeError(err)),
        }
    }

    async fn get(&self) -> Result<Lease> {
        let result = self.api.get(&self.lease_name).await;

        // Map error is it doesn't exists
        match result {
            Ok(lease) => Ok(lease),
            Err(kube::Error::Api(err)) if err.code == 404 => {
                Err(LeaseManagerError::NonexistentLease(self.lease_name.clone()))
            }
            Err(err) => Err(LeaseManagerError::KubeError(err)),
        }
    }

    pub(crate) async fn create(&self, mode: LeaseCreateMode) -> Result<Lease> {
        let result = self.get().await;
        let pp = PostParams::default();
        let data = Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                ..Default::default()
            },
            spec: Default::default(),
        };

        match mode {
            LeaseCreateMode::AutoCreate => {
                // Get it and return ok if it exists,
                // Create if it doesn't exist
                if let Ok(lease) = result {
                    Ok(lease)
                } else if let Err(LeaseManagerError::NonexistentLease(_)) = result {
                    self.api.create(&pp, &data).await.map_err(LeaseManagerError::from)
                } else {
                    result
                }
            }
            LeaseCreateMode::CreateNew => {
                // Get it and fail if it exists,
                // Create if else
                if result.is_ok() {
                    Err(LeaseManagerError::LeaseAlreadyExists(self.lease_name.clone()))
                } else {
                    self.api.create(&pp, &data).await.map_err(LeaseManagerError::from)
                }
            }
            LeaseCreateMode::UseExistent => {
                // Get it and fail if doesn't exist
                result
            }
            #[cfg(test)]
            LeaseCreateMode::Ignore => Ok(data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{init, sleep_secs, TEST_NAMESPACE};
    use k8s_openapi::api::coordination::v1::LeaseSpec;
    use kube::api::DeleteParams;

    async fn setup_simple_leaders_vec(lease_name: &str, count: usize) -> (Vec<LeaseParams>, Vec<LeaseState>) {
        const LEASE_DURATION_SECONDS: DurationSeconds = 2;
        const LEASE_GRACE_SECONDS: DurationSeconds = 1;

        let client = init().await;
        let mut params = vec![];
        let mut states = vec![];

        for i in 0..count {
            let param = LeaseParams::new(format!("leader-{i}"), LEASE_DURATION_SECONDS, LEASE_GRACE_SECONDS);
            let state = LeaseState::new(client.clone(), lease_name, TEST_NAMESPACE, LeaseCreateMode::Ignore)
                .await
                .unwrap();

            params.push(param);
            states.push(state);
        }

        // Create lease
        let _ = states[0].create(LeaseCreateMode::CreateNew).await.unwrap();

        (params, states)
    }

    impl LeaseState {
        pub(crate) async fn delete(&self) -> Result<()> {
            let dp = DeleteParams::default();
            let _ = self.api.delete(&self.lease_name, &dp).await?;

            Ok(())
        }
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn rough_create_delete() {
        const LEASE_NAME: &str = "rough-create-delete-test";

        let client = init().await;
        let state = LeaseState::new(client, LEASE_NAME, TEST_NAMESPACE, LeaseCreateMode::Ignore)
            .await
            .unwrap();

        let lease = state.create(LeaseCreateMode::CreateNew).await.unwrap();
        assert!(lease.spec.is_some());
        assert_eq!(lease.spec.unwrap(), LeaseSpec::default());

        state.delete().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn simple_soft_lock_soft_release() {
        const LEASE_NAME: &str = "simple-soft-lock-soft-release-test";
        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 1).await;

        // Lock
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        assert!(states[0].is_locked());
        assert!(states[0].is_holder(&params[0].identity));
        assert!(!states[0].is_expired());

        // Expire
        sleep_secs(params[0].duration).await;
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
    #[ignore = "uses k8s current-context"]
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
        sleep_secs(params[0].duration).await;
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
    #[ignore = "uses k8s current-context"]
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
    #[ignore = "uses k8s current-context"]
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
        sleep_secs(params[0].duration).await;
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
    #[ignore = "uses k8s current-context"]
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
        sleep_secs(params[0].duration).await;
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
    #[ignore = "uses k8s current-context"]
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
        sleep_secs(params[0].grace).await;
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

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn deleted_lease_state() {
        const LEASE_NAME: &str = "deleted-lease-state-test";

        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 1).await;

        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[0].delete().await.unwrap();

        let result = states[0].sync(LeaseLockOpts::Force).await;
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), LeaseManagerError::NonexistentLease(_)));
        assert!(states[0].holder.is_none());
        assert_eq!(states[0].transitions, 0);
        assert!(states[0].is_expired());
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn update_lease_with_conflict() {
        const LEASE_NAME: &str = "update-lease-with-conflict-test";

        let (params, mut states) = setup_simple_leaders_vec(LEASE_NAME, 2).await;

        // Lock lease ordinary
        states[0].lock(&params[0], LeaseLockOpts::Soft).await.unwrap();
        states[1].sync(LeaseLockOpts::Force).await.unwrap();

        // if lock is orphan - try to lock it softly
        let now: DateTime<Utc> = SystemTime::now().into();
        let patch = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "spec": {
                "acquireTime": MicroTime(now),
                "renewTime": MicroTime(now),
                "holderIdentity": params[1].identity,
                "leaseDurationSeconds": params[1].duration,
                "leaseTransitions": states[1].transitions + 1,
            },
        });

        let patch = Patch::Apply(patch);
        let result = states[1].patch(&params[1], &patch).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(LeaseManagerError::LockConflict)));

        states[0].delete().await.unwrap();
    }
}
