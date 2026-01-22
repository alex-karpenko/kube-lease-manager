use crate::state::LeaseStateError;

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
