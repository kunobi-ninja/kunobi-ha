//! Error types.

/// Error returned by leader-election operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wrapping a `kube` API error.
    #[error("kubernetes api error: {0}")]
    Kube(#[from] kube::Error),

    /// Wrapping timestamp conversion errors from `k8s_openapi::jiff`.
    #[error("timestamp conversion: {0}")]
    Timestamp(String),

    /// Could not observe or acquire the lease within the configured deadline.
    ///
    /// Typically means the Kubernetes API is unreachable for a prolonged
    /// period — the caller should treat this as fatal and exit so the pod
    /// restarts.
    #[error("failed to observe or acquire leader lease within {0:?}: {1}")]
    ObserveTimeout(std::time::Duration, Box<Error>),

    /// The configuration handed to `LeaderElection::acquire` violates
    /// an invariant of the algorithm (e.g. `renew_deadline` is not
    /// strictly less than `lease_duration`).
    #[error("invalid leader-election config: {0}")]
    InvalidConfig(String),
}

/// Convenience `Result` alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
