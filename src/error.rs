//! Error types.

use std::time::Duration;

/// Error returned by leader-election operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    ObserveTimeout(Duration, Box<Error>),

    /// The configuration handed to [`LeaderElection::acquire`][crate::leader::LeaderElection::acquire]
    /// violates an invariant of the algorithm. The contained
    /// [`InvalidConfig`] value identifies which one.
    #[error("invalid leader-election config: {0}")]
    InvalidConfig(#[from] InvalidConfig),
}

/// Specific reason a leader-election configuration failed validation.
///
/// Returned wrapped in [`Error::InvalidConfig`]. Carries the offending
/// values so the caller can produce diagnostics without parsing
/// strings.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidConfig {
    /// `renew_deadline` is not strictly less than `lease_duration`.
    /// Without this gap, a brief renewal hiccup can race a takeover
    /// and produce two simultaneous leaders.
    #[error("renew_deadline ({renew:?}) must be strictly less than lease_duration ({lease:?})")]
    RenewDeadlineNotLess {
        /// Configured `renew_deadline`.
        renew: Duration,
        /// Configured `lease_duration`.
        lease: Duration,
    },

    /// `retry_period` is greater than `renew_deadline`. The follower
    /// poll cadence cannot exceed the leader's renewal deadline; the
    /// algorithm relies on followers checking the lease at least as
    /// often as the leader is expected to renew.
    #[error("retry_period ({retry:?}) must be <= renew_deadline ({renew:?})")]
    RetryPeriodTooLong {
        /// Configured `retry_period`.
        retry: Duration,
        /// Configured `renew_deadline`.
        renew: Duration,
    },

    /// `retry_period` is zero. A zero retry period would busy-loop
    /// against the API server.
    #[error("retry_period must be greater than zero")]
    RetryPeriodZero,

    /// The resolved identity string is empty.
    #[error("identity must not be empty")]
    IdentityEmpty,

    /// The Lease name passed to the builder is empty.
    #[error("lease name must not be empty")]
    NameEmpty,

    /// The namespace passed to the builder is empty.
    #[error("namespace must not be empty")]
    NamespaceEmpty,
}

/// Convenience `Result` alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
