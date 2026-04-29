//! Error types returned by leader-election operations.
//!
//! [`Error`] is the top-level enum every fallible API in this crate
//! returns. Specific configuration-validation failures are nested
//! inside [`Error::InvalidConfig`] as [`InvalidConfig`] variants —
//! callers that want to react to specific misconfigurations match
//! through the wrapper rather than parsing the [`Display`] string.
//!
//! [`Display`]: std::fmt::Display

use std::time::Duration;

/// Error returned by leader-election operations.
///
/// Marked `#[non_exhaustive]` — match through with a wildcard arm
/// to keep your code compatible with future minor releases.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A request to the Kubernetes API failed in a way the renew loop
    /// or initial-acquire loop did not handle internally.
    ///
    /// During the initial-acquire loop, transient [`kube::Error`]s
    /// are folded into the retry path and only surface as
    /// [`Error::ObserveTimeout`] if they persist. After acquisition,
    /// most renewal errors are also retried within `renew_deadline`;
    /// see [`LeaderGuard::lost`][crate::leader::LeaderGuard::lost]
    /// for the runtime path.
    #[error("kubernetes api error: {0}")]
    Kube(#[from] kube::Error),

    /// The system clock produced a value that doesn't fit
    /// `k8s_openapi::jiff::Timestamp`. Should be unreachable in
    /// practice — would require a clock skew of centuries.
    #[error("timestamp conversion: {0}")]
    Timestamp(String),

    /// The Kubernetes API stayed unreachable for longer than
    /// `observe_timeout` while [`LeaderElection::acquire`][crate::leader::LeaderElection::acquire]
    /// was looping.
    ///
    /// Treat this as fatal: exit so kubelet restarts the pod and the
    /// acquisition retries from scratch. The boxed inner [`Error`]
    /// is the most recent transient error encountered.
    #[error("failed to observe or acquire leader lease within {0:?}: {1}")]
    ObserveTimeout(Duration, Box<Error>),

    /// Configuration handed to [`LeaderElection::acquire`][crate::leader::LeaderElection::acquire]
    /// violates an invariant of the algorithm. Returned synchronously
    /// from `acquire().await` before any API call.
    ///
    /// The wrapped [`InvalidConfig`] value identifies the specific
    /// invariant and carries the offending values so callers can
    /// produce typed diagnostics.
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
