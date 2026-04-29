//! Lease-based leader election for Kubernetes controllers.
//!
//! Uses a single `coordination.k8s.io/v1 Lease` object as the shared lock,
//! following the same pattern as `kube-controller-manager` and
//! `kube-scheduler`.
//!
//! # Quickstart
//!
//! ```no_run
//! # async fn example(client: kube::Client) -> kunobi_ha::Result<()> {
//! use kunobi_ha::leader::{LeaderElection, StepDownReason};
//! use std::time::Duration;
//!
//! let leader = LeaderElection::builder(client, "karakuri-system", "karakuri-operator")
//!     .build();
//!
//! // Block until we're leader. `guard` holds the renewal task and, when
//! // dropped / explicitly `step_down`'d, clears our holder identity on the
//! // Lease so the next replica picks up immediately (no need to wait for
//! // TTL to expire).
//! let mut guard = leader.acquire().await?;
//!
//! // Start controllers here, watching `guard.lost()` for step-down.
//! loop {
//!     tokio::select! {
//!         reason = guard.lost() => {
//!             match reason {
//!                 StepDownReason::RenewDeadlineExceeded => {} // API down
//!                 StepDownReason::HolderChanged => {}         // usurped
//!                 StepDownReason::Cancelled => {}             // self-driven
//!                 _ => {}                                     // future variants
//!             }
//!             break;
//!         }
//!         // ... your controller futures ...
//!     }
//! }
//!
//! // Explicit cooperative step-down — next replica takes over in seconds
//! // instead of waiting for the 15s TTL.
//! guard.step_down().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Timing model
//!
//! Follows the Kubernetes reference (`leaderelection.LeaderElectionConfig`):
//!
//! - `lease_duration` — how long a leader is considered valid if they stop
//!   renewing. Default `15s`.
//! - `renew_deadline` — the leader keeps trying to renew within this
//!   window. If it fails past the deadline, it steps down. Default `10s`
//!   (the 2/3 rule).
//! - `retry_period` — followers check the lease at this cadence, and the
//!   leader attempts renewal every period. Default `2s`.
//!
//! The gap `lease_duration - renew_deadline` (= 5s by default) gives a
//! departing leader a grace window to notice loss and step down before any
//! follower could legitimately claim the lease.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::error::{Error, InvalidConfig, Result};

// ---------------------------------------------------------------------------
// Defaults (Kubernetes reference values)
// ---------------------------------------------------------------------------

const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
const DEFAULT_RENEW_DEADLINE: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_PERIOD: Duration = Duration::from_secs(2);
const DEFAULT_OBSERVE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_RENEW_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// How an instance identifies itself in the Lease's `holder_identity`
/// field.
///
/// Two replicas competing for the same Lease must have distinct
/// identities; otherwise either one will be treated as renewing the
/// other's hold.
///
/// The default ([`Identity::PodNameOrUuid`]) is the right choice in
/// Kubernetes — `$HOSTNAME` is the pod name, which is unique within
/// the cluster.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum Identity {
    /// Use `$HOSTNAME`. Falls back to a fresh UUID if the variable is
    /// unset, empty, or unreadable. Recommended default in
    /// Kubernetes.
    #[default]
    PodNameOrUuid,
    /// Always generate a fresh UUID, ignoring `$HOSTNAME`. Useful for
    /// non-Kubernetes uses or when the host name is not unique.
    Generated,
    /// Use this exact string. The caller is responsible for uniqueness.
    Custom(String),
}

impl Identity {
    fn resolve(self) -> String {
        match self {
            Identity::PodNameOrUuid => std::env::var("HOSTNAME")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            Identity::Generated => uuid::Uuid::new_v4().to_string(),
            Identity::Custom(s) => s,
        }
    }
}

impl From<&str> for Identity {
    fn from(s: &str) -> Self {
        Identity::Custom(s.to_string())
    }
}

impl From<String> for Identity {
    fn from(s: String) -> Self {
        Identity::Custom(s)
    }
}

/// Cheaply clonable, thread-safe handle to the current leader status.
///
/// Use this to gate a `/readyz` probe (or anything else that should
/// only be true on the leader replica) without rolling your own
/// `Arc<AtomicBool>`.
///
/// Obtain via [`LeaderElection::state`] before calling `acquire()` so
/// you can spawn your HTTP server BEFORE blocking on lease
/// acquisition.
///
/// # Lifecycle
///
/// The library guarantees, under sequential consistency:
///
/// - Returns `false` until [`LeaderElection::acquire`] resolves
///   `Ok(guard)`. The flip to `true` happens before `acquire()`
///   returns.
/// - Flips back to `false` **before** [`LeaderGuard::lost`] resolves
///   with a [`StepDownReason`] (so `/readyz` reports unready before
///   the user-level shutdown handler runs).
/// - Flips to `false` synchronously when [`LeaderGuard::step_down`] is
///   called or the [`LeaderGuard`] is dropped.
///
/// # Example
///
/// ```no_run
/// # async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
/// use kunobi_ha::leader::LeaderElection;
///
/// let leader = LeaderElection::builder(client, "my-ns", "my-operator").build();
/// let state = leader.state();
///
/// // Spawn the HTTP server BEFORE blocking on the lease — `/readyz`
/// // will return 503 until acquire() succeeds.
/// let probe_state = state.clone();
/// tokio::spawn(async move {
///     // axum / actix / hyper handler reads `probe_state.is_leader()`
///     // and replies 200 / 503.
///     # let _ = probe_state;
/// });
///
/// let mut guard = leader.acquire().await?;
/// assert!(state.is_leader());
/// // Run your reconcilers...
/// guard.step_down().await;
/// assert!(!state.is_leader());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct LeaderState {
    inner: Arc<AtomicBool>,
}

impl LeaderState {
    fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `true` while this replica currently holds the leader lease.
    ///
    /// Cheap (one atomic load); safe to call from any thread, any
    /// number of times.
    pub fn is_leader(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }

    fn set(&self, leader: bool) {
        self.inner.store(leader, Ordering::Release);
    }
}

/// Configure and start a leader election loop.
#[non_exhaustive]
pub struct LeaderElection {
    client: Client,
    namespace: String,
    name: String,
    identity: String,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
    observe_timeout: Duration,
    renew_request_timeout: Duration,
    state: LeaderState,
}

/// Fluent builder returned by [`LeaderElection::builder`].
#[non_exhaustive]
pub struct LeaderElectionBuilder {
    client: Client,
    namespace: String,
    name: String,
    identity: Option<Identity>,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
    observe_timeout: Duration,
    renew_request_timeout: Duration,
}

impl LeaderElection {
    /// Start building a leader-election configuration.
    ///
    /// - `client` is a [`kube::Client`] (typically obtained from
    ///   `kube::Client::try_default().await?`).
    /// - `namespace` is the Kubernetes namespace where the Lease
    ///   object lives. Should be the operator's own namespace.
    /// - `name` is the Lease object name and **must be unique per
    ///   controller** running against the same Kubernetes cluster.
    ///   Use a stable, descriptive name like `"my-operator"`; do not
    ///   include per-replica information.
    ///
    /// Defaults applied by the builder match
    /// `kube-controller-manager` / `kube-scheduler`:
    ///
    /// | parameter | default |
    /// |---|---|
    /// | `lease_duration` | 15s |
    /// | `renew_deadline` | 10s |
    /// | `retry_period` | 2s |
    /// | `observe_timeout` | 5min |
    /// | `renew_request_timeout` | 5s |
    /// | `identity` | [`Identity::PodNameOrUuid`] |
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
    /// use kunobi_ha::leader::LeaderElection;
    /// let leader = LeaderElection::builder(client, "kunobi-system", "my-operator")
    ///     .build();
    /// # let _ = leader; Ok(()) }
    /// ```
    pub fn builder(
        client: Client,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> LeaderElectionBuilder {
        LeaderElectionBuilder {
            client,
            namespace: namespace.into(),
            name: name.into(),
            identity: None,
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_deadline: DEFAULT_RENEW_DEADLINE,
            retry_period: DEFAULT_RETRY_PERIOD,
            observe_timeout: DEFAULT_OBSERVE_TIMEOUT,
            renew_request_timeout: DEFAULT_RENEW_REQUEST_TIMEOUT,
        }
    }
}

impl LeaderElectionBuilder {
    /// Override the identity. Defaults to [`Identity::PodNameOrUuid`].
    ///
    /// `&str` and `String` convert into [`Identity::Custom`]
    /// automatically, so `.identity("foo")` and
    /// `.identity(Identity::Generated)` both work.
    pub fn identity(mut self, identity: impl Into<Identity>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// How long the lease is considered valid after the last
    /// successful renewal. Followers treat the lease as expired and
    /// up-for-grabs once `now > last_renew + lease_duration`.
    ///
    /// Default: 15s. Must be strictly greater than
    /// [`renew_deadline`](Self::renew_deadline) — see the timing
    /// model in the [module docs](self).
    pub fn lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Deadline within which the leader must successfully renew the
    /// Lease. If the leader cannot renew by this time it steps down
    /// with [`StepDownReason::RenewDeadlineExceeded`].
    ///
    /// Default: 10s (2/3 of `lease_duration` per the Kubernetes
    /// reference). Must be strictly less than
    /// [`lease_duration`](Self::lease_duration); the gap gives a
    /// departing leader a grace window to notice loss and step down
    /// before any follower can legitimately claim the lease.
    pub fn renew_deadline(mut self, duration: Duration) -> Self {
        self.renew_deadline = duration;
        self
    }

    /// Cadence for the leader's renewal attempts and the follower's
    /// poll loop. The first follower to poll after a leader stepped
    /// down (or had its TTL expire) takes over the lease.
    ///
    /// Default: 2s. Must be `<= renew_deadline` and `> 0`.
    pub fn retry_period(mut self, duration: Duration) -> Self {
        self.retry_period = duration;
        self
    }

    /// Maximum time the *initial-acquire* loop will tolerate sustained
    /// API errors before bailing out with [`Error::ObserveTimeout`].
    /// Distinct from [`renew_deadline`](Self::renew_deadline), which
    /// applies once we already hold the lease.
    ///
    /// Default: 5 minutes. Treat the resulting error as fatal —
    /// kubelet will restart the pod and we'll retry from scratch.
    pub fn observe_timeout(mut self, duration: Duration) -> Self {
        self.observe_timeout = duration;
        self
    }

    /// Per-renewal-request timeout (covers a single GET + PUT cycle
    /// during the renew loop). Independent of
    /// [`renew_deadline`](Self::renew_deadline), which is the
    /// cumulative budget across multiple failed renewal attempts.
    ///
    /// Default: 5s.
    pub fn renew_request_timeout(mut self, duration: Duration) -> Self {
        self.renew_request_timeout = duration;
        self
    }

    /// Finalise the configuration into a [`LeaderElection`].
    ///
    /// The returned value can be used to:
    /// - obtain a [`LeaderState`] handle via
    ///   [`LeaderElection::state`] before blocking on acquisition;
    /// - block on the lease via [`LeaderElection::acquire`].
    pub fn build(self) -> LeaderElection {
        let identity = self.identity.unwrap_or_default().resolve();
        LeaderElection {
            client: self.client,
            namespace: self.namespace,
            name: self.name,
            identity,
            lease_duration: self.lease_duration,
            renew_deadline: self.renew_deadline,
            retry_period: self.retry_period,
            observe_timeout: self.observe_timeout,
            renew_request_timeout: self.renew_request_timeout,
            state: LeaderState::new(),
        }
    }
}

impl LeaderElection {
    /// Returns a clonable handle to this instance's leader status.
    ///
    /// Cheap to call (clones an `Arc`). Safe to call before
    /// [`acquire`](Self::acquire) — the handle starts as
    /// `is_leader() == false` and flips to `true` when acquire
    /// succeeds.
    ///
    /// See [`LeaderState`] for the full lifecycle contract.
    pub fn state(&self) -> LeaderState {
        self.state.clone()
    }

    /// Block until this instance acquires the lease, then return a
    /// guard that keeps the lease renewed in the background.
    ///
    /// The returned [`LeaderGuard`] should be held for the lifetime
    /// of the controllers. Call [`LeaderGuard::lost`] to wait for
    /// step-down events, and [`LeaderGuard::step_down`] in your
    /// shutdown path to relinquish the lease cooperatively.
    ///
    /// On success, any clone of the [`LeaderState`] handle obtained
    /// via [`Self::state`] flips to `is_leader() == true`
    /// synchronously with this future resolving.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidConfig`] — the durations or
    ///   identity/name/namespace strings violate an invariant of the
    ///   algorithm. Returned synchronously, before any API call.
    /// - [`Error::ObserveTimeout`] — the Kubernetes API has been
    ///   unreachable (or returning errors) for more than
    ///   `observe_timeout` while we tried to acquire. Treat as fatal:
    ///   exit so kubelet restarts the pod.
    /// - [`Error::Kube`] is **not** returned during acquisition —
    ///   transient API errors are folded into the retry loop and
    ///   only surface as `ObserveTimeout` if they persist.
    /// - [`Error::Timestamp`] — the system clock produced a value
    ///   that doesn't fit `k8s_openapi::jiff::Timestamp`. Should be
    ///   unreachable in practice (would require a clock skew of
    ///   centuries).
    ///
    /// # Cancel safety
    ///
    /// This future is **not** cancel-safe. Dropping the future mid-
    /// execution is fine for resource cleanup (no leak), but if a
    /// transient acquire/replace request was in flight at the moment
    /// of cancellation, the holder identity on the Lease may be set
    /// to ours even though no [`LeaderGuard`] is returned. The next
    /// acquire attempt's renew/takeover path handles that correctly
    /// (we recognise our own identity and renew rather than create).
    pub async fn acquire(&self) -> Result<LeaderGuard> {
        self.validate()?;

        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);

        info!(identity = %self.identity, lease = %self.name, "starting leader election");

        let mut observe_error_deadline: Option<tokio::time::Instant> = None;
        loop {
            match try_acquire(&leases, &self.name, &self.identity, self.lease_duration).await {
                Ok(true) => {
                    info!(identity = %self.identity, "acquired leader lease");
                    break;
                }
                Ok(false) => {
                    observe_error_deadline = None;
                    tokio::time::sleep(self.retry_period).await;
                }
                Err(e) => {
                    let now = tokio::time::Instant::now();
                    let deadline = observe_error_deadline.get_or_insert(now + self.observe_timeout);
                    if now >= *deadline {
                        return Err(Error::ObserveTimeout(self.observe_timeout, Box::new(e)));
                    }
                    warn!(%e, "leader election error, retrying");
                    tokio::time::sleep(self.retry_period).await;
                }
            }
        }

        // Flip leader-state to `true` BEFORE returning the guard, so
        // any clone of `self.state()` already held by the caller (e.g.
        // an HTTP server's `/readyz` handler) sees us as ready
        // synchronously with `acquire().await` resolving.
        self.state.set(true);

        let (tx, rx) = watch::channel::<Option<StepDownReason>>(None);
        let handle = tokio::spawn({
            let client = self.client.clone();
            let namespace = self.namespace.clone();
            let name = self.name.clone();
            let identity = self.identity.clone();
            let retry_period = self.retry_period;
            let renew_deadline = self.renew_deadline;
            let lease_duration = self.lease_duration;
            let renew_request_timeout = self.renew_request_timeout;
            let state = self.state.clone();
            async move {
                renew_loop(
                    client,
                    &namespace,
                    &name,
                    &identity,
                    lease_duration,
                    retry_period,
                    renew_deadline,
                    renew_request_timeout,
                    state,
                    tx,
                )
                .await;
            }
        });

        Ok(LeaderGuard {
            client: self.client.clone(),
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            identity: self.identity.clone(),
            rx,
            renew_handle: Some(handle),
            state: self.state.clone(),
        })
    }

    fn validate(&self) -> Result<()> {
        // The 2/3 rule: a leader must finish renewing within
        // `renew_deadline`, which has to leave enough margin under
        // `lease_duration` for the next replica to safely treat the
        // lease as expired. Without this gap, a brief renewal hiccup
        // can race a takeover and produce two leaders.
        if self.renew_deadline >= self.lease_duration {
            return Err(InvalidConfig::RenewDeadlineNotLess {
                renew: self.renew_deadline,
                lease: self.lease_duration,
            }
            .into());
        }
        if self.retry_period > self.renew_deadline {
            return Err(InvalidConfig::RetryPeriodTooLong {
                retry: self.retry_period,
                renew: self.renew_deadline,
            }
            .into());
        }
        if self.retry_period.is_zero() {
            return Err(InvalidConfig::RetryPeriodZero.into());
        }
        if self.identity.is_empty() {
            return Err(InvalidConfig::IdentityEmpty.into());
        }
        if self.name.is_empty() {
            return Err(InvalidConfig::NameEmpty.into());
        }
        if self.namespace.is_empty() {
            return Err(InvalidConfig::NamespaceEmpty.into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Guard
// ---------------------------------------------------------------------------

/// Handle returned by [`LeaderElection::acquire`]. Keeps the lease renewed
/// until dropped or [`step_down`](LeaderGuard::step_down) is called.
/// Why a [`LeaderGuard`] stopped being the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StepDownReason {
    /// The renewal loop kept failing for longer than `renew_deadline`,
    /// almost certainly because the Kubernetes API was unreachable.
    /// Operators usually treat this as fatal and exit.
    RenewDeadlineExceeded,
    /// The Lease's `holder_identity` no longer matches ours — another
    /// instance took over. Operators should shut down their
    /// reconcilers; the new leader is already at work.
    HolderChanged,
    /// The renewal task was cancelled — typically because
    /// [`LeaderGuard::step_down`] was called or the guard was dropped
    /// before any other terminal condition was reached. Also surfaced
    /// if the renewal task panicked.
    Cancelled,
}

/// Active leader handle returned by [`LeaderElection::acquire`].
///
/// Hold the guard for the lifetime of your leader-only work. Dropping
/// it (or calling [`step_down`](Self::step_down)) aborts the renewal
/// task; [`lost`](Self::lost) resolves with a [`StepDownReason`] when
/// you are no longer leader.
#[non_exhaustive]
pub struct LeaderGuard {
    client: Client,
    namespace: String,
    name: String,
    identity: String,
    rx: watch::Receiver<Option<StepDownReason>>,
    renew_handle: Option<tokio::task::JoinHandle<()>>,
    state: LeaderState,
}

impl LeaderGuard {
    /// Wait until this instance is no longer leader, returning the
    /// reason.
    ///
    /// Resolves once the renewal task has signalled stepdown for any
    /// of the cases enumerated by [`StepDownReason`]. Returns
    /// immediately on subsequent calls — idempotent — so it is safe
    /// to use in `tokio::select!` alongside your controller futures.
    ///
    /// The shared [`LeaderState`] handle is flipped to
    /// `is_leader() == false` **before** this resolves, so any HTTP
    /// `/readyz` probe gated on that state reports "not ready"
    /// before the user-level shutdown handler runs.
    ///
    /// # Cancel safety
    ///
    /// This future is cancel-safe. Dropping it has no effect on the
    /// underlying renewal task or the [`LeaderState`] handle.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
    /// # use kunobi_ha::leader::{LeaderElection, StepDownReason};
    /// # let leader = LeaderElection::builder(client, "ns", "lease").build();
    /// # let mut guard = leader.acquire().await?;
    /// # async fn run_controllers() {}
    /// tokio::select! {
    ///     reason = guard.lost() => match reason {
    ///         StepDownReason::RenewDeadlineExceeded => { /* exit pod */ }
    ///         StepDownReason::HolderChanged       => { /* drain */ }
    ///         StepDownReason::Cancelled           => { /* user-driven */ }
    ///         _ => {}
    ///     },
    ///     _ = run_controllers() => {}
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn lost(&mut self) -> StepDownReason {
        loop {
            if let Some(reason) = self.rx.borrow().clone() {
                return reason;
            }
            if self.rx.changed().await.is_err() {
                // All senders dropped without writing a reason — the
                // renewal task was aborted (step_down / Drop) or it
                // panicked. From the caller's perspective, we're no
                // longer leader, and the state handle has already
                // been cleared by step_down_inner / Drop.
                return StepDownReason::Cancelled;
            }
        }
    }

    /// `true` while we still hold the lease.
    ///
    /// Equivalent to checking that [`Self::lost`] has not yet
    /// resolved. Cheap (one watch borrow + atomic compare); safe to
    /// poll from a hot path.
    ///
    /// For sharing the leader status with HTTP probes / metrics,
    /// prefer cloning a [`LeaderState`] handle obtained from
    /// [`LeaderElection::state`] — it doesn't require borrowing the
    /// guard.
    pub fn is_leader(&self) -> bool {
        self.rx.borrow().is_none()
    }

    /// Cooperatively relinquish the lease.
    ///
    /// Performs three steps, in order:
    ///
    /// 1. Flips the shared [`LeaderState`] to `is_leader() == false`
    ///    (synchronously, before any I/O).
    /// 2. Aborts the renewal task.
    /// 3. Best-effort PUT to clear `holder_identity` on the Lease
    ///    object. The next replica's polling loop sees the cleared
    ///    holder on its next tick and takes over within
    ///    `retry_period` instead of waiting for the full
    ///    `lease_duration` TTL. If the PUT fails (network blip
    ///    during shutdown is common), the lease still expires on its
    ///    own — recovery just takes longer.
    ///
    /// Consumes `self`. There is no way to resume the lease after
    /// stepping down — call [`LeaderElection::acquire`] again to
    /// re-elect.
    ///
    /// # Cancel safety
    ///
    /// **Not cancel-safe.** If this future is cancelled mid-PUT, the
    /// holder may not have been cleared on the API server even
    /// though the renewal task was aborted. The next replica still
    /// takes over, just on TTL expiry rather than immediately.
    pub async fn step_down(mut self) {
        self.step_down_inner().await;
    }

    async fn step_down_inner(&mut self) {
        if let Some(handle) = self.renew_handle.take() {
            // Flip the shared LeaderState BEFORE we abort and PUT, so
            // any HTTP `/readyz` handler sees "not ready" while the
            // cooperative-release request is still in flight.
            self.state.set(false);
            handle.abort();
            // Best-effort clear of holder_identity so the next replica
            // doesn't have to wait for the TTL. Any error is logged and
            // ignored; the lease will expire on its own in the worst case.
            let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
            if let Err(e) = clear_holder(&leases, &self.name, &self.identity).await {
                warn!(
                    %e, identity = %self.identity,
                    "failed to cooperatively release lease; will expire on TTL"
                );
            } else {
                info!(identity = %self.identity, "released leader lease cooperatively");
            }
        }
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        // On drop without explicit step_down (e.g. on panic), just abort
        // the renew loop and clear the shared state. Letting the TTL
        // expire is the safe fallback for the lease itself; the state
        // handle clears synchronously so probes update immediately.
        if let Some(handle) = self.renew_handle.take() {
            self.state.set(false);
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn chrono_to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> Result<k8s_openapi::jiff::Timestamp> {
    k8s_openapi::jiff::Timestamp::from_second(dt.timestamp())
        .map_err(|e| Error::Timestamp(e.to_string()))
}

fn timestamp_to_chrono(ts: &k8s_openapi::jiff::Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(ts.as_second(), 0)
}

/// Pure helper: has the lease passed its TTL relative to its last
/// renewal? Used by `try_acquire` to decide whether a lease held by
/// someone else is up for grabs.
///
/// Pulled out so the boundary (`>`, not `>=`) can be exercised
/// exhaustively without depending on real-clock timing.
fn chrono_lease_expired(
    now: chrono::DateTime<chrono::Utc>,
    renew_time: chrono::DateTime<chrono::Utc>,
    lease_duration: chrono::Duration,
) -> bool {
    now > renew_time + lease_duration
}

async fn try_acquire(
    leases: &Api<Lease>,
    name: &str,
    identity: &str,
    lease_duration: Duration,
) -> Result<bool> {
    let now = chrono::Utc::now();
    let micro_now = MicroTime(chrono_to_timestamp(now)?);
    let lease_secs = lease_duration.as_secs() as i32;

    match leases.get(name).await {
        Ok(mut existing) => {
            let spec = existing.spec.as_ref();
            let holder = spec.and_then(|s| s.holder_identity.as_deref());
            let renew_time = spec.and_then(|s| s.renew_time.as_ref());
            let lease_dur = spec
                .and_then(|s| s.lease_duration_seconds)
                .unwrap_or(lease_secs);

            if holder == Some(identity) {
                return renew_existing_lease(leases, name, existing, identity, lease_secs).await;
            }

            // No holder (or empty string) means a previous leader stepped
            // down cooperatively — take over immediately without waiting
            // for TTL. Otherwise, the lease is up for grabs only if the
            // last renewal is older than `lease_duration_seconds`.
            let unowned = holder.map(str::is_empty).unwrap_or(true);
            let expired = !unowned
                && match renew_time {
                    Some(MicroTime(t)) => match timestamp_to_chrono(t) {
                        Some(renew_chrono) => chrono_lease_expired(
                            now,
                            renew_chrono,
                            chrono::Duration::seconds(lease_dur as i64),
                        ),
                        None => {
                            warn!(
                                lease = name,
                                "lease has unrepresentable renew timestamp, treating as expired"
                            );
                            true
                        }
                    },
                    None => true,
                };

            if unowned || expired {
                let transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0) + 1;
                let spec = existing.spec.get_or_insert_with(Default::default);
                spec.holder_identity = Some(identity.to_string());
                spec.lease_duration_seconds = Some(lease_secs);
                spec.acquire_time = Some(micro_now.clone());
                spec.renew_time = Some(micro_now);
                spec.lease_transitions = Some(transitions);
                replace_lease(leases, name, existing).await
            } else {
                Ok(false)
            }
        }
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(name.to_string()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(identity.to_string()),
                    lease_duration_seconds: Some(lease_secs),
                    acquire_time: Some(micro_now.clone()),
                    renew_time: Some(micro_now),
                    lease_transitions: Some(0),
                    ..Default::default()
                }),
            };
            match leases.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(ae)) if ae.code == 409 => {
                    info!(lease = name, "leader lease was created by another instance");
                    Ok(false)
                }
                Err(e) => Err(e.into()),
            }
        }
        Err(e) => Err(e.into()),
    }
}

async fn renew_existing_lease(
    leases: &Api<Lease>,
    name: &str,
    mut lease: Lease,
    identity: &str,
    lease_secs: i32,
) -> Result<bool> {
    let now = MicroTime(chrono_to_timestamp(chrono::Utc::now())?);
    let spec = lease.spec.get_or_insert_with(Default::default);
    spec.holder_identity = Some(identity.to_string());
    spec.lease_duration_seconds = Some(lease_secs);
    spec.renew_time = Some(now);
    replace_lease(leases, name, lease).await
}

async fn replace_lease(leases: &Api<Lease>, name: &str, lease: Lease) -> Result<bool> {
    match leases.replace(name, &PostParams::default(), &lease).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(ae)) if ae.code == 409 => {
            info!(lease = name, "leader lease update conflicted; retrying");
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// Outcome of a renewal attempt. Distinct from a generic `bool` so that
/// the renew loop can tell "we definitely lost the lease" (Lost) apart
/// from a transient API error (Err), which it must retry within
/// `renew_deadline` rather than treat as a stepdown.
#[derive(Debug, PartialEq, Eq)]
enum RenewOutcome {
    /// The renewal PUT succeeded.
    Renewed,
    /// Another instance now holds the lease — we have lost it.
    Lost,
}

async fn renew_lease(
    leases: &Api<Lease>,
    name: &str,
    identity: &str,
    lease_secs: i32,
) -> Result<RenewOutcome> {
    let existing = leases.get(name).await?;
    let holder = existing
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref());
    if holder != Some(identity) {
        warn!(
            lease = name,
            holder = holder.unwrap_or("<none>"),
            identity = identity,
            "leader lease is held by another instance"
        );
        return Ok(RenewOutcome::Lost);
    }
    // PUT directly rather than going through `replace_lease`, which
    // collapses 409 into `Ok(false)`. For renewal we want 409 to
    // surface as `Err` so the renew loop retries within `renew_deadline`
    // instead of stepping down on a single optimistic-concurrency miss.
    let mut lease = existing;
    let now = MicroTime(chrono_to_timestamp(chrono::Utc::now())?);
    let spec = lease.spec.get_or_insert_with(Default::default);
    spec.holder_identity = Some(identity.to_string());
    spec.lease_duration_seconds = Some(lease_secs);
    spec.renew_time = Some(now);
    leases.replace(name, &PostParams::default(), &lease).await?;
    Ok(RenewOutcome::Renewed)
}

/// Clear the holder when we're the current holder. Used by cooperative
/// step-down so the next replica doesn't have to wait for TTL expiry.
async fn clear_holder(leases: &Api<Lease>, name: &str, identity: &str) -> Result<()> {
    let mut existing = leases.get(name).await?;
    let holder = existing
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref());
    if holder != Some(identity) {
        // Not ours to clear — someone else already took over.
        return Ok(());
    }
    let spec = existing.spec.get_or_insert_with(Default::default);
    spec.holder_identity = None;
    // Leave renew_time at its last value so the new leader can compute
    // expiry normally; the absence of `holder_identity` is sufficient
    // signal for immediate takeover.
    replace_lease(leases, name, existing).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn renew_loop(
    client: Client,
    namespace: &str,
    lease_name: &str,
    identity: &str,
    lease_duration: Duration,
    retry_period: Duration,
    renew_deadline: Duration,
    renew_request_timeout: Duration,
    state: LeaderState,
    tx: watch::Sender<Option<StepDownReason>>,
) {
    let leases: Api<Lease> = Api::namespaced(client, namespace);
    let lease_secs = lease_duration.as_secs() as i32;
    let mut interval = tokio::time::interval(retry_period);
    // Tick once immediately after acquire to avoid the first-tick burst
    // race with the initial create/replace.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_renew = tokio::time::Instant::now();

    // Helper: clear shared state BEFORE we publish the StepDownReason,
    // so any HTTP `/readyz` probe gated on `LeaderState::is_leader()`
    // sees the false transition before `LeaderGuard::lost().await`
    // resolves with the reason.
    let stepdown = |reason: StepDownReason| {
        state.set(false);
        let _ = tx.send(Some(reason));
    };

    loop {
        interval.tick().await;

        match tokio::time::timeout(
            renew_request_timeout,
            renew_lease(&leases, lease_name, identity, lease_secs),
        )
        .await
        {
            Ok(Ok(RenewOutcome::Renewed)) => {
                last_renew = tokio::time::Instant::now();
            }
            Ok(Ok(RenewOutcome::Lost)) => {
                warn!("lost leader lease — stepping down");
                stepdown(StepDownReason::HolderChanged);
                return;
            }
            Ok(Err(e)) => {
                warn!(%e, "failed to renew leader lease");
                if renew_deadline_exceeded(tokio::time::Instant::now(), last_renew, renew_deadline)
                {
                    warn!(
                        "renew deadline exceeded — stepping down after {:?} since last success",
                        renew_deadline
                    );
                    stepdown(StepDownReason::RenewDeadlineExceeded);
                    return;
                }
            }
            Err(_) => {
                warn!("timed out renewing leader lease");
                if renew_deadline_exceeded(tokio::time::Instant::now(), last_renew, renew_deadline)
                {
                    warn!(
                        "renew deadline exceeded — stepping down after {:?} since last success",
                        renew_deadline
                    );
                    stepdown(StepDownReason::RenewDeadlineExceeded);
                    return;
                }
            }
        }
    }
}

/// Pure helper: has the leader missed its renew window?
///
/// Pulled out of `renew_loop` so the boundary (`>`, not `>=`) can be
/// exercised exhaustively without depending on real tokio time.
fn renew_deadline_exceeded(
    now: tokio::time::Instant,
    last_renew: tokio::time::Instant,
    renew_deadline: Duration,
) -> bool {
    now.duration_since(last_renew) > renew_deadline
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // -----------------------------------------------------------------------
    // Pure function tests
    // -----------------------------------------------------------------------

    #[test]
    fn chrono_to_timestamp_now_ok() {
        assert!(chrono_to_timestamp(chrono::Utc::now()).is_ok());
    }

    #[test]
    fn chrono_to_timestamp_epoch_ok() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap();
        assert_eq!(chrono_to_timestamp(epoch).unwrap().as_second(), 0);
    }

    // -----------------------------------------------------------------------
    // chrono_lease_expired boundary
    // -----------------------------------------------------------------------

    #[test]
    fn chrono_lease_expired_strict_at_boundary() {
        // Exactly at the deadline: NOT expired (the condition is `>`,
        // not `>=`). A `>=` mutant would let two replicas race to
        // claim a lease at the exact instant of expiry.
        let renew = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let lease = chrono::Duration::seconds(15);
        let now = renew + lease;
        assert!(!chrono_lease_expired(now, renew, lease));
    }

    #[test]
    fn chrono_lease_expired_one_second_past() {
        let renew = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let lease = chrono::Duration::seconds(15);
        let now = renew + lease + chrono::Duration::seconds(1);
        assert!(chrono_lease_expired(now, renew, lease));
    }

    #[test]
    fn chrono_lease_expired_well_before() {
        let renew = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let lease = chrono::Duration::seconds(15);
        let now = renew + chrono::Duration::seconds(1);
        assert!(!chrono_lease_expired(now, renew, lease));
    }

    #[test]
    fn chrono_lease_expired_well_after() {
        let renew = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let lease = chrono::Duration::seconds(15);
        let now = renew + chrono::Duration::seconds(60);
        assert!(chrono_lease_expired(now, renew, lease));
    }

    // -----------------------------------------------------------------------
    // renew_deadline_exceeded boundary
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn renew_deadline_exceeded_strict_at_boundary() {
        // Exactly equal to renew_deadline: must NOT be considered
        // exceeded (the condition is `>`, not `>=`).
        let last = tokio::time::Instant::now();
        let renew_deadline = Duration::from_secs(10);
        let now = last + renew_deadline;
        assert!(!renew_deadline_exceeded(now, last, renew_deadline));
    }

    #[tokio::test]
    async fn renew_deadline_exceeded_just_past_boundary() {
        // One nanosecond past: exceeded.
        let last = tokio::time::Instant::now();
        let renew_deadline = Duration::from_secs(10);
        let now = last + renew_deadline + Duration::from_nanos(1);
        assert!(renew_deadline_exceeded(now, last, renew_deadline));
    }

    #[tokio::test]
    async fn renew_deadline_exceeded_well_below() {
        let last = tokio::time::Instant::now();
        let renew_deadline = Duration::from_secs(10);
        let now = last + Duration::from_secs(1);
        assert!(!renew_deadline_exceeded(now, last, renew_deadline));
    }

    #[tokio::test]
    async fn renew_deadline_exceeded_well_above() {
        let last = tokio::time::Instant::now();
        let renew_deadline = Duration::from_secs(10);
        let now = last + Duration::from_secs(60);
        assert!(renew_deadline_exceeded(now, last, renew_deadline));
    }

    #[test]
    fn timestamp_roundtrip_seconds() {
        let original = chrono::Utc::now();
        let secs = chrono::DateTime::from_timestamp(original.timestamp(), 0).unwrap();
        let ts = chrono_to_timestamp(secs).unwrap();
        assert_eq!(timestamp_to_chrono(&ts), Some(secs));
    }

    // -----------------------------------------------------------------------
    // Property-based invariants for the pure helpers
    // -----------------------------------------------------------------------
    //
    // proptest generates 256 random inputs per !prop and shrinks failing
    // cases to a minimal counter-example. This catches off-by-one and
    // overflow bugs that example-based tests miss because we'd never
    // think to enumerate them.

    use proptest::prelude::*;

    proptest! {
        /// `chrono_lease_expired` is `now > renew_time + lease_duration`.
        /// Encoded as the contract: equality at the deadline is NOT
        /// expired; one nanosecond past IS expired; arbitrary slack on
        /// either side preserves the result.
        #[test]
        fn prop_chrono_lease_expired_matches_definition(
            renew_seconds in 0_i64..2_000_000_000,
            lease_seconds in 1_i64..3600,
            slack_nanos in -10_000_000_000_i64..10_000_000_000,
        ) {
            let renew = chrono::DateTime::from_timestamp(renew_seconds, 0).unwrap();
            let lease = chrono::Duration::seconds(lease_seconds);
            let deadline = renew + lease;
            let now = deadline + chrono::Duration::nanoseconds(slack_nanos);

            let expected = slack_nanos > 0;
            prop_assert_eq!(chrono_lease_expired(now, renew, lease), expected);
        }

        /// At exact equality, `renew_deadline_exceeded` is false (the
        /// `>` boundary). Above by any amount, true. Below, false.
        #[test]
        fn prop_renew_deadline_exceeded_matches_definition(
            renew_secs in 1_u64..3600,
            slack_nanos in -10_000_000_000_i64..10_000_000_000,
        ) {
            let last = tokio::time::Instant::now();
            let renew = Duration::from_secs(renew_secs);
            let baseline = last + renew;
            let now = if slack_nanos >= 0 {
                baseline + Duration::from_nanos(slack_nanos as u64)
            } else {
                baseline - Duration::from_nanos((-slack_nanos) as u64)
            };

            let expected = slack_nanos > 0;
            prop_assert_eq!(renew_deadline_exceeded(now, last, renew), expected);
        }

        /// `validate` accepts iff every documented invariant holds:
        ///   renew_deadline < lease_duration
        ///   retry_period <= renew_deadline
        ///   retry_period > 0
        ///   identity / name / namespace non-empty
        /// Generates random duration tuples and asserts validate's verdict
        /// matches a hand-coded predicate.
        #[test]
        fn prop_validate_matches_documented_constraints(
            lease_ms in 1_u64..60_000,
            renew_ms in 0_u64..60_000,
            retry_ms in 0_u64..60_000,
        ) {
            let lease = Duration::from_millis(lease_ms);
            let renew = Duration::from_millis(renew_ms);
            let retry = Duration::from_millis(retry_ms);

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(async {
                let server = MockServer::start().await;
                let le = LeaderElection::builder(mock_client(&server), "ns", "lease")
                    .identity("me")
                    .lease_duration(lease)
                    .renew_deadline(renew)
                    .retry_period(retry)
                    .build();
                le.validate()
            });

            let valid = renew < lease && retry <= renew && !retry.is_zero();
            prop_assert_eq!(result.is_ok(), valid);
        }
    }

    // -----------------------------------------------------------------------
    // Wiremock-based integration tests
    // -----------------------------------------------------------------------

    fn mock_client(server: &MockServer) -> Client {
        // rustls 0.23+ needs a crypto provider installed exactly once.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = kube::Config::new(
            server
                .uri()
                .parse::<http::Uri>()
                .expect("mock server uri must be valid"),
        );
        Client::try_from(config).expect("build mock kube client")
    }

    // -----------------------------------------------------------------------
    // Config validation
    // -----------------------------------------------------------------------

    fn election_with(
        client: Client,
        lease_duration: Duration,
        renew_deadline: Duration,
        retry_period: Duration,
    ) -> LeaderElection {
        LeaderElection::builder(client, "ns", "lease")
            .identity("me")
            .lease_duration(lease_duration)
            .renew_deadline(renew_deadline)
            .retry_period(retry_period)
            .build()
    }

    #[tokio::test]
    async fn validate_rejects_renew_deadline_ge_lease_duration() {
        let server = MockServer::start().await;
        let le = election_with(
            mock_client(&server),
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(2),
        );
        let err = le.acquire().await.err().expect("must reject renew>=lease");
        assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn validate_rejects_retry_gt_renew_deadline() {
        let server = MockServer::start().await;
        let le = election_with(
            mock_client(&server),
            Duration::from_secs(15),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        let err = le.acquire().await.err().expect("must reject retry>renew");
        assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn validate_rejects_zero_retry_period() {
        let server = MockServer::start().await;
        let le = election_with(
            mock_client(&server),
            Duration::from_secs(15),
            Duration::from_secs(10),
            Duration::ZERO,
        );
        let err = le.acquire().await.err().expect("must reject zero retry");
        assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn validate_rejects_renew_equals_lease_exact_boundary() {
        // Pin the `>=` boundary: equality is rejected, not just `>`. Without
        // this case, a mutant flipping `>=` to `>` would survive.
        let server = MockServer::start().await;
        let le = election_with(
            mock_client(&server),
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(1),
        );
        assert!(le.validate().is_err(), "renew == lease must reject");
    }

    #[tokio::test]
    async fn validate_accepts_retry_equals_renew_exact_boundary() {
        // Pin the `>` boundary: retry == renew is allowed (only `>` rejects).
        // Without this case, a mutant flipping `>` to `>=` would survive.
        let server = MockServer::start().await;
        let le = election_with(
            mock_client(&server),
            Duration::from_secs(15),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
        assert!(le.validate().is_ok(), "retry == renew must be allowed");
    }

    #[tokio::test]
    async fn validate_accepts_default_config() {
        // Defaults must always validate, otherwise the docs lie.
        let server = MockServer::start().await;
        let le = LeaderElection::builder(mock_client(&server), "ns", "lease")
            .identity("me")
            .build();
        assert!(le.validate().is_ok());
    }

    #[tokio::test]
    async fn acquire_observe_timeout_waits_for_full_window() {
        // Persistent 5xx from the API. `acquire` must return
        // `ObserveTimeout` only AFTER ~observe_timeout has elapsed —
        // not on the first error. Catches:
        //   - `+ with -` mutant on `now + observe_timeout`: deadline
        //     ends up in the past, ObserveTimeout fires immediately.
        //   - `>= with <` mutant on `now >= *deadline`: condition
        //     starts true (now < now + observe_timeout), so
        //     ObserveTimeout fires on the first error.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                "reason": "InternalError"
            })))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .lease_duration(Duration::from_millis(100))
            .renew_deadline(Duration::from_millis(50))
            .retry_period(Duration::from_millis(10))
            .observe_timeout(Duration::from_millis(150))
            .build();

        let start = tokio::time::Instant::now();
        let result = leader.acquire().await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "must error after sustained 5xx");
        assert!(
            matches!(result.as_ref().err().unwrap(), Error::ObserveTimeout(_, _)),
            "got {:?}",
            result.err()
        );
        // Original elapsed: ~observe_timeout (150ms+). Mutants fire
        // on first error (~10-50ms). 100ms threshold is well-below
        // the original and well-above any mutant's fast-bail.
        assert!(
            elapsed >= Duration::from_millis(100),
            "ObserveTimeout returned too early ({elapsed:?}) — must wait near observe_timeout"
        );
    }

    // -----------------------------------------------------------------------
    // try_acquire / renew_lease against a mocked Kubernetes API
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn try_acquire_propagates_get_500_as_err() {
        // Differentiates the `ae.code == 404` match guard from `true`:
        // a 5xx must propagate, not be silently treated as "lease absent".
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                "reason": "InternalError"
            })))
            .mount(&server)
            .await;

        let leases: Api<Lease> = Api::namespaced(mock_client(&server), "test-ns");
        let err = try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
            .await
            .expect_err("500 must surface as Err, not Ok(false)");
        // Pin the specific upstream status code. Without this, a mutant
        // flipping the `ae.code == 404` match guard to `true` (which
        // would route a 500 GET error through the create path → POST →
        // wiremock default 404 → Err(Api(404))) survives because both
        // original and mutant are still `Error::Kube(_)`.
        match err {
            Error::Kube(kube::Error::Api(ae)) => {
                assert_eq!(
                    ae.code, 500,
                    "must propagate the original 500, not a downstream 404"
                );
            }
            other => panic!("expected kube Api error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_acquire_propagates_post_500_as_err() {
        // GET returns 404 (no lease) so we attempt POST. A 5xx on POST
        // must propagate, not be silently treated as "another instance
        // beat us" (which is what 409 means).
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404,
                "reason": "NotFound"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                "reason": "InternalError"
            })))
            .mount(&server)
            .await;

        let leases: Api<Lease> = Api::namespaced(mock_client(&server), "test-ns");
        let err = try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
            .await
            .expect_err("POST 500 must surface as Err");
        assert!(matches!(err, Error::Kube(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn try_acquire_propagates_put_500_as_err() {
        // GET shows we are the holder, so try_acquire calls
        // `renew_existing_lease` -> `replace_lease` (PUT). A 5xx on PUT
        // must propagate, not be silently treated as 409 (conflict).
        let server = MockServer::start().await;
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "renewTime": now_iso
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                "reason": "InternalError"
            })))
            .mount(&server)
            .await;

        let leases: Api<Lease> = Api::namespaced(mock_client(&server), "test-ns");
        let err = try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
            .await
            .expect_err("PUT 500 must surface as Err");
        assert!(matches!(err, Error::Kube(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // LeaderGuard public API
    // -----------------------------------------------------------------------

    fn lease_held_by(identity: &str) -> serde_json::Value {
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "my-lease", "namespace": "test-ns"},
            "spec": {
                "holderIdentity": identity,
                "leaseDurationSeconds": 15,
                "renewTime": now_iso,
            }
        })
    }

    #[tokio::test]
    async fn guard_step_down_completes_full_lifecycle() {
        // Acquire -> is_leader true -> step_down. Verifies the public
        // entry points (`acquire`, `is_leader`, `step_down`,
        // `step_down_inner`, `Drop`) all run real work; mutants that
        // replace any of them with `()` would let the test pass only by
        // accident, and these assertions catch the most obvious cases.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .build();
        let guard = leader.acquire().await.expect("acquire must succeed");
        assert!(guard.is_leader(), "is_leader must be true post-acquire");
        guard.step_down().await;

        // step_down's contract is to send a PUT that clears
        // holder_identity so the next replica takes over without
        // waiting for TTL. Verify the mock observed at least one PUT
        // whose body lacks a `holderIdentity` field — k8s-openapi
        // serialises `Option::None` by omitting the field. Without
        // this assertion, mutants replacing `step_down` /
        // `step_down_inner` / `Drop` with `()` survive.
        let cleared = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.method.as_str() == "PUT")
            .filter_map(|r| serde_json::from_slice::<serde_json::Value>(&r.body).ok())
            .any(|v| {
                v.get("spec")
                    .and_then(|s| s.as_object())
                    .map(|obj| {
                        let h = obj.get("holderIdentity");
                        h.is_none() || h.is_some_and(serde_json::Value::is_null)
                    })
                    .unwrap_or(false)
            });
        assert!(
            cleared,
            "step_down must send a PUT with cleared holder_identity"
        );
    }

    #[tokio::test]
    async fn guard_drop_aborts_renew_task() {
        // Verifies that dropping the guard stops the renew loop. Without
        // this, a mutant replacing `<impl Drop for LeaderGuard>::drop`
        // with `()` survives — the renew task would keep PUTting in the
        // background indefinitely.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .lease_duration(Duration::from_millis(200))
            .renew_deadline(Duration::from_millis(100))
            .retry_period(Duration::from_millis(20))
            .build();

        let guard = leader.acquire().await.expect("acquire must succeed");
        // Let the renew loop's first tick complete so `baseline` is
        // stable.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let count_puts = || async {
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .filter(|r| r.method.as_str() == "PUT")
                .count()
        };

        let baseline = count_puts().await;
        drop(guard);

        // Wait several retry_periods. Original Drop: handle.abort()
        // halts the loop, so no new PUTs. Mutant Drop -> (): the loop
        // keeps ticking and we'd see retry_period-pace PUTs continuing.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let after = count_puts().await;

        assert!(
            after.saturating_sub(baseline) <= 1,
            "Drop must abort the renew task; observed {} extra PUTs in 200ms after drop",
            after.saturating_sub(baseline)
        );
    }

    #[tokio::test]
    async fn guard_lost_fires_when_renew_observes_other_holder() {
        // Acquire -> renew loop sees a different holder -> renew_lease
        // returns Lost -> tx.send(false) -> guard.lost() resolves and
        // is_leader flips to false. Exercises the renew-loop deadline
        // and `lost`/`is_leader` mutants that the all-`true` returns
        // would let pass.
        let server = MockServer::start().await;
        // Initial GET: held by us — try_acquire goes through the
        // own-renew path successfully.
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Subsequent GETs: held by someone else — renew sees Lost.
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("other")))
            .mount(&server)
            .await;
        // PUT (initial own-renew): success.
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .lease_duration(Duration::from_millis(200))
            .renew_deadline(Duration::from_millis(100))
            .retry_period(Duration::from_millis(20))
            .renew_request_timeout(Duration::from_millis(50))
            .build();
        let mut guard = leader.acquire().await.expect("acquire must succeed");
        assert!(guard.is_leader(), "must be leader post-acquire");

        let reason = tokio::time::timeout(Duration::from_secs(2), guard.lost())
            .await
            .expect("lost() must fire when renew sees a different holder");
        assert_eq!(reason, StepDownReason::HolderChanged);
        assert!(!guard.is_leader(), "is_leader must be false post-stepdown");
    }

    #[tokio::test]
    async fn guard_lost_returns_renew_deadline_exceeded_on_sustained_failure() {
        // Acquire successfully, then PUT keeps 500-ing; once
        // renew_deadline is exceeded the renew loop sends
        // RenewDeadlineExceeded.
        let server = MockServer::start().await;
        // Initial GET (during try_acquire): we are holder.
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;
        // Initial PUT (own-renew during try_acquire): success.
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Subsequent PUTs (renew loop): always 500.
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                "reason": "InternalError"
            })))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .lease_duration(Duration::from_millis(200))
            .renew_deadline(Duration::from_millis(100))
            .retry_period(Duration::from_millis(20))
            .renew_request_timeout(Duration::from_millis(50))
            .build();
        let mut guard = leader.acquire().await.expect("acquire must succeed");
        assert!(guard.is_leader());

        let reason = tokio::time::timeout(Duration::from_secs(2), guard.lost())
            .await
            .expect("lost() must fire when renew_deadline is exceeded");
        assert_eq!(reason, StepDownReason::RenewDeadlineExceeded);
    }

    #[test]
    fn invalid_config_carries_typed_values() {
        // Pin that the new typed `InvalidConfig` enum carries the
        // offending durations rather than just a string. Caller-side
        // pattern matching depends on this.
        let err: Error = InvalidConfig::RenewDeadlineNotLess {
            renew: Duration::from_secs(10),
            lease: Duration::from_secs(10),
        }
        .into();
        assert!(matches!(
            err,
            Error::InvalidConfig(InvalidConfig::RenewDeadlineNotLess { .. })
        ));
    }

    #[test]
    fn identity_resolve_custom_passes_through() {
        assert_eq!(Identity::Custom("me".into()).resolve(), "me");
    }

    #[test]
    fn identity_resolve_generated_is_uuid_shaped() {
        let s = Identity::Generated.resolve();
        assert_eq!(s.len(), 36, "UUID v4 string is 36 chars: {s}");
        assert_eq!(s.matches('-').count(), 4);
    }

    #[test]
    fn identity_default_is_pod_name_or_uuid() {
        assert!(matches!(Identity::default(), Identity::PodNameOrUuid));
    }

    // -----------------------------------------------------------------------
    // LeaderState lifecycle invariants
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn leader_state_starts_false_before_acquire() {
        let server = MockServer::start().await;
        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .build();
        assert!(!leader.state().is_leader());
    }

    #[tokio::test]
    async fn leader_state_clones_share_underlying_state() {
        // The clonable handle's contract: every clone observes the
        // same atomic. Without this, gating /readyz from a copy held
        // by an HTTP server doesn't work.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .build();
        let probe_handle = leader.state();
        let internal_handle = leader.state();
        assert!(!probe_handle.is_leader());
        assert!(!internal_handle.is_leader());

        let guard = leader.acquire().await.expect("acquire must succeed");
        assert!(probe_handle.is_leader(), "every clone must see leader=true");
        assert!(internal_handle.is_leader());
        guard.step_down().await;
        assert!(
            !probe_handle.is_leader(),
            "every clone must see leader=false after step_down"
        );
    }

    #[tokio::test]
    async fn leader_state_clears_on_guard_drop() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .build();
        let probe = leader.state();

        let guard = leader.acquire().await.expect("acquire must succeed");
        assert!(probe.is_leader());
        drop(guard);
        // Drop is synchronous; the state must be cleared by the time
        // the line above returns control.
        assert!(!probe.is_leader(), "Drop must clear shared LeaderState");
    }

    #[tokio::test]
    async fn leader_state_clears_before_lost_resolves() {
        // The headline guarantee: an HTTP `/readyz` handler reading
        // `LeaderState::is_leader()` must observe `false` BEFORE
        // user-level code awaiting `lost()` runs. Otherwise the user
        // sees "we lost the lease" but probes still answer 200,
        // sending traffic to a non-leader for the brief window
        // between renew_loop's signal and the user's shutdown handler.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("other")))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lease_held_by("me")))
            .mount(&server)
            .await;

        let leader = LeaderElection::builder(mock_client(&server), "test-ns", "my-lease")
            .identity("me")
            .lease_duration(Duration::from_millis(200))
            .renew_deadline(Duration::from_millis(100))
            .retry_period(Duration::from_millis(20))
            .renew_request_timeout(Duration::from_millis(50))
            .build();
        let probe = leader.state();
        let mut guard = leader.acquire().await.expect("acquire must succeed");
        assert!(probe.is_leader());

        let reason = tokio::time::timeout(Duration::from_secs(2), guard.lost())
            .await
            .expect("lost() must fire when renew sees a different holder");
        assert_eq!(reason, StepDownReason::HolderChanged);
        assert!(
            !probe.is_leader(),
            "LeaderState must be cleared BEFORE lost() resolves"
        );
    }

    // -----------------------------------------------------------------------
    // Original try_acquire wiremock tests (kept verbatim)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn try_acquire_creates_lease_when_absent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404,
                "reason": "NotFound", "message": "not found"
            })))
            .mount(&server)
            .await;
        // Body matchers below pin every static field of the create
        // payload. If any field is dropped from the LeaseSpec / metadata
        // struct expression, body_partial_json fails to match and the
        // mock silently 404s — try_acquire returns Err, the test fails.
        // body_string_contains covers the dynamic timestamp fields
        // (acquireTime / renewTime) by checking for the key presence.
        Mock::given(method("POST"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases",
            ))
            .and(body_partial_json(serde_json::json!({
                "metadata": {"name": "my-lease"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 0
                }
            })))
            .and(body_string_contains("acquireTime"))
            .and(body_string_contains("renewTime"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "me", "leaseDurationSeconds": 15}
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        let got = try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION).await;
        assert!(got.unwrap(), "should acquire by creating the lease");
    }

    #[tokio::test]
    async fn try_acquire_renews_own_lease() {
        let server = MockServer::start().await;
        let now = chrono::Utc::now();
        let now_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "renewTime": now_iso
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "me", "leaseDurationSeconds": 15}
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn try_acquire_waits_when_others_hold_non_expired() {
        let server = MockServer::start().await;
        // Lease is held by "other", renewed just now — not expired.
        let now = chrono::Utc::now();
        let now_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "other",
                    "leaseDurationSeconds": 15,
                    "renewTime": now_iso
                }
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            !try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn try_acquire_takes_over_expired_lease() {
        let server = MockServer::start().await;
        // Lease held by "other" but renewed 60s ago with 15s TTL → expired.
        let long_ago = (chrono::Utc::now() - chrono::Duration::seconds(60))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "other",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 3,
                    "renewTime": long_ago
                }
            })))
            .mount(&server)
            .await;
        // Pin every static field of the takeover PUT. lease_transitions
        // bumps from 3 (existing) to 4. Dynamic timestamps are checked
        // by key presence.
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .and(body_partial_json(serde_json::json!({
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 4
                }
            })))
            .and(body_string_contains("acquireTime"))
            .and(body_string_contains("renewTime"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 4
                }
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn try_acquire_takes_over_when_holder_cleared() {
        // Cooperative step-down leaves renew_time fresh but clears
        // holder_identity. Next replica must take over immediately
        // without waiting for TTL — the whole point of step_down.
        let server = MockServer::start().await;
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": null,
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 7,
                    "renewTime": now_iso
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 15,
                    "leaseTransitions": 8
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap(),
            "must take over a lease whose holder_identity has been cleared"
        );
    }

    #[tokio::test]
    async fn try_acquire_returns_false_on_replace_409_during_takeover() {
        // Pre-existing expired lease held by "other"; we attempt a PUT
        // takeover which 409s (another replica beat us). `replace_lease`
        // must collapse the 409 into Ok(false) so the acquire loop can
        // retry on the next tick. Without this test, a mutant flipping
        // `match guard ae.code == 409` to `false` (which would make 409
        // fall through to `Err(e.into())`) survives.
        let server = MockServer::start().await;
        let long_ago = (chrono::Utc::now() - chrono::Duration::seconds(60))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {
                    "holderIdentity": "other",
                    "leaseDurationSeconds": 15,
                    "renewTime": long_ago
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 409,
                "reason": "Conflict"
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            !try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap(),
            "409 on takeover PUT must yield Ok(false), not Err"
        );
    }

    #[tokio::test]
    async fn try_acquire_returns_false_on_create_409() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404,
                "reason": "NotFound"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases",
            ))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 409,
                "reason": "AlreadyExists"
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert!(
            !try_acquire(&leases, "my-lease", "me", DEFAULT_LEASE_DURATION)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn renew_lease_returns_lost_when_holder_changed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "other", "leaseDurationSeconds": 15}
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        assert_eq!(
            renew_lease(&leases, "my-lease", "me", 15).await.unwrap(),
            RenewOutcome::Lost,
        );
    }

    #[tokio::test]
    async fn renew_lease_propagates_put_409_as_err() {
        // 409 on PUT during renewal must surface as an error so the
        // renew loop retries within `renew_deadline`. Treating it as
        // `Lost` would step the leader down on a single transient
        // optimistic-concurrency miss.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "me", "leaseDurationSeconds": 15}
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 409,
                "reason": "Conflict", "message": "the object has been modified"
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        let err = renew_lease(&leases, "my-lease", "me", 15)
            .await
            .expect_err("409 must be Err, not Ok(Lost)");
        assert!(
            matches!(err, Error::Kube(_)),
            "expected kube api error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn clear_holder_skips_when_not_owner() {
        let server = MockServer::start().await;
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path("/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "other", "leaseDurationSeconds": 15, "renewTime": now_iso}
            })))
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        // Should NOT PUT since we're not the holder.
        clear_holder(&leases, "my-lease", "me").await.unwrap();
    }

    #[tokio::test]
    async fn clear_holder_patches_when_owner() {
        let server = MockServer::start().await;
        let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("GET"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"holderIdentity": "me", "leaseDurationSeconds": 15, "renewTime": now_iso}
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/my-lease",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "my-lease", "namespace": "test-ns"},
                "spec": {"leaseDurationSeconds": 15, "renewTime": now_iso}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = mock_client(&server);
        let leases: Api<Lease> = Api::namespaced(client, "test-ns");
        clear_holder(&leases, "my-lease", "me").await.unwrap();
    }
}
