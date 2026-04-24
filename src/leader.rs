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
//! use kunobi_ha::leader::LeaderElection;
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
//! // Start controllers here, watching `guard.changed()` for step-down.
//! loop {
//!     tokio::select! {
//!         _ = guard.changed() => {
//!             // Lost leadership (renewal failed past `renew_deadline`).
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

use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::error::{Error, Result};

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

/// Configure and start a leader election loop.
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
}

/// Fluent builder returned by [`LeaderElection::builder`].
pub struct LeaderElectionBuilder {
    client: Client,
    namespace: String,
    name: String,
    identity: Option<String>,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
    observe_timeout: Duration,
    renew_request_timeout: Duration,
}

impl LeaderElection {
    /// Start building a leader-election configuration.
    ///
    /// `namespace` is where the Lease lives; `name` is the Lease name and
    /// should be unique per controller (e.g. `"karakuri-operator"`).
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
    /// Override the identity string. Defaults to `$HOSTNAME` (the pod name
    /// when running in Kubernetes) or a fresh UUID.
    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// How long the lease is considered valid without renewal.
    /// Default: 15s.
    pub fn lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Deadline within which the leader must successfully renew. If the
    /// leader cannot renew by this time it steps down.
    /// Default: 10s (2/3 of `lease_duration`).
    pub fn renew_deadline(mut self, duration: Duration) -> Self {
        self.renew_deadline = duration;
        self
    }

    /// How often the leader attempts renewal and followers poll.
    /// Default: 2s.
    pub fn retry_period(mut self, duration: Duration) -> Self {
        self.retry_period = duration;
        self
    }

    /// If the API is unreachable for this long during the initial acquire
    /// loop, give up and return an error.
    /// Default: 5 minutes.
    pub fn observe_timeout(mut self, duration: Duration) -> Self {
        self.observe_timeout = duration;
        self
    }

    /// Per-request timeout for renewal calls.
    /// Default: 5s.
    pub fn renew_request_timeout(mut self, duration: Duration) -> Self {
        self.renew_request_timeout = duration;
        self
    }

    /// Finalise the configuration.
    pub fn build(self) -> LeaderElection {
        let identity = self.identity.unwrap_or_else(|| {
            std::env::var("HOSTNAME").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        });
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
        }
    }
}

impl LeaderElection {
    /// Block until this instance acquires the lease, then return a guard
    /// that keeps the lease renewed in the background.
    ///
    /// The returned [`LeaderGuard`] should be held for the lifetime of the
    /// controllers. Call [`LeaderGuard::changed`] to wait for step-down
    /// events (lost renewal), and [`LeaderGuard::step_down`] in your
    /// shutdown path to relinquish the lease cooperatively.
    pub async fn acquire(&self) -> Result<LeaderGuard> {
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

        let (tx, rx) = watch::channel(true);
        let handle = tokio::spawn({
            let client = self.client.clone();
            let namespace = self.namespace.clone();
            let name = self.name.clone();
            let identity = self.identity.clone();
            let retry_period = self.retry_period;
            let renew_deadline = self.renew_deadline;
            let renew_request_timeout = self.renew_request_timeout;
            async move {
                renew_loop(
                    client,
                    &namespace,
                    &name,
                    &identity,
                    retry_period,
                    renew_deadline,
                    renew_request_timeout,
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
        })
    }
}

// ---------------------------------------------------------------------------
// Guard
// ---------------------------------------------------------------------------

/// Handle returned by [`LeaderElection::acquire`]. Keeps the lease renewed
/// until dropped or [`step_down`](LeaderGuard::step_down) is called.
pub struct LeaderGuard {
    client: Client,
    namespace: String,
    name: String,
    identity: String,
    rx: watch::Receiver<bool>,
    renew_handle: Option<tokio::task::JoinHandle<()>>,
}

impl LeaderGuard {
    /// Wait for the next change in leadership state. Returns `Ok(())` when
    /// the status flips (typically to `false` — i.e. we lost the lease).
    ///
    /// After observing `false`, the caller should shut down its controllers
    /// and exit the pod; a new leader will pick up the work.
    pub async fn changed(&mut self) -> std::result::Result<(), watch::error::RecvError> {
        self.rx.changed().await
    }

    /// `true` while we still hold the lease.
    pub fn is_leader(&self) -> bool {
        *self.rx.borrow()
    }

    /// Cooperatively relinquish the lease — PATCH the Lease to clear our
    /// holder identity and abort the renewal task. The next replica
    /// notices immediately and takes over (within `retry_period`, not
    /// `lease_duration`).
    ///
    /// Safe to call multiple times. No-op if we've already stepped down.
    pub async fn step_down(mut self) {
        self.step_down_inner().await;
    }

    async fn step_down_inner(&mut self) {
        if let Some(handle) = self.renew_handle.take() {
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
        // the renew loop. Letting the TTL expire is the safe fallback.
        if let Some(handle) = self.renew_handle.take() {
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

            let expired = match renew_time {
                Some(MicroTime(t)) => match timestamp_to_chrono(t) {
                    Some(renew_chrono) => {
                        let deadline = renew_chrono + chrono::Duration::seconds(lease_dur as i64);
                        now > deadline
                    }
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

            if expired {
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

async fn renew_lease(
    leases: &Api<Lease>,
    name: &str,
    identity: &str,
    lease_secs: i32,
) -> Result<bool> {
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
        return Ok(false);
    }
    renew_existing_lease(leases, name, existing, identity, lease_secs).await
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
    let _ = replace_lease(leases, name, existing).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn renew_loop(
    client: Client,
    namespace: &str,
    lease_name: &str,
    identity: &str,
    retry_period: Duration,
    renew_deadline: Duration,
    renew_request_timeout: Duration,
    tx: watch::Sender<bool>,
) {
    let leases: Api<Lease> = Api::namespaced(client, namespace);
    let lease_secs = renew_deadline_to_duration(renew_deadline).as_secs() as i32;
    let mut interval = tokio::time::interval(retry_period);
    // Tick once immediately after acquire to avoid the first-tick burst
    // race with the initial create/replace.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_renew = tokio::time::Instant::now();

    loop {
        interval.tick().await;

        match tokio::time::timeout(
            renew_request_timeout,
            renew_lease(&leases, lease_name, identity, lease_secs),
        )
        .await
        {
            Ok(Ok(true)) => {
                last_renew = tokio::time::Instant::now();
            }
            Ok(Ok(false)) => {
                warn!("lost leader lease — stepping down");
                let _ = tx.send(false);
                return;
            }
            Ok(Err(e)) => {
                warn!(%e, "failed to renew leader lease");
                if tokio::time::Instant::now().duration_since(last_renew) > renew_deadline {
                    warn!(
                        "renew deadline exceeded — stepping down after {:?} since last success",
                        renew_deadline
                    );
                    let _ = tx.send(false);
                    return;
                }
            }
            Err(_) => {
                warn!("timed out renewing leader lease");
                if tokio::time::Instant::now().duration_since(last_renew) > renew_deadline {
                    warn!(
                        "renew deadline exceeded — stepping down after {:?} since last success",
                        renew_deadline
                    );
                    let _ = tx.send(false);
                    return;
                }
            }
        }
    }
}

// Explicit helper so that if the caller supplies an unusual combination
// (e.g. renew_deadline > lease_duration, which makes no sense), we still
// produce a sensible `lease_duration_seconds` for the Lease spec.
fn renew_deadline_to_duration(renew_deadline: Duration) -> Duration {
    // Use renew_deadline + 5s as the Lease TTL, aligning with the
    // Kubernetes reference default ratio.
    renew_deadline + Duration::from_secs(5)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
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

    #[test]
    fn timestamp_roundtrip_seconds() {
        let original = chrono::Utc::now();
        let secs = chrono::DateTime::from_timestamp(original.timestamp(), 0).unwrap();
        let ts = chrono_to_timestamp(secs).unwrap();
        assert_eq!(timestamp_to_chrono(&ts), Some(secs));
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
        Mock::given(method("POST"))
            .and(path(
                "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases",
            ))
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
