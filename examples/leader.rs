//! Minimal runnable example. Requires a reachable Kubernetes cluster
//! (kubeconfig or in-cluster) and RBAC for the `coordination.k8s.io/v1`
//! Lease resource — see the README for the policy.
//!
//! ```bash
//! KUNOBI_NS=default KUNOBI_LEASE=demo-leader cargo run --example leader
//! ```
//!
//! Run two copies in separate terminals to watch one stand by while the
//! other holds the lease, then `Ctrl-C` the leader to see takeover.
//!
//! Demonstrates two things:
//!
//! 1. Spawning a "probe responder" task BEFORE blocking on
//!    `leader.acquire().await`. In a real service this would be your
//!    HTTP `/readyz` handler; here it's just a logger that prints
//!    `is_leader()` every second. The point is that it must be alive
//!    on standby replicas, not gated on becoming leader.
//! 2. Reading [`LeaderState`] from the spawned task. The library owns
//!    the timing — the flag flips to `true` synchronously with
//!    `acquire()` returning, and back to `false` before
//!    `guard.lost().await` resolves.

use std::time::Duration;

use kunobi_ha::leader::{LeaderElection, LeaderState};
use tokio::signal;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let namespace = std::env::var("KUNOBI_NS").unwrap_or_else(|_| "default".into());
    let lease_name = std::env::var("KUNOBI_LEASE").unwrap_or_else(|_| "kunobi-ha-example".into());

    let client = kube::Client::try_default().await?;

    let leader = LeaderElection::builder(client, namespace, lease_name)
        .lease_duration(Duration::from_secs(15))
        .renew_deadline(Duration::from_secs(10))
        .retry_period(Duration::from_secs(2))
        .build();

    // Get the readiness handle BEFORE blocking on acquire so the
    // probe task can run on standby replicas too. Real services
    // would wire this into an axum/actix `/readyz` handler instead.
    let probe_state: LeaderState = leader.state();
    tokio::spawn(probe_loop(probe_state));

    info!("waiting for leader lease");
    let mut guard = leader.acquire().await?;
    info!("became leader");

    tokio::select! {
        reason = guard.lost() => {
            info!(?reason, "lost lease, exiting so the next replica can take over");
        }
        res = signal::ctrl_c() => {
            res?;
            info!("ctrl-c, stepping down cooperatively");
            guard.step_down().await;
        }
    }

    Ok(())
}

/// Stand-in for an HTTP `/readyz` handler. In production this would
/// translate `is_leader()` into a 200 / 503 response; here we just
/// log the status so you can see the lifecycle in the test output.
async fn probe_loop(state: LeaderState) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        if state.is_leader() {
            info!(ready = true, "probe: leader, /readyz would return 200");
        } else {
            info!(ready = false, "probe: standby, /readyz would return 503");
        }
    }
}
