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

use std::time::Duration;

use kunobi_ha::leader::LeaderElection;
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
