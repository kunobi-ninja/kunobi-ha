# kunobi-ha

Building blocks for high-availability Kubernetes controllers written in Rust.

Today this crate ships one module — `leader`, a Lease-based leader election
implementation following the pattern used by `kube-controller-manager` and
`kube-scheduler`. Future additions (graceful shutdown helpers, readiness
gates, unified health probes) will land here as separate modules.

## Why

Every Kubernetes operator with `replicaCount > 1` has to solve the same
problem: only one replica should actively reconcile at a time. Copying the
same 300-line Lease dance into every operator leads to subtle bugs and
drift over time. This crate hosts the shared implementation once, with
real tests.

## Installation

Like every crate that depends on [`k8s-openapi`], the final binary must
pick the Kubernetes API version it targets. `kunobi-ha` re-exports
`k8s-openapi`'s version flags as proxy features, so you can do this from
your own `Cargo.toml`:

```toml
[dependencies]
kunobi-ha = { version = "0.1", features = ["v1_31"] }
```

Available proxy features: `v1_31`, `v1_32`, `v1_33`, `v1_34`, `v1_35`,
`latest`. Pick the **minimum** Kubernetes API version your operator
needs to support. If you already depend on `k8s-openapi` directly with
a `v1_xx` feature enabled, you don't need a proxy feature here.

## Leader election

```rust
use kunobi_ha::leader::LeaderElection;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = kube::Client::try_default().await?;

    let leader = LeaderElection::builder(client, "my-ns", "my-operator")
        .lease_duration(Duration::from_secs(15))
        .renew_deadline(Duration::from_secs(10))  // 2/3 rule
        .retry_period(Duration::from_secs(2))
        .build();

    // Block until we're leader.
    let mut guard = leader.acquire().await?;

    // Start your controllers here. `guard.changed()` fires when we lose
    // the lease (renewal failed past `renew_deadline`).
    tokio::select! {
        _ = guard.changed() => {
            eprintln!("lost leader lease, shutting down");
        }
        _ = run_my_controllers() => {}
    }

    // Cooperative step-down — the next replica picks up within
    // `retry_period`, not `lease_duration`.
    guard.step_down().await;

    Ok(())
}
# async fn run_my_controllers() {}
```

### Timing model

Follows the Kubernetes reference (`leaderelection.LeaderElectionConfig`):

| Parameter         | Default | Meaning                                                       |
|-------------------|---------|---------------------------------------------------------------|
| `lease_duration`  | 15s     | Lease expiry after last renewal                               |
| `renew_deadline`  | 10s     | Leader must renew within this window; otherwise it steps down |
| `retry_period`    | 2s      | Renewal cadence (leader) / poll cadence (follower)            |
| `observe_timeout` | 5m      | Initial-acquire loop bails if API is unreachable this long    |

### Key differences from the Kubernetes reference

- **Cooperative step-down on shutdown.** `guard.step_down().await` clears
  `holder_identity` on the Lease so the next replica takes over within
  `retry_period` instead of waiting for the full TTL. Most reference
  implementations (including the original Go one) leave this optional —
  we make it ergonomic, and the acquire path notices a cleared holder
  immediately rather than waiting for renewal expiry.
- **Deadline-based, not failure-count-based.** A leader steps down when
  it has been unable to renew for longer than `renew_deadline`, not
  after N consecutive failures. Handles flappy networks more gracefully.

## RBAC

Your controller's ServiceAccount needs:

```yaml
rules:
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    verbs: [get, list, watch, create, update, patch, delete]
```

## Testing

```bash
cargo test
```

Tests use `wiremock` to simulate the Kubernetes API — no real cluster
needed.

## Roadmap

Future modules as we see duplication pop up across Kunobi operators:

- `shutdown` — SIGTERM handler, drain budget, cancel token propagation
- `readiness` — `/healthz /readyz` with gates on leader status and
  dependency health
- `health_probe` — a minimal Axum server that bundles the above

## License

Apache-2.0

[`k8s-openapi`]: https://crates.io/crates/k8s-openapi
