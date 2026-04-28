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
kunobi-ha = { version = "0.4", features = ["v1_31"] }
```

Available proxy features: `v1_31`, `v1_32`, `v1_33`, `v1_34`, `v1_35`,
`latest`. Pick the **minimum** Kubernetes API version your operator
needs to support. If you already depend on `k8s-openapi` directly with
a `v1_xx` feature enabled, you don't need a proxy feature here.

## Leader election

```rust
use kunobi_ha::leader::{LeaderElection, StepDownReason};
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

    // `guard.lost()` resolves with a `StepDownReason` when we are no
    // longer leader: sustained renewal failures, another instance
    // taking over, or the renewal task being cancelled by step_down /
    // guard drop.
    tokio::select! {
        reason = guard.lost() => {
            eprintln!("lost leader lease ({reason:?}), shutting down");
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

A minimal runnable demo lives at [`examples/leader.rs`](examples/leader.rs):

```bash
KUNOBI_NS=default KUNOBI_LEASE=demo cargo run --example leader
```

Run two copies in separate terminals to watch one stand by while the
other holds the lease, then `Ctrl-C` the leader to see takeover.

### Timing model

Follows the Kubernetes reference (`leaderelection.LeaderElectionConfig`):

| Parameter         | Default | Meaning                                                       |
|-------------------|---------|---------------------------------------------------------------|
| `lease_duration`  | 15s     | Lease expiry after last renewal                               |
| `renew_deadline`  | 10s     | Leader must renew within this window; otherwise it steps down |
| `retry_period`    | 2s      | Renewal cadence (leader) / poll cadence (follower)            |
| `observe_timeout` | 5m      | Initial-acquire loop bails if API is unreachable this long    |

`acquire()` rejects nonsensical configurations
(`renew_deadline >= lease_duration`, `retry_period > renew_deadline`,
zero `retry_period`, empty identity/name/namespace) with
`Error::InvalidConfig` rather than silently misbehaving.

### Behavioural notes

- **Cooperative step-down on shutdown.** `guard.step_down().await`
  clears `holder_identity` on the Lease so the next replica takes
  over within `retry_period` instead of waiting for the full TTL.
  Equivalent to client-go's `ReleaseOnCancel`, but always-on rather
  than opt-in. The acquire path also short-circuits when it sees a
  cleared holder, so even if the next replica doesn't notice
  mid-tick, the very next poll picks it up.
- **Transient-error tolerance during renewal.** A 5xx, HTTP timeout,
  or 409 on a renewal PUT does not immediately step down — only a
  sustained failure past `renew_deadline` does. The renew loop
  distinguishes "another instance took over" (`StepDownReason::HolderChanged`)
  from "API hiccup" (retried until `RenewDeadlineExceeded`).
- **Typed identity.** `Identity::PodNameOrUuid` (the default) reads
  `$HOSTNAME`, falling back to a UUID. `Identity::Generated` always
  generates a UUID. `Identity::Custom("…")` lets you pin a specific
  string. `&str` and `String` `Into<Identity>` automatically, so
  `.identity("foo")` still works.

## RBAC

Your controller's ServiceAccount needs the minimum verbs the crate
actually issues — `get` (read the Lease), `create` (first acquire when
the Lease doesn't exist yet), `update` (renew, take over, cooperative
step-down all use PUT):

```yaml
rules:
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    resourceNames: [my-operator]   # optional: tighten to the lease(s) you own
    verbs: [get, create, update]
```

`list`, `watch`, `patch`, `delete` are **not** required. If your
ServiceAccount already has them for other reasons that's fine, but
this crate doesn't issue any of those verbs.

## Testing

```bash
cargo test
```

Tests use `wiremock` to simulate the Kubernetes API — no real cluster
needed.

Dependency hygiene is checked with [`cargo-deny`](https://embarkstudios.github.io/cargo-deny/):

```bash
cargo deny check
```

Configuration lives in [`deny.toml`](deny.toml).

## Roadmap

Future modules as we see duplication pop up across Kunobi operators:

- `shutdown` — SIGTERM handler, drain budget, cancel token propagation
- `readiness` — `/healthz /readyz` with gates on leader status and
  dependency health
- `health_probe` — a minimal Axum server that bundles the above

## License

Apache-2.0

[`k8s-openapi`]: https://crates.io/crates/k8s-openapi
