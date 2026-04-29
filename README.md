# kunobi-ha

[![Crates.io](https://img.shields.io/crates/v/kunobi-ha.svg)](https://crates.io/crates/kunobi-ha)
[![Docs.rs](https://img.shields.io/docsrs/kunobi-ha)](https://docs.rs/kunobi-ha)
[![CI](https://github.com/kunobi-ninja/kunobi-ha/actions/workflows/ci.yml/badge.svg)](https://github.com/kunobi-ninja/kunobi-ha/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.94.1-blue.svg)](Cargo.toml)

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
kunobi-ha = { version = "0.4.1", features = ["v1_31"] }
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

## Readiness gating

Only the leader replica should accept user traffic from the
Kubernetes service. The standard pattern is to gate `/readyz` on
leader status — `kunobi-ha` exposes a clonable `LeaderState` handle
so you don't have to roll your own `Arc<AtomicBool>`:

```rust
use kunobi_ha::leader::{LeaderElection, LeaderState};
use std::sync::Arc;

#[derive(Clone)]
struct AppState { leader: LeaderState /* … */ }

async fn readyz(state: AppState) -> http::StatusCode {
    if state.leader.is_leader() {
        http::StatusCode::OK
    } else {
        http::StatusCode::SERVICE_UNAVAILABLE
    }
}

# async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
let leader = LeaderElection::builder(client, "my-ns", "my-operator").build();
let state  = leader.state();   // get the handle BEFORE blocking on acquire

// Spawn the HTTP server first — `/readyz` returns 503 until acquire
// succeeds. This is the bug that bit umami v0.3.13: spawning the
// server AFTER `acquire().await` made the pod unreachable forever
// when it wasn't the leader.
let probe = state.clone();
tokio::spawn(async move {
    /* serve readyz with `probe` */
    # let _ = probe;
});

let mut guard = leader.acquire().await?;   // state flips to true here
// run reconcilers…
guard.step_down().await;                   // state flips to false
# Ok(()) }
```

The library guarantees that `LeaderState::is_leader()` flips to
`false` BEFORE `guard.lost().await` resolves — kubelet sees the
unready probe, removes the pod from service endpoints, and stops
sending traffic before your shutdown handler runs.

## Common pitfalls

- **Spawning the HTTP server after `acquire()`.** Standby replicas
  block forever on lease acquisition; nothing serves probes. Spawn
  the server first with a `LeaderState` clone, then call
  `acquire().await`.
- **Hand-rolling the readiness flag.** A separate `Arc<AtomicBool>`
  managed by user code can drift out of sync with the actual
  lease — typically by forgetting to clear it on `lost()` or panic.
  Use `LeaderState`; it's lifecycle-coupled to the renewal loop.
- **Treating `lost()` as fatal.** It's a signal, not an error.
  Inspect [`StepDownReason`]; e.g.
  `RenewDeadlineExceeded` warrants a pod restart, but
  `HolderChanged` just means another replica is now leader and you
  should drain gracefully.
- **Asking for too many RBAC verbs.** Only `get`, `create`, `update`
  are needed (see [RBAC](#rbac) below). Granting `list`, `watch`,
  `patch`, `delete` doesn't hurt the crate but violates
  least-privilege.

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

Tooling is pinned via [`mise`](https://mise.jdx.dev):

```bash
mise install
```

```bash
cargo test
```

Tests use `wiremock` to simulate the Kubernetes API — no real cluster
needed.

Dependency hygiene is checked with [`cargo-deny`](https://embarkstudios.github.io/cargo-deny/):

```bash
mise run deny
```

Configuration lives in [`deny.toml`](deny.toml).

Coverage is measured with [`cargo-tarpaulin`](https://github.com/xd009642/tarpaulin).
CI uses the same LLVM engine with a conservative initial `50%` floor and
uploads `coverage/tarpaulin-report.json` as an artifact.

```bash
mise run coverage       # JSON report in ./coverage
mise run coverage:html  # HTML report in ./coverage
```

Mutation testing is local-only because it is slower than the normal CI
path. The focused default mirrors the currently mutation-audited surface:

```bash
mise run mutants
mise run mutants:file -- src/leader.rs
```

## Roadmap

Future modules as we see duplication pop up across Kunobi operators:

- `shutdown` — SIGTERM handler, drain budget, cancel token propagation
- `readiness` — `/healthz /readyz` with gates on leader status and
  dependency health
- `health_probe` — a minimal Axum server that bundles the above

## License

Apache-2.0

[`k8s-openapi`]: https://crates.io/crates/k8s-openapi
