//! Building blocks for **high-availability Kubernetes controllers**
//! written in Rust.
//!
//! Every Kubernetes operator with `replicaCount > 1` has to solve the
//! same set of problems: only one replica should reconcile at a time,
//! `/readyz` must answer `200` only on that replica so service traffic
//! lands there, and shutdown should be cooperative so the next replica
//! picks up in seconds rather than waiting for a Lease TTL. This crate
//! hosts the shared implementation once, with strong test coverage,
//! so each operator doesn't reinvent it.
//!
//! Today the crate ships one module — [`leader`], a Lease-based
//! leader-election implementation following the pattern used by
//! `kube-controller-manager` and `kube-scheduler`. Future additions
//! (graceful shutdown helpers, readiness gates, unified health probe
//! servers) will land here as separate modules.
//!
//! # Quickstart
//!
//! ```no_run
//! # async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
//! use kunobi_ha::leader::{LeaderElection, StepDownReason};
//!
//! let leader = LeaderElection::builder(client, "my-ns", "my-operator").build();
//!
//! // Get a clonable handle to leader status BEFORE blocking on
//! // acquisition. Wire this into your `/readyz` HTTP handler so it
//! // answers 503 on standby replicas and 200 once we become leader.
//! let state = leader.state();
//!
//! // Block until the lease is ours.
//! let mut guard = leader.acquire().await?;
//!
//! // `guard.lost()` resolves when we are no longer leader. Inspect
//! // the `StepDownReason` to decide between "exit and let kubelet
//! // restart us" (`RenewDeadlineExceeded`, the API was unreachable)
//! // and "drain gracefully" (`HolderChanged`, another replica took
//! // over).
//! match guard.lost().await {
//!     StepDownReason::RenewDeadlineExceeded => {} // API was unreachable
//!     StepDownReason::HolderChanged => {}         // another replica took over
//!     StepDownReason::Cancelled => {}             // step_down() / drop
//!     _ => {}                                     // future variants
//! }
//! # let _ = state;
//! # Ok(())
//! # }
//! ```
//!
//! See [`leader`] for the full API and the [`LeaderState`] section
//! for the readiness-gating contract. The crate's README has a
//! "Readiness gating" walk-through for the axum integration shape
//! and a "Common pitfalls" section listing the four mistakes most
//! consumers make on first integration.
//!
//! # Cargo features
//!
//! ## Kubernetes API version
//!
//! `kunobi-ha` follows the [`k8s-openapi`] convention: a binary crate
//! must pick exactly one Kubernetes API version. The crate re-exports
//! `k8s-openapi`'s flags as proxy features so consumers can pin a
//! version through `kunobi-ha` alone:
//!
//! - `v1_31`, `v1_32`, `v1_33`, `v1_34`, `v1_35`
//! - `latest` (currently aliased to `v1_35`; tracks `k8s-openapi/latest`)
//!
//! Pick the **minimum** API version your operator must support. If
//! you already depend on `k8s-openapi` directly with a `v1_xx`
//! feature enabled, you don't need a proxy feature here.
//!
//! `kunobi-ha = { version = "0.4", features = ["v1_31"] }`
//!
//! Library crates that depend on `kunobi-ha` MUST NOT enable any of
//! these features in their own `[dependencies]` — the choice of
//! Kubernetes version belongs to the final binary.
//!
//! [`k8s-openapi`]: https://crates.io/crates/k8s-openapi
//!
//! # When to use
//!
//! - You are writing a Kubernetes operator/controller that must run
//!   with multiple replicas for HA but where only one reconciles at
//!   a time.
//! - You want first-class typed errors, structured stepdown reasons,
//!   and a tested cooperative-step-down path rather than reimplementing
//!   `kube-rs`'s low-level Lease handling.
//!
//! # When NOT to use
//!
//! - You are writing a non-Kubernetes leader-election service —
//!   reach for [`raft`-style consensus](https://docs.rs/raft) or a
//!   purpose-built coordination service.
//! - You need leader election with custom storage (etcd directly,
//!   Redis, etc.). This crate is specifically for the
//!   `coordination.k8s.io/v1.Lease` resource.
//!
//! # MSRV
//!
//! The crate's `rust-version` tracks current stable. CI verifies the
//! library builds on the pinned MSRV image as well as latest stable.

#![warn(missing_docs)]
// Pedantic lints are valuable but too noisy as hard errors for a small
// crate. Enable them advisory via `cargo clippy -- -W clippy::pedantic`
// locally; CI enforces default + warnings-as-errors.

pub mod error;
pub mod leader;

pub use error::{Error, InvalidConfig, Result};
pub use leader::LeaderState;
