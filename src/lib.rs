//! # kunobi-ha
//!
//! Building blocks for high-availability Kubernetes controllers.
//!
//! Today this crate ships one module — [`leader`], a Lease-based leader
//! election implementation following the pattern used by
//! `kube-controller-manager` and `kube-scheduler`. Future additions
//! (graceful shutdown helpers, readiness gates, unified health probe
//! servers) will land here as separate modules.
//!
//! ## Leader election quickstart
//!
//! ```no_run
//! # async fn run(client: kube::Client) -> kunobi_ha::Result<()> {
//! use kunobi_ha::leader::LeaderElection;
//!
//! let leader = LeaderElection::builder(client, "my-ns", "my-operator").build();
//! let mut guard = leader.acquire().await?;
//!
//! // Start your controllers here. `guard.lost()` fires when we lose
//! // the lease (renewal failed past `renew_deadline`).
//! // Call `guard.step_down().await` in your SIGTERM handler to let the
//! // next replica take over immediately instead of waiting for TTL.
//! # Ok(())
//! # }
//! ```
//!
//! See [`leader`] for the full API.

#![warn(missing_docs)]
// Pedantic lints are valuable but too noisy as hard errors for a small
// crate. Enable them advisory via `cargo clippy -- -W clippy::pedantic`
// locally; CI enforces default + warnings-as-errors.

pub mod error;
pub mod leader;

pub use error::{Error, Result};
