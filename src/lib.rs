//! # skiff
//!
//! An embedded [Raft](https://raft.github.io/) consensus library backed by
//! [sled](https://docs.rs/sled).  Skiff lets you embed a replicated key-value
//! store directly in your application without running a separate consensus
//! service.
//!
//! ## Architecture
//!
//! Each participating process runs a [`Skiff`] node.  Nodes elect a leader
//! among themselves using the Raft protocol.  All writes are routed to the
//! leader, replicated to a majority of followers, and only acknowledged once
//! they are durably committed.  Followers automatically forward client
//! requests to the current leader, so callers can connect to any node.
//!
//! Persistent state (Raft hard state, the log, and applied key-value data) is
//! stored in a [sled](https://docs.rs/sled) embedded database.  A node that
//! restarts will recover its identity and log from disk and re-join the
//! cluster without any manual intervention.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::net::Ipv4Addr;
//! use skiff_rs::{Builder, Client};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Build and start a single-node cluster.
//!     let node = Builder::new()
//!         .set_dir("/tmp/my-skiff-node")
//!         .bind("127.0.0.1".parse()?)
//!         .build()?;
//!
//!     let node_ref = node.clone();
//!     tokio::spawn(async move { node_ref.start().await });
//!
//!     // Block until a leader is elected before connecting a client.
//!     node.wait_for_leader(std::time::Duration::from_secs(2)).await?;
//!
//!     // Connect a client and perform some operations.
//!     let mut client = Client::new(vec!["127.0.0.1".parse()?]);
//!     client.connect().await?;
//!
//!     client.insert("greeting", "hello world").await?;
//!     let value: Option<String> = client.get("greeting").await?;
//!     println!("{:?}", value);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Multi-node cluster
//!
//! Pass the addresses of existing cluster members to [`Builder::join_cluster`]
//! when constructing additional nodes.  The new node will contact one of its
//! peers and register itself via the Raft `add_server` RPC.
//!
//! ```no_run
//! use skiff_rs::Builder;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Node 1 – sole member of a brand-new cluster.
//! let node1 = Builder::new()
//!     .set_dir("/tmp/node1")
//!     .bind("127.0.0.1".parse()?)
//!     .build()?;
//!
//! // Node 2 – joins the cluster seeded by node 1.
//! let node2 = Builder::new()
//!     .set_dir("/tmp/node2")
//!     .bind("127.0.0.2".parse()?)
//!     .join_cluster(vec!["127.0.0.1".parse()?])
//!     .build()?;
//! # Ok(())
//! # }
//! ```

mod builder;
mod client;
mod error;
mod skiff;
mod subscriber;

pub use builder::Builder;
pub use client::Client;
pub use error::Error;
pub use skiff::{ElectionState, Skiff};
pub use subscriber::Subscriber;
