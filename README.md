# skiff

[![Crates.io](https://img.shields.io/crates/v/skiff-rs.svg)](https://crates.io/crates/skiff-rs)
[![docs.rs](https://docs.rs/skiff-rs/badge.svg)](https://docs.rs/skiff-rs)
[![CI](https://github.com/jamesseeman/skiff/actions/workflows/ci.yml/badge.svg)](https://github.com/jamesseeman/skiff/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An embedded [Raft](https://raft.github.io/) consensus library backed by [sled](https://github.com/spacejam/sled). Skiff lets you add replicated, persistent key-value storage directly to your Rust application without running a separate consensus service.

## Features

- Leader election and re-election
- Log replication with majority-commit semantics
- Hard state persistence (survives restarts)
- Dynamic cluster membership (`add_server` / `remove_server`)
- Follower request forwarding (connect to any node)
- Change subscriptions (`watch` prefix for streaming updates)

**Not yet implemented:** log compaction / snapshotting.

## Installation

```toml
[dependencies]
skiff-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

> **Note:** skiff-rs requires `protoc` to be installed at build time.
> On Debian/Ubuntu: `sudo apt install protobuf-compiler`
> On macOS: `brew install protobuf`

## Quick start

```rust
use skiff_rs::{Builder, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build and start a single-node cluster.
    let node = Builder::new()
        .set_dir("/tmp/my-skiff-node")
        .bind("127.0.0.1".parse()?)
        .build()?;

    let node_ref = node.clone();
    tokio::spawn(async move { node_ref.start().await });

    // Block until a leader is elected before connecting a client.
    node.wait_for_leader(std::time::Duration::from_secs(2)).await?;

    // Connect a client and perform some operations.
    let mut client = Client::new(vec!["127.0.0.1".parse()?]);
    client.connect().await?;

    client.insert("greeting", "hello world").await?;
    let value: Option<String> = client.get("greeting").await?;
    println!("{:?}", value); // Some("hello world")

    Ok(())
}
```

## Multi-node cluster

Pass existing node addresses to `join_cluster` when constructing additional nodes. The new node contacts a peer and registers itself via the Raft `add_server` RPC.

```rust
use skiff_rs::Builder;
use std::time::Duration;

// Node 1 — bootstraps a new single-node cluster.
let node1 = Builder::new()
    .set_dir("/tmp/node1")
    .bind("127.0.0.1".parse()?)
    .build()?;

// Node 2 — joins the cluster through node 1.
let node2 = Builder::new()
    .set_dir("/tmp/node2")
    .bind("127.0.0.2".parse()?)
    .join_cluster(vec!["127.0.0.1".parse()?])
    .build()?;

let node1_ref = node1.clone();
tokio::spawn(async move { node1_ref.start().await });

// Wait for node1 to elect itself leader before node2 tries to join.
node1.wait_for_leader(Duration::from_secs(2)).await?;

tokio::spawn(async move { node2.start().await });
```

## Hierarchical keys

Keys use `/` as a path separator, providing a simple namespace hierarchy:

```rust
client.insert("users/alice", alice_data).await?;
client.insert("users/bob", bob_data).await?;

// List all keys under "users/"
let keys = client.list_keys("users/").await?;

// Get all top-level prefixes
let prefixes = client.get_prefixes().await?; // ["users"]
```

## Change subscriptions

```rust
let mut sub = client.watch("users/").await?;
loop {
    let (key, value): (String, MyType) = sub.recv().await?;
    println!("updated: {} = {:?}", key, value);
}
```

## Shutdown

Call `shutdown()` before dropping a node to allow background tasks and the sled database lock to be released cleanly:

```rust
node.shutdown();
drop(node);
```

## Architecture

Each `Skiff` node runs two background tasks:

- **Cluster-join task** — on startup, contacts a peer and calls `add_server` to register the node if the cluster has more than one known member.
- **Election manager** — drives the Raft state machine: sends heartbeats as leader (every 75 ms) or fires a randomised election timeout as follower (150–300 ms).

All inter-node communication uses gRPC (via [tonic](https://github.com/hyperium/tonic)). Persistent state is stored in [sled](https://github.com/spacejam/sled) named trees:

| Tree | Contents |
|------|----------|
| `__raft_meta` | current term, voted_for, last_applied, node ID |
| `__raft_log` | Raft log entries (keyed by big-endian u32 index) |
| `base` | root-level key-value pairs |
| `base_<prefix>` | key-value pairs under a namespace prefix |

## Status

v0.1 — suitable for experimentation and projects that can tolerate the missing features noted above. The on-disk format is not yet considered stable.
