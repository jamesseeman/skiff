use std::fs;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serial_test::serial;
use skiff_rs::Client;
use skiff_rs::ElectionState;
use skiff_rs::Skiff;
use skiff_rs::Subscriber;

/// Build a fresh node at `address`, optionally joining a cluster via `peers`.
/// Always wipes the data directory first for a clean start.
fn build_node(address: &str, peers: Vec<&str>) -> Skiff {
    let dir = format!("target/tmp/test/{}", address);
    if Path::exists(Path::new(&dir)) {
        fs::remove_dir_all(&dir).unwrap();
    }
    build_node_persist(address, peers)
}

/// Build a node re-using an existing data directory (for restart tests).
fn build_node_persist(address: &str, peers: Vec<&str>) -> Skiff {
    let dir = format!("target/tmp/test/{}", address);
    let peer_addrs: Vec<std::net::Ipv4Addr> = peers.iter().map(|p| p.parse().unwrap()).collect();
    let mut builder = skiff_rs::Builder::new()
        .set_dir(&dir)
        .bind(address.parse().unwrap());
    if !peer_addrs.is_empty() {
        builder = builder.join_cluster(peer_addrs);
    }
    builder.build().unwrap()
}

fn get_leader() -> Result<Skiff, Box<dyn std::error::Error>> {
    let dir = String::from("target/tmp/test/127.0.0.1");
    if Path::exists(Path::new(&dir)) {
        fs::remove_dir_all(&dir)?;
    }

    Ok(skiff_rs::Builder::new()
        .set_dir(dir.as_str())
        .bind("127.0.0.1".parse()?)
        .build()?)
}

fn get_follower(address: &str) -> Result<Skiff, Box<dyn std::error::Error>> {
    let dir = format!("target/tmp/test/{}", &address);
    if Path::exists(Path::new(&dir)) {
        fs::remove_dir_all(&dir)?;
    }

    Ok(skiff_rs::Builder::new()
        .set_dir(dir.as_str())
        .join_cluster(vec!["127.0.0.1".parse()?])
        .bind(address.parse()?)
        .build()?)
}

/// Connect a client to 127.0.0.1. Call `wait_for_leader` on the node first
/// to guarantee the server is up and a leader exists before connecting.
async fn get_client() -> Result<Client, Box<dyn std::error::Error>> {
    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await?;
    Ok(client)
}

#[tokio::test]
#[serial]
async fn start_server() {
    let leader = get_leader().unwrap();
    let _handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });
}

#[tokio::test]
#[serial]
async fn leader_election() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    assert!(!(leader.is_leader_elected().await));
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(leader.is_leader_elected().await);
}

#[tokio::test]
#[serial]
async fn client_get() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    assert_eq!(None, client.get::<String>("nil").await.unwrap());
}

#[tokio::test]
#[serial]
async fn client_insert() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    client.insert::<String>("foo", "bar".into()).await.unwrap();
    assert_eq!(
        Some(String::from("bar")),
        client.get::<String>("foo").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn client_remove() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    client
        .insert::<String>("foo2", "bar2".into())
        .await
        .unwrap();
    assert_eq!(
        Some(String::from("bar2")),
        client.get::<String>("foo2").await.unwrap()
    );
    client.remove("foo2").await.unwrap();
    assert_eq!(None, client.get::<String>("foo2").await.unwrap());
}

#[tokio::test]
#[serial]
async fn drop_tree() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    client
        .insert::<String>("parent/foo", "bar".into())
        .await
        .unwrap();
    assert_eq!(vec!["parent"], client.get_prefixes().await.unwrap());
    client.remove("parent/foo").await.unwrap();
    assert_eq!(Vec::<String>::new(), client.get_prefixes().await.unwrap());
}

#[tokio::test]
#[serial]
async fn two_node_cluster() {
    let leader = get_leader().unwrap();
    let leader_ref = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();

    let follower = get_follower("127.0.0.2").unwrap();
    let follower_clone = follower.clone();
    let _ = tokio::spawn(async move {
        let _ = follower_clone.start().await;
    });

    // Wait for the follower to join and the cluster config to replicate.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let leader_cluster = leader.get_cluster().await.unwrap();
    let follower_cluster = follower.get_cluster().await.unwrap();

    assert_eq!(leader_cluster, follower_cluster);
    assert_eq!(2, leader_cluster.len());
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct MyStruct {
    enabled: bool,
    name: String,
    age: u8,
    height: f32,
}

#[tokio::test]
#[serial]
async fn custom_struct() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();

    let my_struct = MyStruct {
        enabled: true,
        name: "foo".into(),
        age: 100,
        height: 32.456789,
    };

    let mut client = get_client().await.unwrap();
    assert_eq!(None, client.get::<MyStruct>("mystruct").await.unwrap());

    // Insert and check
    client
        .insert::<MyStruct>("mystruct", my_struct.clone())
        .await
        .unwrap();
    assert_eq!(
        Some(my_struct),
        client.get::<MyStruct>("mystruct").await.unwrap()
    );

    // Delete
    client.remove("mystruct").await.unwrap();
    assert_eq!(None, client.get::<MyStruct>("mystruct").await.unwrap());
}

#[tokio::test]
#[serial]
async fn get_prefixes() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    assert_eq!(Vec::<String>::new(), client.get_prefixes().await.unwrap());
    client.insert::<String>("parent/foo", "bar".into()).await;
    assert_eq!(vec!["parent"], client.get_prefixes().await.unwrap());
    client
        .insert::<String>("grandparent/parent/foo", "bar".into())
        .await;
    assert_eq!(
        vec!["parent", "grandparent/parent"],
        client.get_prefixes().await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn list_prefixes() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client = get_client().await.unwrap();
    assert_eq!(Vec::<String>::new(), client.list_keys("").await.unwrap());

    client.insert::<String>("foo", "bar".into()).await;
    client.insert::<String>("parent/foo", "bar".into()).await;
    client
        .insert::<String>("parent/child/foo", "bar".into())
        .await;
    client
        .insert::<String>("grandparent/parent/foo", "bar".into())
        .await;
    assert_eq!(
        vec![
            "foo",
            "parent/foo",
            "parent/child/foo",
            "grandparent/parent/foo"
        ],
        client.list_keys("").await.unwrap()
    );
    assert_eq!(
        vec!["parent/foo", "parent/child/foo"],
        client.list_keys("parent/").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn watch_prefix() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let mut client1 = get_client().await.unwrap();
    let mut client2: Client = get_client().await.unwrap();

    tokio::spawn(async move {
        client1.insert::<String>("foo", "bar".into()).await;
        client1.insert::<String>("parent/foo", "bar".into()).await;
        client1
            .insert::<String>("parent/child/foo", "bar".into())
            .await;
        client1
            .insert::<String>("grandparent/parent/foo", "bar".into())
            .await;
    });

    let mut subscriber: Subscriber = client2.watch("parent/").await.unwrap();
    let mut recvd = Vec::new();
    for _ in 0..2 {
        let (key, _) = subscriber.recv::<String>().await.unwrap();
        recvd.push(key);
    }

    assert_eq!(
        vec!["parent/foo".to_string(), "parent/child/foo".to_string()],
        recvd
    );
}

// ── multi-node / failure tests ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn three_node_cluster() {
    let leader = build_node("127.0.0.1", vec![]);
    let follower1 = build_node("127.0.0.2", vec!["127.0.0.1"]);
    let follower2 = build_node("127.0.0.3", vec!["127.0.0.1"]);

    let leader_ref = leader.clone();
    let follower1_ref = follower1.clone();
    let follower2_ref = follower2.clone();

    tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = follower1_ref.start().await;
    });
    tokio::spawn(async move {
        let _ = follower2_ref.start().await;
    });
    // Wait for both followers to join and the cluster config to replicate.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(3, leader.get_cluster().await.unwrap().len());
    assert_eq!(3, follower1.get_cluster().await.unwrap().len());
    assert_eq!(3, follower2.get_cluster().await.unwrap().len());

    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await.unwrap();
    client
        .insert::<String>("replicated", "value".into())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // All three nodes carry the insert (reads are forwarded to leader)
    for addr in &["127.0.0.1", "127.0.0.2", "127.0.0.3"] {
        let mut c = skiff_rs::Client::new(vec![addr.parse().unwrap()]);
        c.connect().await.unwrap();
        assert_eq!(
            Some("value".to_string()),
            c.get::<String>("replicated").await.unwrap()
        );
    }
}

#[tokio::test]
#[serial]
async fn leader_failure_reelection() {
    let leader = build_node("127.0.0.1", vec![]);
    let follower1 = build_node("127.0.0.2", vec!["127.0.0.1"]);
    let follower2 = build_node("127.0.0.3", vec!["127.0.0.1"]);

    let leader_ref = leader.clone();
    let follower1_ref = follower1.clone();
    let follower2_ref = follower2.clone();

    let leader_handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });
    leader_ref
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = follower1_ref.start().await;
    });
    tokio::spawn(async move {
        let _ = follower2_ref.start().await;
    });
    // Wait for both followers to join.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(3, follower1.get_cluster().await.unwrap().len());

    // Shut down the leader: stop its election_manager first so it stops
    // sending heartbeats, then kill the gRPC server. Followers will time
    // out and re-elect.
    leader_ref.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    leader_handle.abort();

    // Poll until one of the remaining nodes wins an election (up to 2s).
    let mut new_leader_elected = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let f1 = follower1.get_election_state().await;
        let f2 = follower2.get_election_state().await;
        if matches!(f1, ElectionState::Leader) || matches!(f2, ElectionState::Leader) {
            new_leader_elected = true;
            break;
        }
    }
    assert!(
        new_leader_elected,
        "no new leader elected after old leader died"
    );

    // New cluster of 2 can still commit (2 > 3/2 = 1).
    let mut client = skiff_rs::Client::new(vec![
        "127.0.0.2".parse().unwrap(),
        "127.0.0.3".parse().unwrap(),
    ]);
    client.connect().await.unwrap();
    client
        .insert::<String>("post_failover", "ok".into())
        .await
        .unwrap();
    assert_eq!(
        Some("ok".to_string()),
        client.get::<String>("post_failover").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn follower_crash_and_rejoin() {
    let leader = build_node("127.0.0.1", vec![]);
    let follower1 = build_node("127.0.0.2", vec!["127.0.0.1"]);
    let follower2 = build_node("127.0.0.3", vec!["127.0.0.1"]);

    let leader_ref = leader.clone();
    let follower1_ref = follower1.clone();

    tokio::spawn(async move {
        let _ = leader_ref.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = follower1_ref.start().await;
    });
    let follower2_handle = {
        let f = follower2.clone();
        tokio::spawn(async move {
            let _ = f.start().await;
        })
    };
    // Wait for both followers to join.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Insert before crash.
    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await.unwrap();
    client
        .insert::<String>("before_crash", "yes".into())
        .await
        .unwrap();

    // Kill follower2: stop its election_manager, then kill the gRPC server, then release sled.
    let follower2_id = follower2.get_id();
    follower2.shutdown();
    follower2_handle.abort();
    drop(follower2);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Insert while follower2 is down (leader + follower1 = majority).
    client
        .insert::<String>("during_crash", "yes".into())
        .await
        .unwrap();

    // Restart follower2 from its existing data directory.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let follower2_restart = build_node_persist("127.0.0.3", vec!["127.0.0.1"]);
    let f2_id = follower2_restart.get_id();
    assert_eq!(
        follower2_id, f2_id,
        "node id must be stable across restarts"
    );

    let f2_ref = follower2_restart.clone();
    tokio::spawn(async move {
        let _ = f2_ref.start().await;
    });

    // Wait until follower2 is back in a known-cluster state.
    follower2_restart
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();

    // Both keys are readable (forwarded to leader).
    let mut c = skiff_rs::Client::new(vec!["127.0.0.3".parse().unwrap()]);
    c.connect().await.unwrap();
    assert_eq!(
        Some("yes".to_string()),
        c.get::<String>("before_crash").await.unwrap()
    );
    assert_eq!(
        Some("yes".to_string()),
        c.get::<String>("during_crash").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn concurrent_inserts() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let _handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();

    let mut tasks = vec![];
    for i in 0u32..10 {
        tasks.push(tokio::spawn(async move {
            let mut c = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
            c.insert::<u32>(&format!("concurrent_{}", i), i)
                .await
                .unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    for i in 0u32..10 {
        assert_eq!(
            Some(i),
            client
                .get::<u32>(&format!("concurrent_{}", i))
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
#[serial]
async fn remove_server_from_cluster() {
    let leader = get_leader().unwrap();
    let follower = get_follower("127.0.0.2").unwrap();

    let leader_clone = leader.clone();
    let follower_clone = follower.clone();
    let _lh = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let _fh = tokio::spawn(async move {
        let _ = follower_clone.start().await;
    });
    // Wait for the follower to join.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(2, leader.get_cluster().await.unwrap().len());

    let follower_id = follower.get_id();
    let follower_addr = follower.get_address();

    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await.unwrap();
    client
        .remove_node(follower_id, follower_addr)
        .await
        .unwrap();

    // Allow the Configure entry to commit and apply.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(1, leader.get_cluster().await.unwrap().len());
}

#[tokio::test]
#[serial]
async fn restart_persistence() {
    let dir = "target/tmp/test/persist";
    if Path::exists(Path::new(dir)) {
        fs::remove_dir_all(dir).unwrap();
    }

    // First run: insert data.
    let node = skiff_rs::Builder::new()
        .set_dir(dir)
        .bind("127.0.0.1".parse().unwrap())
        .build()
        .unwrap();
    let id_before = node.get_id();
    let node_clone = node.clone();
    let handle = tokio::spawn(async move {
        let _ = node_clone.start().await;
    });
    node.wait_for_leader(Duration::from_secs(2)).await.unwrap();

    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await.unwrap();
    client
        .insert::<String>("persistent_key", "persistent_value".into())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Simulate crash: stop background tasks, kill gRPC server, release sled lock.
    node.shutdown();
    handle.abort();
    drop(node);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second run: restart from same directory.
    let node2 = skiff_rs::Builder::new()
        .set_dir(dir)
        .bind("127.0.0.1".parse().unwrap())
        .build()
        .unwrap();

    assert_eq!(id_before, node2.get_id(), "node id must survive restart");

    let node2_ref = node2.clone();
    tokio::spawn(async move {
        let _ = node2_ref.start().await;
    });
    node2.wait_for_leader(Duration::from_secs(2)).await.unwrap();

    let mut client2 = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client2.connect().await.unwrap();
    assert_eq!(
        Some("persistent_value".to_string()),
        client2.get::<String>("persistent_key").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn subscriber_replication() {
    let leader = build_node("127.0.0.1", vec![]);
    let follower1 = build_node("127.0.0.2", vec!["127.0.0.1"]);
    let follower2 = build_node("127.0.0.3", vec!["127.0.0.1"]);

    let l = leader.clone();
    let f1 = follower1.clone();
    let f2 = follower2.clone();

    tokio::spawn(async move {
        let _ = l.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = f1.start().await;
    });
    tokio::spawn(async move {
        let _ = f2.start().await;
    });
    // Wait for both followers to join.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Subscribe from follower2.
    let mut sub_client = skiff_rs::Client::new(vec!["127.0.0.3".parse().unwrap()]);
    sub_client.connect().await.unwrap();
    let mut sub = sub_client.watch("repl/").await.unwrap();

    // Insert from leader.
    let mut write_client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    write_client.connect().await.unwrap();
    write_client
        .insert::<String>("repl/key1", "val1".into())
        .await
        .unwrap();

    let (key, val) = tokio::time::timeout(Duration::from_secs(5), sub.recv::<String>())
        .await
        .expect("subscriber recv timed out")
        .unwrap();

    assert_eq!("repl/key1", key);
    assert_eq!("val1", val);
}

/// Verifies that a client insert returns an error when the cluster cannot reach quorum.
/// A 2-node cluster requires both nodes; killing the follower makes commit impossible.
/// NOTE: this test takes ~5 seconds due to the server-side commit timeout.
#[tokio::test]
#[serial]
async fn insert_timeout_leader_loss() {
    let leader = get_leader().unwrap();
    let follower = get_follower("127.0.0.2").unwrap();

    let leader_clone = leader.clone();
    let follower_clone = follower.clone();
    let _lh = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });
    leader
        .wait_for_leader(Duration::from_secs(2))
        .await
        .unwrap();
    let follower_handle = tokio::spawn(async move {
        let _ = follower_clone.start().await;
    });
    // Wait for the follower to join.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(2, leader.get_cluster().await.unwrap().len());

    // Kill follower — leader now can't reach quorum (needs 2/2).
    // shutdown() closes the gRPC server (via serve_with_shutdown) and the election_manager.
    follower.shutdown();
    follower_handle.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = skiff_rs::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    client.connect().await.unwrap();
    let result = client.insert::<String>("no_quorum", "x".into()).await;
    assert!(result.is_err(), "insert should fail when quorum is lost");
}
