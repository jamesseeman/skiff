use serial_test::serial;
use skiff::Client;
use skiff::Skiff;

fn get_leader() -> Result<Skiff, anyhow::Error> {
    Ok(skiff::Builder::new()
        .set_dir("/tmp/test/127.0.0.1")
        .bind("127.0.0.1".parse()?)
        .build()?)
}

fn get_follower(address: &str) -> Result<Skiff, anyhow::Error> {
    Ok(skiff::Builder::new()
        .set_dir(format!("/tmp/test/{}", &address).as_str())
        .join_cluster(vec!["127.0.0.1".parse()?])
        .bind(address.parse()?)
        .build()?)
}

// Todo: there's still some race conditions here, for some reason if from_millis is too low
// tests will fail even though client successfully connects. Need to identify when server is
// actually ready
async fn get_client() -> Result<Client, anyhow::Error> {
    let mut client = skiff::Client::new(vec!["127.0.0.1".parse().unwrap()]);
    for _ in 0..5 {
        if let Ok(_) = client.connect().await {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }

    Ok(client)
}

#[tokio::test]
#[serial]
async fn start_server() {
    let leader = get_leader().unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });
}

#[tokio::test]
#[serial]
async fn leader_election() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    assert!(!(leader.is_leader_elected().await));
    // Give leader time to elect itself
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert!(leader.is_leader_elected().await);
}

#[tokio::test]
#[serial]
async fn client_get() {
    let leader = get_leader().unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });

    let mut client = get_client().await.unwrap();
    assert_eq!(None, client.get::<String>("nil").await.unwrap());
}

#[tokio::test]
#[serial]
async fn client_insert() {
    let leader = get_leader().unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });

    let mut client = get_client().await.unwrap();
    client.insert::<String>("foo", "bar".into()).await;
    assert_eq!(
        Some(String::from("bar")),
        client.get::<String>("foo").await.unwrap()
    );
}

#[tokio::test]
#[serial]
async fn client_remove() {
    let leader = get_leader().unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });

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
async fn two_node_cluster() {
    let leader = get_leader().unwrap();
    let leader_clone = leader.clone();
    let handle = tokio::spawn(async move {
        let _ = leader_clone.start().await;
    });

    // Give leader time to elect itself
    // Todo: again, need more reliable method for determining when servers are ready
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let follower = get_follower("127.0.0.2").unwrap();
    let follower_clone = follower.clone();
    let _ = tokio::spawn(async move {
        let _ = follower_clone.start().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let leader_cluster = leader.get_cluster().await.unwrap();
    let follower_cluster = follower.get_cluster().await.unwrap();

    assert_eq!(leader_cluster, follower_cluster);
    assert_eq!(2, leader_cluster.len());
}
