use serial_test::serial;
use skiff::Client;
use skiff::Skiff;

fn get_skiff(address: &str) -> Result<Skiff, anyhow::Error> {
    Ok(skiff::Builder::new()
        .set_dir("/tmp/test/")
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
    let leader = get_skiff("127.0.0.1").unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });

    println!("ok");
}

#[tokio::test]
#[serial]
async fn client_get() {
    let leader = get_skiff("127.0.0.1").unwrap();
    let handle = tokio::spawn(async move {
        let _ = leader.start().await;
    });

    let mut client = get_client().await.unwrap();
    assert_eq!(None, client.get::<String>("nil").await.unwrap());
}

#[tokio::test]
#[serial]
async fn client_insert() {
    let leader = get_skiff("127.0.0.1").unwrap();
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
    let leader = get_skiff("127.0.0.1").unwrap();
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
