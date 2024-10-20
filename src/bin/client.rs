use anyhow::Result;
use skiff::Skiff;
use std::net::Ipv4Addr;

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut client = skiff::Client::new(vec![
        "192.168.5.70".parse()?,
        "127.0.0.1".parse()?,
    ]);

    println!("{:?}", client.get::<bool>("test", None).await?);
    println!("{:?}", client.insert("test", true, None).await?);

    Ok(())
}