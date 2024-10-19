use anyhow::Result;
use skiff::Skiff;
use std::net::Ipv4Addr;

#[tokio::main]
pub async fn main() -> Result<()> {

    //let mut skiff = Skiff::new("192.168.5.70".parse()?);
    let mut skiff = Skiff::from_cluster("192.168.5.70".parse()?, vec![
        "192.168.5.70".parse()?,
        "127.0.0.1".parse()?,
    ], "/tmp/skiff/1")?;

    println!("{:?}", skiff);

    skiff.start().await?;

    //skiff.join_cluster(Ipv4Addr::from("127.0.0.1".into()))?;

    /*println!("{:?}", skiff.get_cluster());
    println!("{:?}", skiff.get_leader());

    let value = skiff.get("tree", "key")?;
    skiff.set("tree", "key2", value + 1)?;
    skiff.remove("tree", "key2")?;

    let value = skiff.get("tree", "key2")?;
    */

    Ok(())
}
