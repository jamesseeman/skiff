use anyhow::Result;
use skiff::Skiff;
use std::net::Ipv4Addr;

#[tokio::main]
pub async fn main() -> Result<()> {
    let follower = skiff::Builder::new()
        .set_dir("/tmp/skiff/2")
        .bind("127.0.0.1".parse()?)
        .join_cluster(vec!["192.168.5.70".parse()?])
        .build()?;

    follower.start().await?;

    //let mut skiff = Skiff::new("192.168.5.70".parse()?, Some("/tmp/skiff/2"))?;
    // let mut skiff = Skiff::from_cluster("127.0.0.1".parse()?, vec![
    //     "192.168.5.70".parse()?,
    //     "127.0.0.1".parse()?,
    // ], "/tmp/skiff/2")?;

    //println!("{:?}", skiff);

    //skiff.start().await?;

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
