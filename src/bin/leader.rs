use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    let leader = skiff::Builder::new()
        .set_dir("/tmp/skiff/1")
        .bind("192.168.5.70".parse()?)
        .build()?;

    leader.start().await?;

    // let follower = skiff::Builder::new()
    //     .set_dir("/tmp/skiff/2")
    //     .bind("127.0.0.1".parse()?)
    //     .join_cluster("192.168.5.70".parse()?)
    //     .build();
// 
    // follower.start().await?;

    //let old_follower = skiff::Builder::from_config(0, Some("/tmp/skiff"));
    //old_follower.start().await?;

    Ok(())
}
