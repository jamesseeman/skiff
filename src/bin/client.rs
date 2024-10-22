use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct MyStruct {
    works: bool,
    name: String,
    age: u8,
    height: f32,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut client = skiff::Client::new(vec!["192.168.5.70".parse()?, "127.0.0.1".parse()?]);

    let t1 = MyStruct {
        works: true,
        name: "name".to_string(),
        age: 34,
        height: 1.89f32,
    };

    println!("My struct: {:?}", t1);

    println!("{:?}", client.get::<MyStruct>("t2", None).await?);
    println!("{:?}", client.insert("t2", t1, None).await?);
    println!("{:?}", client.get::<MyStruct>("t2", None).await?);
    println!("{:?}", client.remove("t2", None).await?);
    println!("{:?}", client.get::<MyStruct>("t2", None).await?);

    Ok(())
}