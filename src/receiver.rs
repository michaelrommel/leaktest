use bytes::Bytes;
use cap::Cap;
use chrono::Local;
use opendal::Operator;
use opendal::services::S3;
use std::{alloc, env};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Start: {} bytes", ALLOCATOR.allocated());
    let builder = S3::default()
        .endpoint("http://192.168.13.187:9000")
        .region("eu-west-2")
        .bucket(&env::var("S3BUCKET")?)
        .access_key_id(&env::var("AWS_ACCESS_KEY")?)
        .secret_access_key(&env::var("AWS_SECRET_KEY")?)
        .root("/");
    println!("Builder: {} bytes", ALLOCATOR.allocated());
    let op: Operator = Operator::new(builder)?.finish();
    println!("Operator: {} bytes", ALLOCATOR.allocated());

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    let receiver_task = tokio::spawn(async move {
        let mut n = 0;
        let mut total = 0;
        let mut writer = op.writer("testfile.txt").await?;
        let (mut socket, _) = listener.accept().await.unwrap();
        println!("Client connected");
        println!("Time,Received,Memory");

        loop {
            let mut chunk = vec![0u8; 128 * 1024];
            let count = socket.read(&mut chunk).await.expect("Failed to read");
            if count == 0 {
                break;
            }
            n += 1;
            total += count;
            if n % 400 == 0 {
                println!(
                    "{},{},{}",
                    Local::now().to_rfc3339(),
                    total,
                    ALLOCATOR.allocated()
                );
            }
            // chunk.truncate(count);
            // let data = Bytes::from(chunk);
            let data = chunk[..count].to_vec();
            writer.write(data).await?;
        }
        println!("\nStream finished.");
        writer.close().await?;
        Ok::<(), opendal::Error>(())
    });

    let _ = receiver_task.await;
    println!("End: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
