use cap::Cap;
use chrono::Local;
use futures::StreamExt;
use opendal::Operator;
use opendal::services::S3;
use std::{alloc, env};
use tokio::fs::File;
use tokio::time::{Duration, sleep};
use tokio_util::io::ReaderStream;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Start: {} bytes", ALLOCATOR.allocated());
    let builder = S3::default()
        .endpoint("https://s3.amazonaws.com")
        .region("eu-west-2")
        .bucket(&env::var("S3BUCKET")?)
        .root("/");
    println!("builder: {} bytes", ALLOCATOR.allocated());
    let op: Operator = Operator::new(builder)?.finish();
    println!("operator: {} bytes", ALLOCATOR.allocated());
    let file = File::open("mediumfile.dat").await.unwrap();
    let mut stream = ReaderStream::with_capacity(file, 1024);
    // let mut stream = ReaderStream::new(file);
    println!("stream: {} bytes", ALLOCATOR.allocated());
    let mut writer = op.writer("testfile.txt").await?;
    println!("writer: {} bytes", ALLOCATOR.allocated());

    let mut n = 0;
    let mut total = 0;

    println!("Time,Received,Memory");
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.unwrap();
        let count = bytes.len();
        n += 1;
        total += count;
        if count > 4096 {
            println!("received: {} bytes", count);
        }
        if n % 400 == 0 {
            println!(
                "{},{},{}",
                Local::now().to_rfc3339(),
                total,
                ALLOCATOR.allocated()
            );
        }
        sleep(Duration::from_millis(10)).await;
        writer.write(bytes).await?;
    }
    println!("\nStream finished.");
    writer.close().await?;

    println!("Currently allocated: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
