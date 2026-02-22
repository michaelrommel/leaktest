use bytes::Bytes;
use cap::Cap;
use futures::StreamExt;
use opendal::Operator;
use opendal::services::S3;
use std::{alloc, env};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};
use tokio_util::io::ReaderStream;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ALLOCATOR.set_limit(200 * 1024 * 1024).unwrap();
    println!("Start: {} bytes", ALLOCATOR.allocated());
    let builder = S3::default()
        .endpoint("https://s3.amazonaws.com")
        .region("eu-west-2")
        .bucket(&env::var("S3BUCKET")?)
        .root("/");
    println!("builder: {} bytes", ALLOCATOR.allocated());
    let op: Operator = Operator::new(builder)?.finish();
    println!("operator: {} bytes", ALLOCATOR.allocated());

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    let receiver_task = tokio::spawn(async move {
        let mut n = 0;
        let mut total = 0;
        let mut writer = op.writer("testfile.txt").await?;
        let (mut socket, _) = listener.accept().await.unwrap();
        println!("[Receiver] Client connected");

        loop {
            let mut chunk = vec![0u8; 128 * 1024];
            let count = socket.read(&mut chunk).await.expect("Failed to read");
            if count == 0 {
                break;
            }
            n += 1;
            total += count;
            if count > 4096 {
                println!("received: {} bytes", count);
            }
            if n % 100 == 0 {
                println!(
                    "{}: recv: {} allocated: {} bytes",
                    n,
                    total,
                    ALLOCATOR.allocated()
                );
            }
            chunk.truncate(count);
            let data = Bytes::from(chunk);
            writer.write(data).await?;
        }
        println!("\n[Receiver] Stream finished.");
        writer.close().await?;
        Ok::<(), opendal::Error>(())
    });

    let sender_task = tokio::spawn(async move {
        let file = File::open("mediumfile.dat").await.unwrap();
        // let mut stream = ReaderStream::with_capacity(file, 1024);
        let mut stream = ReaderStream::new(file);

        println!("stream: {} bytes", ALLOCATOR.allocated());

        // Wait a tiny bit for the listener to be ready
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let mut dest_stream = TcpStream::connect(addr).await.expect("Failed to connect");
        println!("[Sender] Connected");

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap();
            // println!("sending: {} bytes", bytes.len());
            dest_stream
                .write_all(&bytes)
                .await
                .expect("Failed to write");
            sleep(Duration::from_millis(100)).await;
        }

        // Shutting down the write half signals EOF to the receiver
        dest_stream.shutdown().await.expect("Failed to shutdown");
    });

    let _ = tokio::join!(receiver_task, sender_task);
    println!("Currently allocated: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
