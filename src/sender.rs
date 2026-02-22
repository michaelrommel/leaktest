use cap::Cap;
use futures::StreamExt;
use std::alloc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};
use tokio_util::io::ReaderStream;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Start: {} bytes", ALLOCATOR.allocated());
    let addr = "127.0.0.1:8080";
    let sender_task = tokio::spawn(async move {
        let file = File::open("mediumfile.dat").await.unwrap();
        let mut stream = ReaderStream::with_capacity(file, 1024 * 1024);

        println!("Stream: {} bytes", ALLOCATOR.allocated());

        let mut dest_stream = TcpStream::connect(addr).await.expect("Failed to connect");
        println!("Connected");

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap();
            dest_stream
                .write_all(&bytes)
                .await
                .expect("Failed to write");
            sleep(Duration::from_millis(10)).await;
        }

        // Shutting down the write half signals EOF to the receiver
        dest_stream.shutdown().await.expect("Failed to shutdown");
    });

    let _ = sender_task.await;
    println!("End: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
