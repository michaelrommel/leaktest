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

        // the default setting is to read data in 4kB chunks
        let mut stream = ReaderStream::new(file);
        // by making the chunks larger by default, we send larger network packets
        // and the OS on the receiving side hands those larger chunks to the application
        // hence mitigating the issue. But we do not have control on the server side how
        // the client sends the stream.
        //let mut stream = ReaderStream::with_capacity(file, 32 * 1024);

        println!("Stream: {} bytes", ALLOCATOR.allocated());

        let mut dest_stream = TcpStream::connect(addr).await.expect("Failed to connect");
        println!("Connected");

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap();
            dest_stream
                .write_all(&bytes)
                .await
                .expect("Failed to write");
            // if we send small packets and sleep a bit between the packets, the
            // receiver on an idle system will pick them up very quickly, getting
            // a lot of those small packets into the queue.
            sleep(Duration::from_millis(5)).await;
        }

        // Shutting down the write half signals EOF to the receiver
        dest_stream.shutdown().await.expect("Failed to shutdown");
    });

    let _ = sender_task.await;
    println!("End: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
