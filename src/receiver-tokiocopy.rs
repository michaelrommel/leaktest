use cap::Cap;
use chrono::Local;
use opendal::Operator;
use opendal::services::S3;
use std::task::{Context, Poll};
use std::thread::sleep;
use std::time::Duration;
use std::{alloc, env, pin::Pin};
use tokio::io::{self, AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

struct ProgressReader<R> {
    inner: R,
    total: usize,
    last: usize,
    interval: usize,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let after = buf.filled().len();
            let chunklen = after - before;
            self.total += chunklen;
            if self.total - self.last >= self.interval {
                println!(
                    "{},{},{}",
                    Local::now().to_rfc3339(),
                    self.total,
                    ALLOCATOR.allocated()
                );
                self.last = self.total;
            }
        }
        // If we wait in the receiver a little bit each time after we got a
        // packet, it gives the OS the ability to fill up the receive buffer, and
        // the next time, we get larger packets, hence the problem is not as pronounced
        //sleep(Duration::from_millis(4));
        result
    }
}

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
        let mut writer = op
            .writer("testfile.txt")
            .await?
            .into_futures_async_write()
            .compat_write();

        let (socket, _) = listener.accept().await.unwrap();
        println!("Client connected");

        let rs = ReaderStream::new(socket);
        let async_reader = StreamReader::new(rs);
        tokio::pin!(async_reader);

        println!("Time,Received,Memory");
        let mut reader = ProgressReader {
            inner: async_reader,
            total: 0,
            last: 0,
            interval: 400 * 1024,
        };

        let bytes_written = tokio::io::copy(&mut reader, &mut writer).await.unwrap();

        println!("\nStream finished: {} bytes", bytes_written);
        let _ = writer.shutdown().await;

        Ok::<(), opendal::Error>(())
    });

    let _ = receiver_task.await;
    println!("End: {} bytes", ALLOCATOR.allocated());
    Ok(())
}
