use std::time::Duration;

use rzmq::{Context, Msg, SocketType, ZmqError};

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?; // Or Context::with_io_uring_options(...)

    // REP socket (standard Tokio TCP for simplicity, or could also be io_uring)
    let rep = ctx.socket(SocketType::Rep)?;
    let rep_endpoint = "tcp://127.0.0.1:5555";
    rep.bind(rep_endpoint).await?;

    // Spawn a task for REP to handle requests
    tokio::spawn(async move {
        loop {
            let req_parts = rep.recv_multipart().await.unwrap();
            // REP echoes the payload
            let mut reply_parts = Vec::new();
            reply_parts.push(Msg::from_static(b"")); // Delimiter
            reply_parts.push(req_parts[0].clone()); // Echo payload
            rep.send_multipart(reply_parts).await.unwrap();
        }
    });

    // REQ socket, configured to use io_uring for its session
    let req = ctx.socket(SocketType::Req)?;
    // Option to enable io_uring for this socket's sessions
    req.set_option(rzmq::socket::options::IO_URING_SESSION_ENABLED, true).await?;
    // Potentially other io_uring options like buffer sizes, multishot, zerocopy

    req.connect(rep_endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connection

    for i in 0..3 {
        let request_payload = format!("Request-{}", i);
        println!("REQ: Sending '{}'", request_payload);
        req.send(Msg::from_vec(request_payload.into_bytes())).await?;

        println!("REQ: Receiving reply...");
        let reply = req.recv().await?;
        println!("REQ: Received '{}'", String::from_utf8_lossy(reply.data().unwrap()));
    }

    req.close().await?;
    // rep will be closed when ctx is termed
    ctx.term().await?;
    Ok(())
}