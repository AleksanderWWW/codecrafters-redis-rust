use tokio::net::{TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use chrono::prelude::*;

fn info(msg: &str) {
    println!("[INFO] [{}] {}", Utc::now().to_string(), msg);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    info("Listening on port 6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        
        // Ownership of `stream` is moved into the async task—no Arc required
        tokio::spawn(async move {
            info("new connection");
            let mut buf = [0; 512];
            
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) | Err(_) => {
                        info("ending connection");
                        break;
                    }
                    Ok(n) => n,
                };

                if let Err(_) = stream.write_all(b"+PONG\r\n").await {
                    break;
                }
            }
        });
    }
}
