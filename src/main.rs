use tokio::net::{TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use chrono::prelude::*;

use codecrafters_redis::resp::{parse_resp, RespCommand};

fn info(msg: &str) {
    println!("[INFO] [{}] {}", Utc::now().to_string(), msg);
}

fn error(msg: &str) {
    eprintln!("[ERROR] [{}] {}", Utc::now().to_string(), msg);
}

const HOST: &'static str = "127.0.0.1";
const PORT: &'static str = "6379";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", HOST, PORT);
    let listener = TcpListener::bind(addr).await?;

    info(&format!("listening on port {}", PORT));

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            info("new connection");
            let mut buf = [0; 512];
            
            loop {
                let bytes_read = match stream.read(&mut buf).await {
                    Ok(0) => {
                        info("ending connection");
                        break;
                    }
                    Ok(n) => n,
                    Err(err) => {
                        error(&format!("{}", err.to_string()));
                        continue;
                    }
                };

                match parse_resp(&buf[..bytes_read]) {
                    Ok(RespCommand::Ping) => {
                        info("received PING");
                        if let Err(_) = stream.write_all(b"+PONG\r\n").await {
                            break;
                        }
                    }
                    Ok(RespCommand::Echo(val)) => {
                        info(&format!("received ECHO {}", val));
                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                        if let Err(_) = stream.write_all(response.as_bytes()).await {
                            break;
                        }
                    }
                    Err(err) => {
                        error(err);
                        continue;
                    }
                }               
            }
        });
    }
}
