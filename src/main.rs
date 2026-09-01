use std::collections::HashMap;
use std::sync::{Arc};

use tokio::sync::Mutex;

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

    let str_data = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    loop {
        let (mut stream, _) = listener.accept().await?;

        let value = str_data.clone();
        tokio::spawn(async move {
            let mut buf = [0; 512];
            
            loop {
                let bytes_read = match stream.read(&mut buf).await {
                    Ok(0) => {
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
                        info("PING");
                        if let Err(_) = stream.write_all(make_pong_message().as_bytes()).await {
                            break;
                        }
                    }
                    Ok(RespCommand::Echo(val)) => {
                        info(&format!("ECHO {}", val));
                        if let Err(_) = stream.write_all(make_echo_message(&val).as_bytes()).await {
                            break;
                        }
                    }
                    Ok(RespCommand::SetString(k, v)) => {
                        info(&format!("SET {} {}", k, v));
                        let mut db = value.lock().await;
                        db.insert(k, v);
                        if let Err(_) = stream.write_all(make_ok_message().as_bytes()).await {
                            break;
                        }
                    }
                    Ok(RespCommand::GetString(k)) => {
                        info(&format!("GET {}", k));
                        let db = value.lock().await;
                        let val = db.get(&k);
                        if val.is_none() {
                            if let Err(_) = stream.write_all(make_empty_message().as_bytes()).await {
                                break;
                            }
                            continue;
                        }

                        if let Err(_) = stream.write_all(make_bulk_string(val.unwrap()).as_bytes()).await {
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


fn make_pong_message() -> String {
    String::from("+PONG\r\n")
}

fn make_echo_message(val: &str) -> String {
    format!("${}\r\n{}\r\n", val.len(), val)
}

fn make_ok_message() -> String {
    String::from("+OK\r\n")
}

fn make_empty_message() -> String {
    String::from("$-1\r\n")
}

fn make_bulk_string(val: &str) -> String {
    format!("${}\r\n{}\r\n", val.len(), val)
}
