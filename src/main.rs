use std::collections::HashMap;
use std::sync::Arc;

use codecrafters_redis::parser::{EntryType, Op, parse};
use tokio::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use codecrafters_redis::entry::Entry;
use codecrafters_redis::log::{info, error};

type Data = HashMap<String, Entry>;
type DataArr = HashMap<String, Vec<String>>;

const HOST: &'static str = "127.0.0.1";
const PORT: &'static str = "6379";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", HOST, PORT);
    let listener = TcpListener::bind(addr).await?;

    info(&format!("listening on port {}", PORT));

    let str_data = Arc::new(RwLock::new(Data::new()));
    let arr_data = Arc::new(RwLock::new(DataArr::new()));

    loop {
        let (mut stream, _) = listener.accept().await?;

        let value = str_data.clone();
        let arr_value = arr_data.clone();
        
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

                match parse(&buf[..bytes_read]) {
                    Ok(Op::Ping) => {
                        info("PING");
                        if let Err(_) = stream.write_all(make_pong_message().as_bytes()).await {
                            break;
                        }
                    }
                    Ok(Op::Echo(val)) => {
                        info(&format!("ECHO {}", val));
                        if let Err(_) = stream.write_all(make_echo_message(&val).as_bytes()).await {
                            break;
                        }
                    }
                    Ok(Op::Set(k, v, ttl_ms)) => {
                        info(&format!("SET {} {:?}", k, v));
                        let entry = match ttl_ms {
                            None => Entry::new(v),
                            Some(ttl) => Entry::with_ttl(v, ttl)
                        };

                        {
                            let mut db = value.write().await;
                            db.insert(k, entry);                            
                        }

                        if let Err(_) = stream.write_all(make_ok_message().as_bytes()).await {
                            break;
                        }
                    }
                    Ok(Op::Get(k)) => {
                        info(&format!("GET {}", k));
                        let val: Option<Entry> = {
                            let guard = value.read().await;
                            match guard.get(&k) {
                                None => None,
                                Some(e) => Some(Entry {
                                    val: e.val.clone(),
                                    expires_at: e.expires_at,
                                })
                            }
                            
                        };

                        match val {
                            None => {
                                if let Err(_) = stream.write_all(make_empty_message().as_bytes()).await {
                                    break;
                                }
                                continue;
                            },
                            Some(v) => {
                                if v.is_expired() {
                                    {
                                        info("value present, but expired - removing");
                                        let mut guard = value.write().await;
                                        guard.remove(&k);
                                    }
                                    if let Err(_) = stream.write_all(make_empty_message().as_bytes()).await {
                                        break;
                                    }
                                } else {
                                    let src = match v.val {
                                        EntryType::Str(val) => make_bulk_string(&val),
                                        _ => break
                                    };
                                    if let Err(_) = stream
                                        .write_all(src.as_bytes())
                                        .await
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Op::RPush(k, v)) => {
                        info(&format!("RPUSH {} {:?}", k, v));
                        {
                            
                            let mut db = arr_value.write().await;
                            db.insert(k, vec!(v));
                        }
                        if let Err(_) = stream
                                        .write_all(make_int_message(1).as_bytes())
                                        .await
                                    {
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

fn make_int_message(val: i64) -> String {
    format!(":{}\r\n", val)
}
