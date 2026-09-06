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

                let response = match parse(&buf[..bytes_read]) {
                    Ok(Op::Ping) => {
                        info("PING");
                        make_pong_message()
                    }
                    Ok(Op::Echo(val)) => {
                        info(&format!("ECHO {}", val));
                        make_echo_message(&val)
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
                        make_ok_message()
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
                                make_empty_message()
                            },
                            Some(v) => {
                                if v.is_expired() {
                                    {
                                        info("value present, but expired - removing");
                                        let mut guard = value.write().await;
                                        guard.remove(&k);
                                    }
                                    make_empty_message()
                                } else {
                                    let src = match v.val {
                                        EntryType::Str(val) => make_bulk_string(&val),
                                        _ => break
                                    };
                                    src
                                }
                            }
                        }
                    }
                    Ok(Op::RPush(k, vs)) => {
                        let l = {
                            
                            let mut db = arr_value.write().await;

                            let arr = db.entry(k).or_default();

                            for v in vs.iter() {
                                 arr.push(v.to_string());
                            }

                            arr.len()

                        };
                        make_int_message(l as i64)
                        // if let Err(_) = stream
                        //                 .write_all(make_int_message(l as i64).as_bytes())
                        //                 .await
                        //             {
                        //                 break;
                        //             }
                    }
                    Ok(Op::LRange(key, start, stop)) => {
                        let default: Vec<String> = vec!();
                        let msg = {
                            if start > stop { make_range_message(&[]) } else {
                                let db = arr_value.read().await;
                                
                                let entry = db.get(&key).unwrap_or(&default);

                                let rg = {
                                    if entry.len() <= start { &[] } else {
                                    if stop >= entry.len() {
                                        &entry[start..]
                                    } else {
                                            &entry[start..stop + 1]
                                    }
                                    }
                                };
                                make_range_message(rg)
                            }
                        };
                        msg
                        
                    }
                    Err(err) => {
                        error(err);
                        continue;
                    }
                };
                
                if let Err(_) = stream.write_all(response.as_bytes()).await {
                    break;
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

fn make_range_message(rg: &[String]) -> String {
    let l = rg.len();

    let mut msg = format!("*{}\r\n", l);

    for elem in rg {
        msg = msg + &format!("${}\r\n{}\r\n", elem.len(), elem);
    }

    msg
}
