use std::collections::HashMap;
use std::sync::Arc;

use codecrafters_redis::parser::{Op, parse};
use tokio::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use codecrafters_redis::entry::Entry;
use codecrafters_redis::handlers::{
    ArrOpType, handle_blpop, handle_echo, handle_err, handle_get, handle_llen, handle_lpop, handle_lrange, handle_ping, handle_push, handle_set, handle_type, handle_xadd
};
use codecrafters_redis::log::{error, info};

type Data = HashMap<String, Entry>;

const HOST: &str = "127.0.0.1";
const PORT: &str = "6379";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", HOST, PORT);
    let listener = TcpListener::bind(addr).await?;

    info(&format!("listening on port {}", PORT));

    let data = Arc::new(RwLock::new(Data::new()));

    loop {
        let (mut stream, _) = listener.accept().await?;

        let data_cl = data.clone();

        tokio::spawn(async move {
            let mut buf = [0; 512]; // TODO: tokyo mut buf

            loop {
                let bytes_read = match stream.read(&mut buf).await {
                    Ok(0) => {
                        break;
                    }
                    Ok(n) => n,
                    Err(err) => {
                        error(&format!("{}", err));
                        break;
                    }
                };

                let response = match parse(&buf[..bytes_read]) {
                    Ok(Op::Ping) => {
                        info("PING");
                        handle_ping()
                    }
                    Ok(Op::Echo(val)) => {
                        info(&format!("ECHO {}", val));
                        handle_echo(&val)
                    }
                    Ok(Op::Set(k, v, ttl_ms)) => {
                        info(&format!("SET {} {:?}", k, v));
                        handle_set(k, v, ttl_ms, data_cl.clone()).await
                    }
                    Ok(Op::Get(k)) => {
                        info(&format!("GET {}", k));
                        handle_get(k, data_cl.clone()).await
                    }
                    Ok(Op::RPush(k, vs)) => {
                        info(&format!("RPUSH {}", k));
                        handle_push(k, vs, data_cl.clone(), ArrOpType::Right).await
                    }
                    Ok(Op::LPush(k, vs)) => {
                        info(&format!("LPUSH {}", k));
                        handle_push(k, vs, data_cl.clone(), ArrOpType::Left).await
                    }
                    Ok(Op::LRange(key, start, stop)) => {
                        info(&format!("LRANGE {} {} {}", key, start, stop));
                        handle_lrange(key, start, stop, data_cl.clone()).await
                    }
                    Ok(Op::Llen(k)) => {
                        info(&format!("LLEN {}", k));
                        handle_llen(k, data_cl.clone()).await
                    }
                    Ok(Op::Lpop(k, n)) => {
                        info(&format!("LPOP {}", k));
                        handle_lpop(k, n, data_cl.clone()).await
                    }
                    Ok(Op::Blpop(k, timeout)) => {
                        info(&format!("BLPOP {} {}", k, timeout));
                        handle_blpop(k, timeout, data_cl.clone()).await
                    }
                    Ok(Op::Type(k)) => {
                        info(&format!("TYPE {}", k));
                        handle_type(k, data_cl.clone()).await
                    }
                    Ok(Op::XAdd(k, se)) => {
                        info(&format!("XADD {} {:?}", k, se));
                        handle_xadd(k, se, data_cl.clone()).await
                    }
                    Err(err) => {
                        error(err);
                        handle_err().await
                    }
                };

                if let Err(_) = stream.write_all(response.as_bytes()).await {
                    break;
                }
            }
        });
    }
}
