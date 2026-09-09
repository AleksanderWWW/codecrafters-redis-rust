use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use codecrafters_redis::parser::{Op, parse};
use tokio::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use codecrafters_redis::entry::Entry;
use codecrafters_redis::log::{info, error};
use codecrafters_redis::handlers::{
    ArrOpType, handle_echo, handle_err, handle_get, handle_lrange, handle_ping, handle_push, handle_set
};

type Data = HashMap<String, Entry>;
type DataArr = HashMap<String, VecDeque<String>>;

const HOST: &str = "127.0.0.1";
const PORT: &str = "6379";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", HOST, PORT);
    let listener = TcpListener::bind(addr).await?;

    info(&format!("listening on port {}", PORT));

    let str_data = Arc::new(RwLock::new(Data::new()));
    let arr_data = Arc::new(RwLock::new(DataArr::new()));

    loop {
        let (mut stream, _) = listener.accept().await?;

        let str_data_cl = str_data.clone();
        let arr_data_cl = arr_data.clone();
        
        tokio::spawn(async move {
            let mut buf = [0; 512];  // TODO: tokyo mut buf

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
                        handle_set(k, v, ttl_ms, str_data_cl.clone()).await
                    }
                    Ok(Op::Get(k)) => {
                        info(&format!("GET {}", k));
                        handle_get(k, str_data_cl.clone()).await
                    }
                    Ok(Op::RPush(k, vs)) => {
                        info(&format!("RPUSH {}", k));
                        handle_push(k, vs, arr_data_cl.clone(), ArrOpType::Right).await
                    }
                    Ok(Op::LPush(k, vs)) => {
                        info(&format!("LPUSH {}", k));
                        handle_push(k, vs, arr_data_cl.clone(), ArrOpType::Left).await
                    }
                    Ok(Op::LRange(key, start, stop)) => {
                        info(&format!("LRANGE {} {} {}", key, start, stop));
                        handle_lrange(key, start, stop, arr_data_cl.clone()).await
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
