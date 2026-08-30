#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};
use chrono::prelude::*;

fn info(msg: &str) {
    println!("{}", format!("[INFO] [{}] {}", Utc::now().to_string(), msg));
}

fn main() {
    info("Listening on port 6379");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                info("new connection");
                stream.write_all("+PONG\r\n".as_bytes());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
