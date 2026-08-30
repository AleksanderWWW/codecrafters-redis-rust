#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};
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
                let mut buf = [0; 512];
                loop {
                    let bytes_read = stream.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
