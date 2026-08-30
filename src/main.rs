#![allow(unused_imports)]
use std::net::TcpListener;
use chrono::prelude::*;

fn info(msg: &str) {
    println!("{}", format!("[INFO] [{}] {}", Utc::now().to_string(), msg));
}

fn main() {
    info("Listening on port 6379");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
