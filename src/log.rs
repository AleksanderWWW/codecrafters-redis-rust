use chrono::Utc;

pub fn info(msg: &str) {
    println!("[INFO] [{}] {}", Utc::now().to_string(), msg);
}

pub fn error(msg: &str) {
    eprintln!("[ERROR] [{}] {}", Utc::now().to_string(), msg);
}
