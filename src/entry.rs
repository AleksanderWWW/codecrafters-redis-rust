use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct StringEntry {
    pub val: String,
    pub expires_at: Option<u128>,
}

impl StringEntry {
    pub fn new(val: String) -> Self {
        Self { val, expires_at: None }
    }

    pub fn with_ttl(val: String, ttl_ms: u128) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        Self {
            val,
            expires_at: Some(now_ms + ttl_ms),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            None => false,
            Some(expires_at) => expires_at < SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis()
        }
    }
}
