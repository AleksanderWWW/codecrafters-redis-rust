use std::time::{SystemTime, UNIX_EPOCH};

use crate::parser::EntryType;

#[derive(Debug, Clone)]
pub struct Entry {
    pub val: EntryType,
    pub expires_at: Option<u128>,
}

impl Entry {
    pub fn new(val: EntryType) -> Self {
        Self { val, expires_at: None }
    }

    pub fn with_ttl(val: EntryType, ttl_ms: u128) -> Self {
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
