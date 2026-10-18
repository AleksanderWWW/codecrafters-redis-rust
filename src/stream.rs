use std::collections::{BTreeMap, HashMap};

pub type StreamId = String;

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub entry_id: StreamId,
    pub data: HashMap<String, String>
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>
}

impl Stream {
    pub fn new(val: Option<StreamEntry>) -> Self {
        if let Some(v) = val {
            let mut entries = BTreeMap::new();
            entries.insert(v.entry_id.clone(), v);
            Self {entries}
        } else {
            Self{entries: BTreeMap::new()}
        }
    }

    pub fn insert(&mut self, val: StreamEntry) {
        self.entries.insert(val.entry_id.clone(), val);
    }
}
