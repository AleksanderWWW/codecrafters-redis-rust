use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Copy)]
pub struct StreamId {
    pub milisecond_time: u64,
    pub serial_num: u64
}

impl StreamId {
    pub fn replace_ms_time(&self, val: u64) -> Self {
        return Self {
            milisecond_time: val,
            serial_num: self.serial_num
        }
    }

    pub fn replace_seq(&self, val: u64) -> Self {
        return Self {
            milisecond_time: self.milisecond_time,
            serial_num: val
        }
    }
}


#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub entry_id: StreamId,
    pub data: HashMap<String, String>
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>,
    pub latest_id: StreamId
}

impl Stream {
    pub fn new(val: Option<StreamEntry>) -> Self {
        if let Some(v) = val {
            let id = v.entry_id;

            let mut entries = BTreeMap::new();
            entries.insert(id, v);
            Self {entries, latest_id: id}
        } else {
            Self{entries: BTreeMap::new(), latest_id: StreamId { milisecond_time: 0, serial_num: 0}}
        }
    }

    pub fn insert(&mut self, val: StreamEntry) -> Result<StreamId, &'static str> {
        let id = val.entry_id;
        if id.milisecond_time == 0 && id.serial_num == 0 {
            return Err("The ID specified in XADD must be greater than 0-0")
        }

        let mut parsed_id = id;
        if parsed_id.serial_num == u64::MAX {
            parsed_id = parsed_id.replace_seq(id.serial_num + 1);
        }

        if id <= self.latest_id{
            return Err("The ID specified in XADD is equal or smaller than the target stream top item")
        }
        self.entries.insert(parsed_id, val);
        self.latest_id = parsed_id;
        Ok(parsed_id)
    }
}
