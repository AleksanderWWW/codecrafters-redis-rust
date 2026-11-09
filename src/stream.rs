use std::{cmp::max, collections::{BTreeMap, HashMap}};
use std::time::{SystemTime, UNIX_EPOCH};
use std::ops::Bound::Included;
use itertools::Itertools;

use crate::{AUTO_SENTINEL, handlers::make_bulk_string};

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

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

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.milisecond_time, self.serial_num)
    }
}


#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub entry_id: StreamId,
    pub data: HashMap<String, String>
}


pub enum RespItem {
    Bulk(String),
    Array(Vec<RespItem>),
}

pub fn serialize_resp_item(item: &RespItem) -> String {
    match item {
        RespItem::Bulk(s) => make_bulk_string(s),
        RespItem::Array(arr) => {
            let inner = arr.iter().map(serialize_resp_item).join("");
            format!("*{}\r\n{}", arr.len(), inner)
        }
    }
}

pub fn entry_to_resp_arr(entry: &StreamEntry) -> RespItem {
    let mut kv_items = Vec::with_capacity(entry.data.len() * 2);
    for (k, v) in &entry.data {
        kv_items.push(RespItem::Bulk(k.clone()));
        kv_items.push(RespItem::Bulk(v.clone()));
    }

    RespItem::Array(vec![
        RespItem::Bulk(entry.entry_id.to_string()),
        RespItem::Array(kv_items),
    ])
}


#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>,
    pub latest_id: StreamId,
}

impl Stream {
    pub fn new(val: Option<StreamEntry>) -> Self {
        if let Some(v) = val {
            let id = v.entry_id;

            let mut entries = BTreeMap::new();
            entries.insert(id, v);
            Self {entries, latest_id: id}
        } else {
            Self{
                entries: BTreeMap::new(), 
                latest_id: StreamId { milisecond_time: 0, serial_num: 0},
            }
        }
    }

    pub fn insert(&mut self, val: StreamEntry) -> Result<StreamId, &'static str> {
        let id = val.entry_id;
        if id.milisecond_time == 0 && id.serial_num == 0 {
            return Err("The ID specified in XADD must be greater than 0-0")
        }

        let mut parsed_id = id;
        if parsed_id.milisecond_time == AUTO_SENTINEL {
            // account for clock skew - don't allow to go back in time
            parsed_id = parsed_id.replace_ms_time(
                max(current_timestamp_millis(), self.latest_id.milisecond_time)
            )
        }
        if parsed_id.serial_num == AUTO_SENTINEL {
            if parsed_id.milisecond_time == self.latest_id.milisecond_time {
                // e.g. latest 0-4, new 0-* so new parsed should be 0-5
                parsed_id = parsed_id.replace_seq(self.latest_id.serial_num + 1);
            } else {
                // e.g. latest: 0-4, new 1-* so new parsed should be 1-0
                parsed_id = parsed_id.replace_seq(0)
            }
        }

        if parsed_id <= self.latest_id{
            return Err("The ID specified in XADD is equal or smaller than the target stream top item")
        }
        self.entries.insert(parsed_id, val);
        self.latest_id = parsed_id;
        Ok(parsed_id)
    }

    pub fn range(&self, mut start: StreamId, stop: StreamId) -> Vec<&StreamEntry> {
        let mut res: Vec<&StreamEntry> = vec!();
        if start.serial_num == AUTO_SENTINEL {
            start = start.replace_seq(0)
        }
        for elem in self.entries.range(
            (
                Included(&start), Included(&stop)
            )
        ) {
            res.push(elem.1);
        };
        res
    }
}
