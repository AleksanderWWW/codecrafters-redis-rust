use std::cmp::min;
use std::collections::vec_deque::Iter;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::log::info;
use crate::entry::EntryType;
use crate::stream::{Stream, StreamEntry};

type EntryArc = Arc<RwLock<HashMap<String, Entry>>>;

pub fn handle_ping() -> String {
    make_pong_message()
}

pub fn handle_echo(val: &str) -> String {
    make_string_message(val)
}

pub async fn handle_set(k: String, v: EntryType, ttl_ms: Option<u128>, value: EntryArc) -> String {
    let entry = match ttl_ms {
        None => Entry::new(v),
        Some(ttl) => Entry::with_ttl(v, ttl),
    };

    {
        let mut db = value.write().await;
        db.insert(k, entry);
    }
    make_ok_message()
}

pub async fn handle_get(k: String, value: EntryArc) -> String {
    let val: Option<Entry> = {
        let guard = value.read().await;
        match guard.get(&k) {
            None => None,
            Some(e) => Some(Entry {
                val: e.val.clone(),
                expires_at: e.expires_at,
            }),
        }
    };

    match val {
        None => make_empty_message(),
        Some(v) => {
            if v.is_expired() {
                {
                    info("value present, but expired - removing");
                    let mut guard = value.write().await;
                    guard.remove(&k);
                }
                make_empty_message()
            } else {
                let src = match v.val {
                    EntryType::Str(val) => make_bulk_string(&val),
                    _ => make_empty_message(),
                };
                src
            }
        }
    }
}

pub enum ArrOpType {
    Left,
    Right,
}

pub async fn handle_push(
    k: String,
    vs: Vec<String>,
    arr_value: EntryArc,
    op_type: ArrOpType,
) -> String {
    let mut db = arr_value.write().await;

    let arr = db.entry(k).or_insert_with(|| Entry { val: EntryType::List(VecDeque::default()), expires_at: None });

    if arr.is_expired() {
        arr.val = EntryType::List(VecDeque::new());
        arr.expires_at = None;
    }

    if let EntryType::List(ref mut _arr) = arr.val {
        for v in vs.iter() {
            match op_type {
                ArrOpType::Right => _arr.push_back(v.to_string()),
                ArrOpType::Left => _arr.push_front(v.to_string()),
            }
        }
        return make_int_message(_arr.len() as i64)
    } else {
        return make_wrongtype_message()
    }
}

pub async fn handle_lrange(key: String, start: i32, stop: i32, arr_value: EntryArc) -> String {
    let msg = {
        if start_idx_truly_bigger(start, stop) {
            make_empty_range_message()
        } else {
            let db = arr_value.read().await;

            let entry = match db.get(&key) {
                Some(en) => en,
                None => return make_empty_range_message()
            };
            
            let arr = match entry.val {
                EntryType::List(ref arr) => arr,
                _ => return make_wrongtype_message()
            };

            let u_start: usize = match start < 0 {
                true => {
                    let tmp = arr.len() as i32 + start;
                    match usize::try_from(tmp) {
                        Ok(val) => val,
                        Err(_) => 0,
                    }
                }
                false => start as usize,
            };

            let u_stop: usize = match stop < 0 {
                true => {
                    let tmp = arr.len() as i32 + stop;
                    match usize::try_from(tmp) {
                        Ok(val) => val,
                        Err(_) => 0,
                    }
                }
                false => stop as usize,
            };

            let rg: Iter<String> = {
                if arr.len() <= u_start {
                    arr.range(0..0)
                } else {
                    if u_stop >= arr.len() {
                        arr.range(u_start..)
                    } else {
                        arr.range(u_start..u_stop + 1)
                    }
                }
            };

            make_range_message(rg.collect())
        }
    };
    msg
}

fn start_idx_truly_bigger(start: i32, stop: i32) -> bool {
    start > 0 && stop > 0 && start > stop
}

pub async fn handle_llen(k: String, arr_value: EntryArc) -> String {
    let db = arr_value.read().await;

    let Some(entry) = db.get(&k) else {
        return make_int_message(0)
    };

    match entry.val {
        EntryType::List(ref arr) => return make_int_message(arr.len() as i64),
        _ => return make_wrongtype_message(),
    };
    
}

pub async fn handle_lpop(k: String, n: Option<u64>, arr_value: EntryArc) -> String {
    let mut db = arr_value.write().await;

    let Some(entry) = db.get_mut(&k) else {
        return make_empty_message();
    };

    let EntryType::List(ref mut arr) = entry.val else {
        return make_wrongtype_message()
    };

    let msg = match n {
        None => {
            match arr.pop_front() {
                None => make_empty_message(),
                Some(elem) => make_string_message(&elem),
            }
        }
        Some(n_elems) => {
            do_lpop(n_elems, arr)
        }
    };

    msg
}

fn do_lpop(n_elems: u64, val: &mut VecDeque<String>) -> String {
    let n_to_remove = min(n_elems, val.len() as u64);
    let mut removed_elems: Vec<String> = vec!();

    for _ in 0..n_to_remove {
        match val.pop_front() {
            None => break,
            Some(x) => removed_elems.push(x),
        }
    }

    make_range_message(removed_elems.iter().collect())
}

pub async fn handle_blpop(k: String, timeout: f64, arr_value: EntryArc) -> String {
    
    let deadline = Instant::now() + Duration::from_millis((timeout * 1000.0) as u64);

    let has_timeout = timeout > 0.0;

    loop {
        let popped_val = {
            let mut db = arr_value.write().await;

            
            if let Some(entry) = db.get_mut(&k) {
                if let EntryType::List(ref mut arr) = entry.val {
                    arr.pop_front()
                } else {
                    return make_wrongtype_message()
                }
            } else {
                None
            }
        };

        if let Some(elem) = popped_val {
            return make_range_message(vec![&k, &elem]);
        }

        if has_timeout && Instant::now() > deadline {
            return make_null_array_message()
        }
        sleep(std::time::Duration::from_millis(10)).await;
    }
}

pub async fn handle_type(k: String, arr_value: EntryArc) -> String {
    let db = arr_value.read().await;

    let Some(entry) = db.get(&k) else {
        return make_simple_string_message("none")
    };

    match entry.val {
        EntryType::List(_) => return make_simple_string_message("list"),
        EntryType::Str(_) => return make_simple_string_message("string"),
        EntryType::Stream(_) => return make_simple_string_message("stream")
    }
}

pub async fn handle_xadd(k: String, se: StreamEntry, arr_value: EntryArc) -> String {
    let mut db = arr_value.write().await;

    let entry_id = se.entry_id;

    let entry = db.entry(k).or_insert_with(|| {
        Entry::new(EntryType::Stream(Stream::new(None)))
    });

    match entry.val {
        EntryType::Stream(ref mut s) => {
            if let Err(e) = s.insert(se) {
                return make_error_message(e)
            }
        },
        _ => return make_wrongtype_message()
    }

    make_bulk_string(&format!("{}-{}", entry_id.milisecond_time, entry_id.serial_num))
}

pub async fn handle_err() -> String {
    make_empty_message()
}


fn make_error_message(val: &str) -> String {
    format!("-ERR {}\r\n", val)
}

fn make_pong_message() -> String {
    String::from("+PONG\r\n")
}

fn make_string_message(val: &str) -> String {
    format!("${}\r\n{}\r\n", val.len(), val)
}

fn make_simple_string_message(val: &str) -> String {
    format!("+{}\r\n", val)
}

fn make_ok_message() -> String {
    String::from("+OK\r\n")
}

fn make_empty_message() -> String {
    String::from("$-1\r\n")
}

fn make_bulk_string(val: &str) -> String {
    format!("${}\r\n{}\r\n", val.len(), val)
}

fn make_int_message(val: i64) -> String {
    format!(":{}\r\n", val)
}

fn make_empty_range_message() -> String {
    format!("*{}\r\n", 0)
}

fn make_range_message(rg: Vec<&String>) -> String {
    let l = rg.len();

    let mut msg = format!("*{}\r\n", l);

    for elem in rg {
        msg = msg + &format!("${}\r\n{}\r\n", elem.len(), elem);
    }

    msg
}

fn make_null_array_message() -> String {
    format!("*{}\r\n", -1)
}

pub fn make_wrongtype_message() -> String {
    String::from("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}
