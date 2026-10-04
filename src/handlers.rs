use core::time;
use std::cmp::min;
use std::collections::vec_deque::Iter;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::thread::sleep;

use tokio::sync::RwLock;

use crate::entry::Entry;
use crate::log::info;
use crate::parser::EntryType;

type StrArc = Arc<RwLock<HashMap<String, Entry>>>;
type ArrArc = Arc<RwLock<HashMap<String, VecDeque<String>>>>;

pub fn handle_ping() -> String {
    make_pong_message()
}

pub fn handle_echo(val: &str) -> String {
    make_string_message(val)
}

pub async fn handle_set(k: String, v: EntryType, ttl_ms: Option<u128>, value: StrArc) -> String {
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

pub async fn handle_get(k: String, value: StrArc) -> String {
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
    arr_value: ArrArc,
    op_type: ArrOpType,
) -> String {
    let l = {
        let mut db = arr_value.write().await;

        let arr = db.entry(k).or_default();

        for v in vs.iter() {
            match op_type {
                ArrOpType::Right => arr.push_back(v.to_string()),
                ArrOpType::Left => arr.push_front(v.to_string()),
            }
        }

        arr.len()
    };

    make_int_message(l as i64)
}

pub async fn handle_lrange(key: String, start: i32, stop: i32, arr_value: ArrArc) -> String {
    let default: VecDeque<String> = VecDeque::new();
    let msg = {
        if start_idx_truly_bigger(start, stop) {
            make_empty_range_message()
        } else {
            let db = arr_value.read().await;

            let entry = db.get(&key).unwrap_or(&default);

            let u_start: usize = match start < 0 {
                true => {
                    let tmp = entry.len() as i32 + start;
                    match usize::try_from(tmp) {
                        Ok(val) => val,
                        Err(_) => 0,
                    }
                }
                false => start as usize,
            };

            let u_stop: usize = match stop < 0 {
                true => {
                    let tmp = entry.len() as i32 + stop;
                    match usize::try_from(tmp) {
                        Ok(val) => val,
                        Err(_) => 0,
                    }
                }
                false => stop as usize,
            };

            let rg: Iter<String> = {
                if entry.len() <= u_start {
                    entry.range(0..0)
                } else {
                    if u_stop >= entry.len() {
                        entry.range(u_start..)
                    } else {
                        entry.range(u_start..u_stop + 1)
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

pub async fn handle_llen(k: String, arr_value: ArrArc) -> String {
    let default: VecDeque<String> = VecDeque::new();
    let db = arr_value.read().await;

    let entry = db.get(&k).unwrap_or(&default);

    make_int_message(entry.len() as i64)
}

pub async fn handle_lpop(k: String, n: Option<u64>, arr_value: ArrArc) -> String {
    let mut db = arr_value.write().await;

    match db.get_mut(&k) {
        None => make_empty_message(),
        Some(val) => {
            match n {
                None => {
                    match val.pop_front() {
                        None => make_empty_message(),
                        Some(elem) => make_string_message(&elem),
                }
            }
                Some(n_elems) => {
                    do_lpop(n_elems, val)
                }
            }
        }
    }
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

pub async fn handle_blpop(k: String, timeout: u32, arr_value: ArrArc) -> String {
    
    loop {
        let mut db = arr_value.write().await;

        match db.get_mut(&k) {
            None => {
                sleep(time::Duration::from_millis(1000));
                continue;
            },
            Some(val) => {
                if val.len() == 0 {
                    sleep(time::Duration::from_millis(1000));
                    continue;
                }

                val.push_front(k);
                
                return do_lpop(2, val)
            }
        }
    }
}

pub async fn handle_err() -> String {
    make_empty_message()
}

fn make_pong_message() -> String {
    String::from("+PONG\r\n")
}

fn make_string_message(val: &str) -> String {
    format!("${}\r\n{}\r\n", val.len(), val)
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
