use std::{collections::HashMap, slice::Iter};

use crate::entry::EntryType;
use crate::log::info;
use crate::stream::{StreamEntry, StreamId};


#[derive(Debug)]
pub enum Op {
    Ping,
    Echo(String),
    Get(String),
    Set(String, EntryType, Option<u128>),
    RPush(String, Vec<String>),
    LPush(String, Vec<String>),
    LRange(String, i32, i32),
    Llen(String),
    Lpop(String, Option<u64>),
    Blpop(String, f64),
    Type(String),
    XAdd(String, StreamEntry)
}

const MAX_ARGS: usize = 10_000;

pub fn parse(req: &[u8]) -> Result<Op, &'static str> {
    let mut ri = req.iter();

    // first byte
    match ri.next() {
        Some(val) => {
            if *val != b'*' {
                return Err("invalid message");
            }
        }
        None => return Err("invalid message"),
    };

    let num_args = match get_num_bytes(&mut ri) {
        None => return Err("invalid message"),
        Some(val) => val,
    };

    if num_args > MAX_ARGS {
        return Err("too many arguments");
    }

    let mut args: Vec<String> = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        args.push(parse_bulk_string(&mut ri)?)
    }

    info(&format!("{:?}", args));

    match args[0].to_ascii_lowercase().as_str() {
        "ping" => Ok(Op::Ping),
        "echo" => {
            if args.len() < 2 {
                Err("invalid message")
            } else {
                let msg = args[1].clone();
                Ok(Op::Echo(msg))
            }
        }
        "get" => {
            if args.len() < 2 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                Ok(Op::Get(key))
            }
        }
        "set" => {
            if args.len() < 3 {
                Err("invalid message")
            } else {
                let (key, val) = (args[1].clone(), args[2].clone());
                let mut ttl: Option<u128> = None;
                if args.len() == 5 && args[3].to_ascii_lowercase() == "px" {
                    ttl = match args[4].trim().parse::<u128>() {
                        Err(_) => None,
                        Ok(val) => Some(val),
                    };
                }

                Ok(Op::Set(key, EntryType::Str(val), ttl))
            }
        }
        "rpush" => {
            if args.len() < 3 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                let mut res: Vec<String> = vec![];

                for elem in args[2..].iter() {
                    res.push(elem.to_string());
                }
                Ok(Op::RPush(key, res))
            }
        }
        "lpush" => {
            if args.len() < 3 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                let mut res: Vec<String> = vec![];

                for elem in args[2..].iter() {
                    res.push(elem.to_string());
                }
                Ok(Op::LPush(key, res))
            }
        }
        "lrange" => {
            if args.len() < 4 {
                Err("invalid message")
            } else {
                let key = args[1].clone();

                let start = match args[2].trim().parse::<i32>() {
                    Err(_) => return Err("invalid message"),
                    Ok(val) => Some(val),
                };

                let stop = match args[3].trim().parse::<i32>() {
                    Err(_) => return Err("invalid message"),
                    Ok(val) => Some(val),
                };

                Ok(Op::LRange(key, start.unwrap(), stop.unwrap()))
            }
        }
        "llen" => {
            if args.len() < 2 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                Ok(Op::Llen(key))
            }
        }
        "lpop" => {
            if args.len() < 2 {
                Err("too few arguments to LPOP")
            } else {
                let key = args[1].clone();
                if args.len() == 3 {
                    let n = match args[2].trim().parse::<u64>() {
                        Err(_) => return Err("could not parse the second argument"),
                        Ok(val) => val
                    };
                    Ok(Op::Lpop(key, Some(n)))
                } else {
                    Ok(Op::Lpop(key, None))
                }
                
            }
        }
        "blpop" => {
            if args.len() < 3 {
                Err("too few arguments to BLPOP")
            } else {
                let key = args[1].clone();
                let timeout = match args[2].trim().parse::<f64>() {
                    Err(_) => return Err("could not parse timout to BLPOP"),
                    Ok(val) => val
                };

                Ok(Op::Blpop(key, timeout))

            }
        }
        "type" => {
              if args.len() < 2 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                Ok(Op::Type(key))
            }
        }
        "xadd" => {
            if args.len() < 3 {
                Err("invalid message")
            } else {
                let key = args[1].clone();
                let id = args[2].clone();
                let data: HashMap<String, String> = HashMap::new();

                let parts: Vec<&str> = id.split("-").collect();

                let mili_time: u64;
                if parts[0].trim() == "*" {
                    mili_time = u64::MAX;
                } else {
                    mili_time = match parts[0].trim().parse::<u64>() {
                        Ok(val) => val,
                        Err(_) => return Err("Could not parse milisecond time from ID")
                    };
                }

                let seq: u64;
                if parts[1].trim() == "*" {
                    seq = u64::MAX;
                } else {
                    seq = match parts[1].trim().parse::<u64>() {
                        Ok(val) => val,
                        Err(_) => return Err("Could not parse milisecond time from ID")
                    };
                }

                Ok(Op::XAdd(key, StreamEntry{
                    entry_id: StreamId{
                        milisecond_time: mili_time,
                        serial_num: seq
                    },
                    data
                }))
            }
        }
        _ => Err("invalid message"),
    }
}

fn get_num_bytes(ri: &mut Iter<'_, u8>) -> Option<usize> {
    let mut num_bytes: usize = 0;
    loop {
        match ri.next() {
            None => return None,
            Some(&val) => {
                if val == b'\r' {
                    continue;
                }
                if val == b'\n' {
                    break;
                }

                if val.is_ascii_digit() {
                    num_bytes = num_bytes * 10 + (val - b'0') as usize;
                }
            }
        }
    }

    Some(num_bytes)
}

fn parse_bulk_string(ri: &mut Iter<'_, u8>) -> Result<String, &'static str> {
    match ri.next() {
        Some(&b'$') => {}
        _ => return Err("invalid message: expected '$'"),
    }

    let num_bytes = match get_num_bytes(ri) {
        None => return Err("invalid message"),
        Some(val) => val,
    };

    let mut s: Vec<u8> = Vec::with_capacity(num_bytes);
    for _ in 0..num_bytes {
        match ri.next() {
            None => return Err("invalid message"),
            Some(&val) => s.push(val),
        }
    }

    // consume the last \r\n
    match (ri.next(), ri.next()) {
        (Some(&b'\r'), Some(&b'\n')) => {}
        _ => return Err("invalid message: missing trailing CRLF"),
    }

    String::from_utf8(s).map_err(|_| "invalid message")
}
