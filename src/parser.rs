use std::slice::Iter;

use crate::log::info;

#[derive(Debug, Clone)]
pub enum EntryType {
    Str(String),
    Int(i128),
    Bool(bool),
}

#[derive(Debug)]
pub enum Op {
    Ping,
    Echo(String),
    Get(String),
    Set(String, EntryType, Option<u128>),
    RPush(String, Vec<String>)
}

const MAX_ARGS: usize = 10_000;


pub fn parse(req: &[u8]) -> Result<Op, &'static str> {
    let mut ri = req.iter();

    // first byte
    match ri.next() {
        Some(val) => {
            if *val != b'*' { 
                return Err("invalid message")
            }
        }
        None => return Err("invalid message")
    };

    
    let num_args = match get_num_bytes(&mut ri) {
        None => return Err("invalid message"),
        Some(val) => val
    };

    if num_args > MAX_ARGS {
        return Err("too many arguments")
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
                Err("invalid error")
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
                let mut res: Vec<String> = vec!();

                for elem in args[2..].iter() {
                    res.push(elem.to_string());
                }
                Ok(Op::RPush(key, res))
            }
        }
        _ => Err("invalid message") 
    }
    
}

fn get_num_bytes(ri: &mut Iter<'_, u8>) -> Option<usize>{
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
        Some(&b'$') => {},
        _ => return Err("invalid message: expected '$'"),
    }

    let num_bytes = match get_num_bytes(ri) {
        None => return Err("invalid message"),
        Some(val) => val
    };

    let mut s: Vec<u8> = Vec::with_capacity(num_bytes);
    for _ in 0..num_bytes {
        match ri.next() {
            None => return Err("invalid message"),
            Some(&val) => s.push(val),
        }
    };

    // consume the last \r\n
    match (ri.next(), ri.next()) {
        (Some(&b'\r'), Some(&b'\n')) => {}
        _ => return Err("invalid message: missing trailing CRLF"),
    }

    String::from_utf8(s).map_err(|_| "invalid message")
}
