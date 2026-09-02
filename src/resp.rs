use crate::resp::RespCommand::GetString;

pub enum RespCommand {
    Echo(String),
    Ping,
    SetString(String, String, Option<u128>),
    GetString(String),
}

const PING: &[u8; 4] = b"ping";
const ECHO: &[u8; 4] = b"echo";
const SET: &[u8; 3] = b"set";
const GET: &[u8; 3] = b"get";

pub fn parse_resp(req: &[u8]) -> Result<RespCommand, &'static str> {
    let mut parts = req.split(|&window| window == b'\r');

    parts.next(); // temp

    parts.next(); //temp

    let cmd = match parts.next() {
        Some(val) => {
            let v = val.strip_prefix(b"\n").unwrap_or(val).to_ascii_lowercase();
            v
        }
        None => return Err("invalid message"),
    };

    if cmd == PING {
        return Ok(RespCommand::Ping);
    }

    if cmd == ECHO {
        parts.next();
        let val = match parts.next() {
            Some(v) => v.strip_prefix(b"\n").unwrap_or(v),
            None => return Err("invalid message"),
        };
        return Ok(RespCommand::Echo(String::from(
            str::from_utf8(val).unwrap(),
        )));
    }

    if cmd == SET {
        parts.next();

        let key = match parts.next() {
            Some(k) => str::from_utf8(k.strip_prefix(b"\n").unwrap_or(k)).unwrap(),
            None => return Err("invalid message"),
        };

        parts.next();

        let val = match parts.next() {
            Some(v) => str::from_utf8(v.strip_prefix(b"\n").unwrap_or(v)).unwrap(),
            None => return Err("invalid message"),
        };

        let is_ttl_set = match parts.next() {
            None => false,
            Some(_) => {
                match parts.next() {
                    None => false,
                    Some(val) => {
                        val.to_ascii_lowercase() == b"\npx"
                    }
                }
            }
        };

        let ttl: Option<u128> = if is_ttl_set {
            parts.next();
            match parts.next() {
                None => None,
                Some(val) => match parse_ascii_u128(val.strip_prefix(b"\n").unwrap_or(val)) {
                    Err(_) => {
                        return Err("failed to parse ttl")
                    }
                    Ok(v) => Some(v)
                },
            }
        } else {
            None
        };
        
        return Ok(RespCommand::SetString(String::from(key), String::from(val), ttl));
    }

    if cmd == GET {
        parts.next();

        let key = match parts.next() {
            Some(k) => str::from_utf8(k.strip_prefix(b"\n").unwrap_or(k)).unwrap(),
            None => return Err("invalid message"),
        };

        return Ok(GetString(String::from(key)));
    }

    Err("invalid message")
}


fn parse_ascii_u128(bytes: &[u8]) -> Result<u128, std::num::ParseIntError> {
    let s = std::str::from_utf8(bytes).map_err(|_| {
        // Fallback for invalid UTF-8
        "invalid string".parse::<u128>().unwrap_err()
    })?;
    
    s.trim().parse::<u128>()
}
