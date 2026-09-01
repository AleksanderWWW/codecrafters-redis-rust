use crate::resp::RespCommand::GetString;

pub enum RespCommand {
    Echo(String),
    Ping,
    SetString(String, String),
    GetString(String),
}

pub fn parse_resp(req: &[u8]) -> Result<RespCommand, &'static str> {
    let lowercase_req = req.to_ascii_lowercase();

    let mut parts = lowercase_req.split(|&window| window == b'\r');

    parts.next(); // temp

    parts.next(); //temp

    let cmd = match parts.next() {
        Some(val) => {
            if val == b"\nping" {
                return Ok(RespCommand::Ping);
            }
            val
        }
        None => return Err("invalid message"),
    };
    if cmd == b"\necho" {
        parts.next();
        let val = match parts.next() {
            Some(v) => v.strip_prefix(b"\n").unwrap_or(v),
            None => return Err("invalid message"),
        };
        return Ok(RespCommand::Echo(String::from(
            str::from_utf8(val).unwrap(),
        )));
    }

    if cmd == b"\nset" {
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

        return Ok(RespCommand::SetString(String::from(key), String::from(val)))
    }

    if cmd == b"\nget" {
        parts.next();

        let key = match parts.next() {
            Some(k) => str::from_utf8(k.strip_prefix(b"\n").unwrap_or(k)).unwrap(),
            None => return Err("invalid message"),
        };

        return Ok(GetString(String::from(key)));
    }

    Ok(RespCommand::Ping)
}
