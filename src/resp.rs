pub enum RespCommand {
    Echo(String),
    Ping,
}

pub fn parse_resp(req: &[u8]) -> Result<RespCommand, &'static str> {
    let lowercase_req = req.to_ascii_lowercase();

    let mut parts = lowercase_req.split(|&window| window == b'\r');


    parts.next(); // temp

    parts.next(); //temp

    let msg = match parts.next() {
        Some(val) => {
            if val == b"\nping" {
                return Ok(RespCommand::Ping)
            }
            val
        }
        None => return Err("invalid message")
    };
    if msg == b"\necho" {
        parts.next();
        let val = match parts.next() {
            Some(v) => v.strip_prefix(b"\n").unwrap_or(v),
            None => return Err("invalid message")
        };
        return Ok(RespCommand::Echo(String::from(str::from_utf8(val).unwrap())))
    }

    Ok(RespCommand::Ping)
}
