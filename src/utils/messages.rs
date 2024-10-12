pub enum RedisMessageType {
    SimpleString(String),
}

pub trait RespEncoder {
    fn encode(&self) -> Vec<u8>;
}

pub trait RespDecoder {
    fn decode(input: Vec<u8>) -> RedisMessageType;
}

impl RespEncoder for RedisMessageType {
    fn encode(&self) -> Vec<u8> {
        match self {
            RedisMessageType::SimpleString(data) => format!("+{}\r\n", data).as_bytes().to_vec(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::RedisMessageType;
    use super::RespEncoder;

    #[test]
    fn encode_simple_string() {
        let input = RedisMessageType::SimpleString("Test".into());
        let expected = "+Test\r\n".as_bytes().to_vec();

        assert_eq!(expected, input.encode())
    }
}
