use crate::consts::{CR, CRLF, CRLF1, LF};
use anyhow::{anyhow, Result};
use parse_utils::{parse_numeric_value_till_CRLF, split_vec_at_CRLF};
use std::collections::VecDeque;
use std::{fmt::Display, ops::Deref};

mod parse_utils {
    use super::*;

    pub fn split_vec_at_CRLF(mut data: VecDeque<u8>) -> Result<(VecDeque<u8>, VecDeque<u8>)> {
        let mut parsed_value = VecDeque::new();
        loop {
            let val = data
                .pop_front()
                .ok_or(anyhow!("Missing char in redis message string"))?;
            if val == CR && Some(&LF) == data.get(0) {
                data.pop_front();
                break;
            }

            parsed_value.push_back(val);
        }

        return Ok((parsed_value, data));
    }

    pub fn parse_numeric_value_till_CRLF(data: &mut VecDeque<u8>) -> Result<usize> {
        let mut length = 0;
        loop {
            let char = data
                .pop_front()
                .ok_or(anyhow!("Unexpected end while reading length"))?;

            match char {
                b'0'..=b'9' => {
                    length = length * 10 + (char - b'0') as usize;
                }
                CR => {
                    if data.pop_front() != Some(LF) {
                        return Err(anyhow!("Invalid CRLF after numeric value"));
                    }
                    break;
                }
                _val => {
                    return Err(anyhow!(
                        "Invalid character in numeric value data '{}'",
                        _val as char
                    ))
                }
            }
        }

        return Ok(length);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisMessageType {
    SimpleString(VecDeque<u8>),
    Error(VecDeque<u8>),
    BulkString(VecDeque<u8>),
    NullBulkString,
    Integer(i64),
    Array(VecDeque<RedisMessageType>),
}

impl Display for RedisMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "{}", self.message_type());
    }
}

impl RedisMessageType {
    pub fn message_type(&self) -> &'static str {
        return match self {
            Self::SimpleString(_) => "SimpleString",
            Self::Error(_) => "Error",
            Self::Array(_) => "Array",
            Self::BulkString(_) => "BulkString",
            Self::NullBulkString => "NullBulkString",
            Self::Integer(_) => "Integer",
        };
    }

    pub fn decode(data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
        return match data.get(0) {
            Some(b'+') => parse_simple_string(data),
            Some(b'-') => parse_error_string(data),
            Some(b'$') => parse_bulk_string(data),
            Some(b':') => parse_integer(data),
            Some(b'*') => parse_array(data),
            Some(_val) => Err(anyhow!("Unhandled first_char in redis data: '{}'", _val)),
            None => Err(anyhow!("Missing char in redis message string!")),
        };
    }

    pub fn encode(mut self) -> VecDeque<u8> {
        match self {
            Self::SimpleString(mut data) => {
                data.push_front(b'+');
                data.push_back(CR);
                data.push_back(LF);
                data
            }
            Self::Error(mut data) => {
                data.push_front(b'-');
                data.push_back(CR);
                data.push_back(LF);
                data
            }
            Self::BulkString(data) => encode_bulk_string(data),
            Self::NullBulkString => format!("$-1{CRLF}").as_bytes().to_vec().into(),
            Self::Integer(data) => format!(":{}{CRLF}", data).as_bytes().to_vec().into(),
            Self::Array(data) => encode_array(data).into(),
        }
    }

    pub fn simple_string<S: Into<String>>(s: S) -> RedisMessageType {
        return RedisMessageType::SimpleString(s.into().into_bytes().into());
    }

    pub fn error<S: Into<String>>(s: S) -> RedisMessageType {
        return RedisMessageType::Error(s.into().into_bytes().into());
    }

    pub fn bulk_string<S: Into<String>>(s: S) -> RedisMessageType {
        return RedisMessageType::BulkString(s.into().into_bytes().into());
    }

    pub fn bulk_string_array<S: Into<String>>(values: Vec<S>) -> Self {
        let value = values.into_iter().map(|v| RedisMessageType::bulk_string(v)).collect();
        return RedisMessageType::Array(value);
    }

    /// returns the value if self is of type BulkString
    /// Else returns a RedisMessageType::Error with an error message
    pub fn bulk_string_value(&self) -> Result<String, RedisMessageType> {
        return match self {
            Self::BulkString(val) => Ok(String::from_utf8_lossy(val.clone().make_contiguous()).to_string()),
            _ => Err(Self::error(format!(
                "Expected BulkString not {}",
                self.message_type()
            ))),
        };
    }

}

fn encode_bulk_string(mut data: VecDeque<u8>) -> VecDeque<u8> {
    let mut val = format!("${}{CRLF}", data.len()).as_bytes().to_vec();
    while let Some(x) = val.pop() {
        data.push_front(x);
    }

    data.push_back(CR);
    data.push_back(LF);

    return data;
}

fn encode_array(mut data: VecDeque<RedisMessageType>) -> VecDeque<u8> {
    let mut encoded = VecDeque::new();

    encoded.extend(b"*");
    encoded.extend(data.len().to_string().as_bytes());
    encoded.extend(b"\r\n");

    for v in data {
        encoded.append(&mut v.encode());
    }

    return encoded;
}

fn parse_simple_string(mut data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
    if b'+'
        != data
            .pop_front()
            .ok_or(anyhow!("Missing char in redis message string"))?
    {
        return Err(anyhow!("Simple string must begin with '+'!"));
    }

    let (string_value, data) = split_vec_at_CRLF(data)?;

    return Ok((RedisMessageType::SimpleString(string_value), data));
}

fn parse_error_string(mut data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
    if b'-'
        != data
            .pop_front()
            .ok_or(anyhow!("Missing char in redis message error"))?
    {
        return Err(anyhow!("Error string must begin with '-'!"));
    }

    let (error_value, data) = split_vec_at_CRLF(data)?;

    return Ok((RedisMessageType::Error(error_value), data));
}

fn parse_integer(mut data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
    if b':'
        != data
            .pop_front()
            .ok_or(anyhow!("Missing char in redis message integer"))?
    {
        return Err(anyhow!("Simple string must begin with '+'!"));
    }

    let sign = data
        .get(0)
        .ok_or(anyhow!("Missing char in redis message integer"))?;
    let is_negative = match sign {
        b'-' => {
            data.pop_front();
            true
        }
        b'+' => {
            data.pop_front();
            false
        }
        b'0'..b'9' => false,
        _val => {
            return Err(anyhow!(
                "Sign missing and first char in Integer message not numeric {}",
                _val
            ))
        }
    };

    let mut value = parse_numeric_value_till_CRLF(&mut data)? as i64;
    if is_negative {
        value *= -1
    };

    return Ok((RedisMessageType::Integer(value), data));
}

fn parse_bulk_string(mut data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
    if b'$'
        != data
            .pop_front()
            .ok_or(anyhow!("Missing char in redis message string"))?
    {
        return Err(anyhow!("Bulk string must begin with '$'!"));
    }

    let data_length = parse_numeric_value_till_CRLF(&mut data)?;

    if data.len() < data_length + 2 {
        return Err(anyhow!("Bulk string shorter than declared length"));
    }
    let payload: VecDeque<u8> = data.drain(..data_length).collect();

    if data.pop_front() != Some(CR) {
        return Err(anyhow!("Bulk string must end on '\\r'"));
    }

    if data.pop_front() != Some(LF) {
        return Err(anyhow!("Bulk string must end on '\\r\\n'"));
    }

    return Ok((RedisMessageType::BulkString(payload), data));
}

fn parse_array(mut data: VecDeque<u8>) -> Result<(RedisMessageType, VecDeque<u8>)> {
    if b'*'
        != data
            .pop_front()
            .ok_or(anyhow!("Missing char in redis message string"))?
    {
        return Err(anyhow!("Array must begin with '*'!"));
    }

    let data_length = parse_numeric_value_till_CRLF(&mut data)?;
    let mut array = VecDeque::with_capacity(data_length);

    for _ in 0..data_length {
        let (element, new_data) = RedisMessageType::decode(data)?;
        data = new_data;
        array.push_back(element);
    }

    return Ok((RedisMessageType::Array(array), data));
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(test)]
    mod test_simple_string {
        use super::*;

        #[test]
        fn decode_valid_string() {
            let expected = RedisMessageType::SimpleString("Test".as_bytes().to_vec().into());
            let input = "+Test\r\n".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert!(result_rest.is_empty());
        }

        #[test]
        fn decode_empty_string() {
            let expected = RedisMessageType::SimpleString(vec![].into());
            let input = "+\r\nasdf".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert_eq!(4, result_rest.len())
        }

        #[test]
        fn compare_constructor_string() {
            let expected = RedisMessageType::SimpleString("Test".as_bytes().to_vec().into());
            let result = RedisMessageType::simple_string("Test");

            assert_eq!(expected, result);
        }

        #[test]
        fn encode() {
            let input = RedisMessageType::SimpleString("Test".as_bytes().to_vec().into());
            let expected: VecDeque<u8> = "+Test\r\n".as_bytes().to_vec().into();

            let result = input.encode();

            assert_eq!(expected, result);
        }
    }

    #[cfg(test)]
    mod test_error_string {
        use super::*;

        #[test]
        fn decode_valid_string() {
            let expected = RedisMessageType::Error("Test".as_bytes().to_vec().into());
            let input = "-Test\r\n".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert!(result_rest.is_empty())
        }

        #[test]
        fn decode_empty_string() {
            let expected = RedisMessageType::Error("".as_bytes().to_vec().into());
            let input = "-\r\nasd".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert_eq!(3, result_rest.len())
        }

        #[test]
        fn compare_constructor_string() {
            let expected = RedisMessageType::Error("Test".as_bytes().to_vec().into());
            let result = RedisMessageType::error("Test");

            assert_eq!(expected, result);
        }

        #[test]
        fn encode() {
            let input = RedisMessageType::Error("Test".as_bytes().to_vec().into());
            let expected: VecDeque<u8> = "-Test\r\n".as_bytes().to_vec().into();

            let result = input.encode();

            assert_eq!(expected, result)
        }
    }

    #[cfg(test)]
    mod test_bulk_string {
        use super::*;

        #[test]
        fn decode_valid_string() {
            let expected = RedisMessageType::BulkString("Test".as_bytes().to_vec().into());
            let input = "$4\r\nTest\r\nasdf".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert_eq!(4, result_rest.len());
        }

        #[test]
        fn decode_empty_string() {
            let expected = RedisMessageType::BulkString("".as_bytes().to_vec().into());
            let input = "$0\r\n\r\n".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert!(result_rest.is_empty());
        }

        #[test]
        fn compare_constructor() {
            let expected = RedisMessageType::BulkString("Test".as_bytes().to_vec().into());
            let result = RedisMessageType::bulk_string("Test");

            assert_eq!(expected, result);
        }

        #[test]
        fn encode() {
            let input = RedisMessageType::BulkString("Test".as_bytes().to_vec().into());
            let expected: VecDeque<u8> = "$4\r\nTest\r\n".as_bytes().to_vec().into();

            let result = input.encode();

            assert_eq!(expected, result);
        }
    }

    #[cfg(test)]
    mod test_integer {
        use super::*;

        #[test]
        fn decode_valid_string_positive_signed() {
            let expected = RedisMessageType::Integer(123);
            let input = ":+123\r\n".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert!(result_rest.is_empty());
        }

        #[test]
        fn decode_valid_string_positive_unsigned() {
            let expected = RedisMessageType::Integer(13);
            let input = ":13\r\nasdfg".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert_eq!(5, result_rest.len());
        }

        #[test]
        fn decode_valid_string_negative() {
            let expected = RedisMessageType::Integer(-23);
            let input = ":-23\r\n".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result_type);
            assert!(result_rest.is_empty());
        }

        #[test]
        fn encode_positive() {
            let input = RedisMessageType::Integer(123);
            let expected: VecDeque<u8> = ":123\r\n".as_bytes().to_vec().into();

            let result = input.encode();

            assert_eq!(expected, result);
        }

        #[test]
        fn encode_negative() {
            let input = RedisMessageType::Integer(-3);
            let expected: VecDeque<u8> = ":-3\r\n".as_bytes().to_vec().into();

            let result = input.encode();

            assert_eq!(expected, result);
        }
    }

    #[cfg(test)]
    mod test_array {
        use super::*;

        #[test]
        fn decode_empty_array() {
            let expected = RedisMessageType::Array(vec![].into());
            let input = "*0\r\n".as_bytes().to_vec().into();

            let result = RedisMessageType::decode(input).unwrap();

            assert_eq!(expected, result.0)
        }

        #[test]
        fn decode_valid_multivalue_string() {
            let expected = RedisMessageType::Array(
                vec![
                    RedisMessageType::Integer(123),
                    RedisMessageType::Integer(-23),
                    RedisMessageType::simple_string("asdf test me here!"),
                    RedisMessageType::bulk_string("Imma test\r\ner here!"),
                ]
                .into(),
            );
            let input =
                "*4\r\n:123\r\n:-23\r\n+asdf test me here!\r\n$19\r\nImma test\r\ner here!\r\n123456".as_bytes().to_vec().into();

            let (result_type, result_rest) = RedisMessageType::decode(input).unwrap();
            assert_eq!(expected, result_type);
            assert_eq!(6, result_rest.len());
        }
    }
}
