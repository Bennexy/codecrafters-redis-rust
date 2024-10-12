use std::fmt::Display;

pub type RedisParseResult = Result<(RedisType, Vec<u8>), RedisParseError>;

const CR: u8 = b'\r';
const LF: u8 = b'\n';

#[derive(Debug)]
pub enum RedisParseError {
    NotEnoughBytes,
    InvalidFormat,
    InvalidStartingByte,
    NoStartingByte,
    Other(Box<dyn std::error::Error>),
}

impl RedisParseError {
    pub fn as_redis_type(&self) -> RedisType {
        RedisType::SimpleError(format!("An error occured: {:?}", self).as_bytes().to_vec())
    }

    pub fn as_redis_type_with_additional_information(&self, information: &str) -> RedisType {
        RedisType::SimpleError(
            format!(
                "An error occured: {:?}\nAdditional Context: {}",
                self, information
            )
            .as_bytes()
            .to_vec(),
        )
    }
}

impl From<std::num::ParseIntError> for RedisParseError {
    fn from(from: std::num::ParseIntError) -> Self {
        Self::Other(Box::new(from))
    }
}

impl From<std::str::Utf8Error> for RedisParseError {
    fn from(from: std::str::Utf8Error) -> Self {
        Self::Other(Box::new(from))
    }
}

impl From<std::io::Error> for RedisParseError {
    fn from(from: std::io::Error) -> Self {
        Self::Other(Box::new(from))
    }
}

// improvement -> implement a type equal for RedisParseError and make this implementation a full eq
impl PartialEq for RedisParseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RedisParseError::NotEnoughBytes, RedisParseError::NotEnoughBytes) => true,
            (RedisParseError::InvalidFormat, RedisParseError::InvalidFormat) => true,
            (RedisParseError::InvalidStartingByte, RedisParseError::InvalidStartingByte) => true,
            (RedisParseError::Other(x), RedisParseError::Other(y)) => {
                let parse_out_error_name = |input: &Box<dyn std::error::Error>| {
                    let debug_message = format!("{:?}", input);

                    let end_index = match debug_message.find(" {") {
                        Some(index) => index,         // Stop at " {"
                        None => return debug_message, // Return None if " {" is not found
                    };
                    String::from(&debug_message[..end_index])
                };
                let x_name = parse_out_error_name(x);
                let y_name = parse_out_error_name(y);
                x_name == y_name
            }
            _ => false,
        }
    }
    fn ne(&self, other: &Self) -> bool {
        match (self, other) {
            (RedisParseError::NotEnoughBytes, RedisParseError::NotEnoughBytes) => false,
            (RedisParseError::InvalidFormat, RedisParseError::InvalidFormat) => false,
            (RedisParseError::InvalidStartingByte, RedisParseError::InvalidStartingByte) => false,
            (RedisParseError::Other(x), RedisParseError::Other(y)) => {
                let parse_out_error_name = |input: &Box<dyn std::error::Error>| {
                    let debug_message = format!("{:?}", input);

                    let end_index = match debug_message.find(" {") {
                        Some(index) => index,         // Stop at " {"
                        None => return debug_message, // Return None if " {" is not found
                    };
                    String::from(&debug_message[..end_index])
                };
                let x_name = parse_out_error_name(x);
                let y_name = parse_out_error_name(y);

                x_name != y_name
            }
            _ => false,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisType {
    Null,
    NullBulkString,
    Integer(Vec<u8>),
    Boolean(Vec<u8>),
    RDBFile(Vec<u8>),
    BigNumber(Vec<u8>),
    BulkString(Vec<u8>),
    SimpleError(Vec<u8>),
    SimpleString(Vec<u8>),
    Push(Vec<RedisType>),
    Array(Vec<RedisType>),
    // Double,
    // VerbatimString,
    // Set(HashSet<RedisTypeVec<u8>)
    // Map,
}

impl Display for RedisType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array(_data) => f.write_str("RedisType::Array"),
            Self::BigNumber(_data) => f.write_str("RedisType::BigNumber"),
            Self::Boolean(_data) => f.write_str("RedisType::Boolean"),
            Self::BulkString(_data) => f.write_str("RedisType::BulkString"),
            Self::NullBulkString => f.write_str("RedisType::NullBulkString"),
            Self::Null => f.write_str("RedisType::Null"),
            Self::SimpleError(_data) => f.write_str("RedisType::SimpleError"),
            Self::SimpleString(_data) => f.write_str("RedisType::SimpleString"),
            Self::RDBFile(_data) => f.write_str("RedisType::RDBFile"),
            Self::Integer(_data) => f.write_str("RedisType::Integer"),
            Self::Push(_data) => f.write_str("RedisType::Push"),
        }
    }
}

impl RedisType {
    pub fn array_bulk_string_from_vec_str(data: Vec<&str>) -> RedisParseResult {
        let mut raw_bytes = Vec::new();
        raw_bytes.extend_from_slice(format!("*{}\r\n", data.len()).as_bytes());

        for element in data {
            raw_bytes.extend_from_slice(format!("${}\r\n{}\r\n", element.len(), element).as_bytes())
        }

        return Self::from_vec_u8(raw_bytes);
    }

    pub fn bulk_string_from_vec_str(data: Vec<&str>) -> RedisParseResult {
        let mut concated = data.join("\n");
        concated.push('\n');

        let raw = format!("${}\r\n{}\r\n", concated.len(), concated)
            .as_bytes()
            .to_vec();

        return Self::from_vec_u8(raw);
    }

    pub fn from_vec_u8(data: Vec<u8>) -> RedisParseResult {
        return Self::deserilize(data);
    }

    pub fn from_str(data: &str) -> RedisParseResult {
        return Self::from_vec_u8(data.as_bytes().to_vec());
    }

    pub fn deserilize(data: Vec<u8>) -> RedisParseResult {
        match data.get(0) {
            Some(index) => match index {
                b'+' => parse_simple_string(data),
                b'-' => parse_simple_error(data),
                b'$' => parse_bulk_string(data),
                b'*' => parse_array(data),
                b':' => parse_integer(data),
                b'_' => parse_null(data),
                b'#' => parse_boolean(data),
                b'(' => parse_big_number(data),
                b'~' => todo!("impl set "),
                b'>' => parse_push(data),
                _ => Err(RedisParseError::InvalidStartingByte),
            },
            None => Err(RedisParseError::NoStartingByte),
        }
    }

    pub fn serilize<W>(&self, writer: &mut W) -> Result<(), RedisParseError>
    where
        W: std::io::Write,
    {
        match self {
            Self::NullBulkString => {
                writer.write_all("$-1\r\n".as_bytes())?;
            }
            Self::Null => {
                writer.write_all(&[b'_', CR, LF])?;
            }
            Self::SimpleString(data) => {
                writer.write_all(&[b'+'])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::SimpleError(data) => {
                writer.write_all(&[b'-'])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::Integer(data) => {
                writer.write_all(&[b'+'])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::BulkString(data) => {
                writer.write_all(&[b'$'])?;
                writer.write_all(format!("{}", data.len()).as_bytes())?;
                writer.write_all(&[CR, LF])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::Array(data) => {
                writer.write_all(&[b'*'])?;
                writer.write_all(format!("{}", data.len()).as_bytes())?;
                writer.write_all(&[CR, LF])?;
                for element in data {
                    element.serilize(writer)?;
                }
            }
            Self::Boolean(data) => {
                writer.write_all(&[b'#'])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::BigNumber(data) => {
                writer.write_all(&[b'('])?;
                writer.write_all(data)?;
                writer.write_all(&[CR, LF])?;
            }
            Self::Push(data) => {
                writer.write_all(&[b'>'])?;
                writer.write_all(format!("{}", data.len()).as_bytes())?;
                writer.write_all(&[CR, LF])?;
                for element in data {
                    element.serilize(writer)?;
                }
            }
            Self::RDBFile(data) => {
                writer.write_all(&[b'$'])?;
                writer.write_all(format!("{}", data.len()).as_bytes())?;
                writer.write_all(&[CR, LF])?;
                writer.write_all(data)?;
            }
        };
        return Ok(());
    }

    pub fn simple_error_from_string(data: String) -> Self {
        let raw = data.clone().as_bytes().to_vec();
        return Self::SimpleError(raw);
    }
}

// need tests
fn parse_integer(data: Vec<u8>) -> RedisParseResult {
    let parse_res = parse_untill_cr_lf(&data[1..])?;
    let _ = std::str::from_utf8(parse_res.0.as_slice())?.parse::<isize>()?; // check if parsing works
    return Ok((RedisType::Integer(parse_res.0), parse_res.1));
}

fn parse_simple_string(data: Vec<u8>) -> RedisParseResult {
    parse_untill_cr_lf(&data[1..]).map(|(x, y)| (RedisType::SimpleString(x), y))
}

fn parse_big_number(data: Vec<u8>) -> RedisParseResult {
    parse_untill_cr_lf(&data[1..]).map(|(x, y)| (RedisType::BigNumber(x), y))
}

fn parse_null(data: Vec<u8>) -> RedisParseResult {
    let (x, y) = parse_untill_cr_lf(&data[1..])?;
    if x == [] && y == [] {
        return Ok((RedisType::Null, y));
    } else {
        return Err(RedisParseError::InvalidFormat);
    }
}

fn parse_boolean(data: Vec<u8>) -> RedisParseResult {
    let (x, y) = parse_untill_cr_lf(&data[1..])?;
    if x == &[b't'] || x == &[b'f'] {
        return Ok((RedisType::Boolean(x), y));
    } else {
        return Err(RedisParseError::InvalidFormat);
    }
}

fn parse_simple_error(data: Vec<u8>) -> RedisParseResult {
    parse_untill_cr_lf(&data[1..]).map(|(x, y)| (RedisType::SimpleError(x), y))
}

fn parse_bulk_string(data: Vec<u8>) -> RedisParseResult {
    let res = parse_untill_cr_lf(&data[1..])?;
    let res_str = std::str::from_utf8(&res.0)?;
    let index = match res_str.parse::<usize>() {
        Ok(val) => val,
        Err(_) => match res_str.parse::<i8>() {
            Ok(val) => {
                if val == -1 {
                    return Ok((RedisType::NullBulkString, res.1));
                } else {
                    return Err(RedisParseError::InvalidFormat);
                }
            }
            Err(err) => return Err(err.into()),
        },
    };
    parse_until_index(&res.1, index).map(|(x, y)| (RedisType::BulkString(x), y))
}

fn parse_array(data: Vec<u8>) -> RedisParseResult {
    let array_length_parse = parse_untill_cr_lf(&data[1..])?;
    let array_length = std::str::from_utf8(array_length_parse.0.as_slice())?.parse::<usize>()?;
    if array_length >= data.len() {
        return Err(RedisParseError::InvalidFormat);
    };

    let mut array = Vec::with_capacity(array_length);
    let mut current_slice = array_length_parse.1;

    for index in 0..array_length {
        let (element, next_slice) = RedisType::deserilize(current_slice)?;
        current_slice = next_slice;
        array.push(element);
        if current_slice == [] && index + 1 != array_length {
            return Err(RedisParseError::InvalidFormat);
        }
    }

    return Ok((RedisType::Array(array), current_slice));
}

fn parse_push(data: Vec<u8>) -> RedisParseResult {
    let array_length_parse = parse_untill_cr_lf(&data[1..])?;
    let array_length = std::str::from_utf8(array_length_parse.0.as_slice())?.parse::<usize>()?;
    if array_length >= data.len() {
        return Err(RedisParseError::InvalidFormat);
    };

    let mut array = Vec::with_capacity(array_length);
    let mut current_slice = array_length_parse.1;

    for index in 0..array_length {
        let (element, next_slice) = RedisType::deserilize(current_slice)?;
        current_slice = next_slice;
        array.push(element);
        if current_slice == [] && index + 1 != array_length {
            return Err(RedisParseError::InvalidFormat);
        }
    }

    return Ok((RedisType::Push(array), current_slice));
}

fn _parse_set(data: Vec<u8>) -> RedisParseResult {
    let array_length_parse = parse_untill_cr_lf(&data[1..])?;
    let array_length = std::str::from_utf8(array_length_parse.0.as_slice())?.parse::<usize>()?;
    if array_length >= data.len() {
        return Err(RedisParseError::InvalidFormat);
    };

    let mut array = Vec::with_capacity(array_length);
    let mut current_slice = array_length_parse.1;

    for index in 0..array_length {
        let (element, next_slice) = RedisType::deserilize(current_slice)?;
        current_slice = next_slice;
        array.push(element);
        if current_slice == [] && index + 1 != array_length {
            return Err(RedisParseError::InvalidFormat);
        }
    }

    return Ok((RedisType::Array(array), current_slice));
}

fn parse_untill_cr_lf(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), RedisParseError> {
    for (index, pattern) in data.iter().zip(data.iter().skip(1)).enumerate() {
        if pattern == (&CR, &LF) {
            return Ok((data[0..index].to_vec(), data[index + 2..].to_vec()));
        }
    }
    return Err(RedisParseError::NotEnoughBytes);
}

fn parse_until_index(data: &[u8], index: usize) -> Result<(Vec<u8>, Vec<u8>), RedisParseError> {
    if data.len() < index {
        return Err(RedisParseError::NotEnoughBytes);
    } else if data.len() == index {
        return Err(RedisParseError::NotEnoughBytes);
    } else if (data[index], data[index + 1]) == (CR, LF) {
        return Ok((data[..index].to_vec(), data[index + 2..].to_vec()));
    } else {
        return Err(RedisParseError::InvalidFormat);
    }
}

pub fn decode_hex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| hex_val(pair[0]) << 4 | hex_val(pair[1]))
        .collect()
}

const fn hex_val(c: u8) -> u8 {
    match c {
        b'A'..=b'F' => c - b'A' + 10,
        b'a'..=b'f' => c - b'a' + 10,
        b'0'..=b'9' => c - b'0',
        _ => panic!("shiit"),
    }
}

#[cfg(test)]
pub mod test_parseing_bulk_string {
    use super::{parse_bulk_string, RedisParseError, RedisParseResult, RedisType};

    #[test]
    fn test_parse_bulk_string_single_element() {
        let raw = "$3\r\nfoo\r\n".as_bytes();

        let result = parse_bulk_string(raw.to_vec());
        let expected: RedisParseResult = Ok((
            RedisType::BulkString("foo".as_bytes().to_vec()),
            "".as_bytes().to_vec(),
        ));

        assert_eq!(result, expected);
    }
    #[test]
    fn test_parse_bulk_string_multiple_elements() {
        let raw = "$3\r\nfoo\r\n".as_bytes();

        let result = parse_bulk_string(raw.to_vec());
        let expected: RedisParseResult = Ok((
            RedisType::BulkString("foo".as_bytes().to_vec()),
            "".as_bytes().to_vec(),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_bulk_string_not_enough_bytes() {
        let raw = "$3\r\nfoo".as_bytes();

        let result = parse_bulk_string(raw.to_vec());
        let expected: RedisParseResult = Err(RedisParseError::NotEnoughBytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_bulk_string_invalid_length() {
        let raw = "$30\r\nfoo\r\n".as_bytes();

        let result = parse_bulk_string(raw.to_vec());
        let expected: RedisParseResult = Err(RedisParseError::NotEnoughBytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_bulk_string_invalid_length_parse() {
        let raw = "$#0\r\nfoo\r\n".as_bytes().to_vec();

        let result = parse_bulk_string(raw);
        let expected: RedisParseResult = Err("d".parse::<usize>().err().unwrap().into()); // mock parse int error

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        let raw = "$0\r\n\r\n".as_bytes().to_vec();

        let result = parse_bulk_string(raw);
        let expected: RedisParseResult = Ok((
            RedisType::BulkString("".as_bytes().to_vec()),
            "".as_bytes().to_vec(),
        ));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_bulk_string_missing_second_part() {
        let raw = "$0\r\n".as_bytes().to_vec();

        let result = parse_bulk_string(raw);
        let expected: RedisParseResult = Err(RedisParseError::NotEnoughBytes);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let raw = "$-1\r\n".as_bytes().to_vec();

        let result = parse_bulk_string(raw);
        let expected: RedisParseResult = Ok((RedisType::NullBulkString, "".as_bytes().to_vec()));
        assert_eq!(result, expected);
    }
}

#[cfg(test)]
pub mod test_parse_arrays {
    use super::{parse_array, RedisParseError, RedisParseResult, RedisType};

    #[test]
    fn test_parse_simple_string_array() {
        let raw = "*2\r\n+Hi There\r\n+World!\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Ok((
            RedisType::Array(vec![
                RedisType::SimpleString("Hi There".as_bytes().to_vec()),
                RedisType::SimpleString("World!".as_bytes().to_vec()),
            ]),
            "".as_bytes().to_vec(),
        ));

        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_simple_string_array_not_enough_bytes() {
        let raw = "*2\r\n+Hi There\r\n+World!".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::NotEnoughBytes);

        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_simple_string_array_not_enough_bytes_2() {
        let raw = "*2\r\n+Hi There+World!\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::InvalidFormat);

        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_invalid_starting_byte_array() {
        let raw = "*1\r\n1".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::InvalidStartingByte);
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_malformed_data_array() {
        let raw = "*1\r\n$-5\r\nhi\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::InvalidFormat);
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_malformed_data_2_array() {
        let raw = "*1\r\n$50\r\nhi\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::NotEnoughBytes);
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_empty_array() {
        let raw = "*0\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Ok((RedisType::Array(vec![]), "".as_bytes().to_vec()));
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_malformed_array_data_array() {
        let raw = "*5\r\n".as_bytes().to_vec();

        let result = parse_array(raw);
        let expected: RedisParseResult = Err(RedisParseError::InvalidFormat);
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_bulk_string_array() {
        let raw = "*4\r\n$4\r\necho\r\n$2\r\nhi\r\n$5\r\nthere\r\n$6\r\nworld!\r\n"
            .as_bytes()
            .to_vec();

        let result: RedisType = parse_array(raw).unwrap().0;
        let expected: RedisType = RedisType::Array(vec![
            RedisType::BulkString("echo".as_bytes().to_vec()),
            RedisType::BulkString("hi".as_bytes().to_vec()),
            RedisType::BulkString("there".as_bytes().to_vec()),
            RedisType::BulkString("world!".as_bytes().to_vec()),
        ]);
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_bulk_string_array_2() {
        let raw = vec![
            42, 52, 13, 10, 36, 52, 13, 10, 101, 99, 104, 111, 13, 10, 36, 50, 13, 10, 104, 105,
            13, 10, 36, 53, 13, 10, 116, 104, 101, 114, 101, 13, 10, 36, 54, 13, 10, 119, 111, 114,
            108, 100, 33, 13, 10,
        ];

        let result: RedisType = RedisType::deserilize(raw).unwrap().0;
        let expected: RedisType = RedisType::Array(vec![
            RedisType::BulkString("echo".as_bytes().to_vec()),
            RedisType::BulkString("hi".as_bytes().to_vec()),
            RedisType::BulkString("there".as_bytes().to_vec()),
            RedisType::BulkString("world!".as_bytes().to_vec()),
        ]);
        assert_eq!(expected, result);
    }
}
