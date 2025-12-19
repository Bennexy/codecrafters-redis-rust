use std::collections::HashMap;

use anyhow::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RdbFile {
    header: Header,
    metadata: Metadata,
    db: Database,
    eof: EndOfFile,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Header {
    magic_string: [u8; 5],
    version: [u8; 4],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Metadata {
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Database {
    database_selector: u8,
    hash_table_size: u32,
    expiry_hash_table_size: u32,
    subsections: Vec<DatabaseSubSection>
}


#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseSubSection {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EndOfFile {}

use anyhow::{anyhow, Result};

impl RdbFile {
    pub fn decode(input: Vec<u8>) -> Result<RdbFile> {
        let s = input.as_slice();

        let (raw_header, s) = s.split_at(9);

        let header = Header::decode(raw_header)?;

        let (metadata, metdata_size) = Metadata::decode(s)?;

        let (raw_metadata, s) = s.split_at(metdata_size);

        let db = Database::decode(s).unwrap().0;
        let eof = EndOfFile {};

        return Ok(RdbFile {
            header,
            metadata,
            db,
            eof,
        });
    }
}

impl Header {
    pub fn decode<T: AsRef<[u8]>>(input: T) -> Result<Header> {
        let s = input.as_ref();

        if s.len() != 9 {
            return Err(anyhow!("Header decode input must to of length 9!"));
        }

        // playing around with unsafe. Its safe since the length of the input is validated.
        let (magic_string, version) = unsafe {
            let ptr = s.as_ptr();
            let magic_string: [u8; 5] = *(ptr as *const [u8; 5]);
            let version: [u8; 4] = *(ptr.add(5) as *const [u8; 4]);
            (magic_string, version)
        };

        if magic_string != "REDIS".as_bytes() {
            return Err(anyhow!("Magic string is incorrect! Must be 'REDIS'"));
        }

        return Ok(Header {
            magic_string,
            version,
        });
    }
}

impl Metadata {
    pub fn decode<T: AsRef<[u8]>>(input: T) -> Result<(Metadata, usize)> {
        let s = input.as_ref();

        if s[0] != 0xFA {
            return Err(anyhow!("Metadata section must begin with 0xFA"));
        }

        let mut metadata_vec = Vec::new();

        let mut index = 1;

        while s[index] != 0xFE {
            let (string_length, length_bytes) = parse_string_length(&s[index..])
                .ok_or_else(|| anyhow!("Unable to parse string length in metadata section!"))?;

            let start = index + length_bytes;
            let end = start + string_length;

            let string_bytes = s
                .get(start..end)
                .ok_or_else(|| anyhow!("unable to parse the string length"))?;

            metadata_vec.push(string_bytes);

            index = end;
        }

        let metadata: HashMap<Vec<u8>, Vec<u8>> = metadata_vec
            .chunks(2)
            .filter_map(|chunk| {
                if let [k, v] = chunk {
                    Some((k.to_vec(), v.to_vec()))
                } else {
                    panic!("Parsing db file metadata has a single header. Should never happen ")
                }
            })
            .collect();

        return Ok((Metadata { metadata }, index));
    }
}

impl Database {
    pub fn decode<T: AsRef<[u8]>>(input: T) -> Result<(Database, usize)> {
        let s = input.as_ref();

        let mut subsections: Vec<DatabaseSubSection> = Vec::new();

        
        let database_selector = 0;
        let hash_table_size = 0;
        let expiry_hash_table_size = 0;


        return Ok((Database { subsections, database_selector, hash_table_size, expiry_hash_table_size }, 0))
    }
}

/// parse length encoding as descibed here: https://rdb.fnordig.de/file_format.html#length-encoding
fn parse_string_length(buf: &[u8]) -> Option<(usize, usize)> {
    let b0 = buf.get(0)?;

    match b0 >> 6 {
        0b00 => Some(((b0 & 0x3F) as usize, 1)),
        0b01 => buf
            .get(1)
            .map(|&b1| ((((b0 & 0x3F) as usize) << 8) | b1 as usize, 2)),
        0b10 => buf
            .get(1..=4)
            .map(|bytes| (u32::from_be_bytes(bytes.try_into().unwrap()) as usize, 5)),
        0b11 => None,
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(test)]
    mod test_metadata {

        use std::collections::HashMap;

        use super::Metadata;

        #[test]
        fn test_parse_success() {
            let data = vec![
                0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x06, 0x36, 0x2E,
                0x30, 0x2E, 0x31, 0x36, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF,
            ];

            let mut map = HashMap::new();
            map.insert(
                vec![0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72],
                vec![0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36],
            );
            let expected = Metadata { metadata: map };

            let (metadata, metadata_length) = Metadata::decode(data).unwrap();

            assert_eq!(18, metadata_length);
            assert_eq!(expected, metadata);
        }

        #[test]
        fn test_parse_fail_invalid_start_byte() {
            let data = vec![0xFF];

            let result = Metadata::decode(data);

            assert!(
                result
                    .as_ref()
                    .is_err_and(|e| e.to_string() == "Metadata section must begin with 0xFA"),
                "Expected error about metadata section starting with 0xFA, but got: {:?}",
                result
            );
        }

    }
}
