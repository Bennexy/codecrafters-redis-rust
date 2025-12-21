use std::collections::HashMap;
use std::slice::SliceIndex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    subsections: Vec<DatabaseSubSection>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseSubSection {
    header: DatabaseSubSectionHeader,
    key_value_data_units: Vec<KeyValueDataUnit>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EndOfFile {}

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use log::{error, info, trace};

use crate::db::data_store::{get_db, DataUnit, Expiry};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KeyValueDataUnit {
    key: String,
    value: String,
    expiry: Option<SystemTime>,
}

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

    pub fn get_database(&self) -> &Database {
        return &self.db;
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
            let (string_length, length_bytes) = parse_length_encoding(&s[index..])
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

        let mut index = 0;

        let mut subsections: Vec<DatabaseSubSection> = Vec::new();
        while s
            .get(index)
            .ok_or(anyhow!("err missing bytes to parse data base section"))?
            == &0xFE
        {
            let data = s.get(index..).ok_or(anyhow!(
                "missing bytes for the database subsection parsing!"
            ))?;
            let (subsection, parsed_length) = DatabaseSubSection::decode(data)?;
            subsections.push(subsection);

            index += parsed_length;
        }

        return Ok((Database { subsections }, index));
    }

    pub fn to_dashmap(&self) -> DashMap<String, DataUnit> {
        let mut map: DashMap<String, DataUnit> = DashMap::with_capacity(
            self.subsections
                .iter()
                .map(|v| v.key_value_data_units.len())
                .sum(),
        );

        self.subsections.iter().for_each(|database_sub_section| {
            database_sub_section
                .key_value_data_units
                .iter()
                .for_each(|key_value_data_unit| {
                    let data_unit = key_value_data_unit.to_data_unit();
                    map.insert(data_unit.key.clone(), data_unit);
                });
        });

        return map;
    }
}

impl DatabaseSubSection {
    pub fn decode<T: AsRef<[u8]>>(input: T) -> Result<(DatabaseSubSection, usize)> {
        let (header, mut bytes_parsed) = DatabaseSubSectionHeader::decode(&input)?;

        let raw = input.as_ref();
        let mut key_value_data_units = Vec::with_capacity(header.hash_table_size);

        for _ in 0..header.hash_table_size {
            let (data_unit, data_unit_bytes_parsed) = KeyValueDataUnit::decode(
                &raw.get(bytes_parsed..)
                    .ok_or(anyhow!("Requires bytes for data Unit parsing!"))?,
            )?;
            key_value_data_units.push(data_unit);

            bytes_parsed += data_unit_bytes_parsed;
        }

        return Ok((
            DatabaseSubSection {
                header,
                key_value_data_units,
            },
            bytes_parsed,
        ));
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatabaseSubSectionHeader {
    index: usize,
    hash_table_size: usize,
    expiry_hash_table_size: usize,
}

impl DatabaseSubSectionHeader {
    pub fn decode<T: AsRef<[u8]>>(input: T) -> Result<(DatabaseSubSectionHeader, usize), Error> {
        let bytes = input.as_ref();

        if *bytes
            .get(0)
            .ok_or(anyhow!("Missing byte 1 for DatabaseSubSectionHeader!"))?
            != 0xFE
        {
            return Err(anyhow!(
                "Malformed DatabaseSubSectionHeader must begin with '0xFE'"
            ));
        }

        let (index, index_parsed_bytes) = parse_length_encoding(&bytes[1..])
            .ok_or(anyhow!("Expected valid value for db subsection index!"))?;

        trace!(
            "header parsing - index: {:?}, parsed_bytes: {:?}",
            &index,
            &index_parsed_bytes
        );

        if *bytes.get(index_parsed_bytes + 1).ok_or(anyhow!("arr"))? != 0xFB as u8 {
            return Err(anyhow!(
                "Expected to have a key to indecate hash table size!"
            ));
        }

        let (hash_table_size, parsed_bytes_hash_table_size) =
            parse_length_encoding(&bytes[index_parsed_bytes + 2..])
                .ok_or(anyhow!("Expected value for hash table size!"))?;

        trace!(
            "header parsing - hash_table_size: {:?}, parsed_bytes_hash_table_size: {:?}",
            &hash_table_size,
            &parsed_bytes_hash_table_size
        );

        let (expiry_hash_table_size, parsed_bytes_expiry_hash_table_size) =
            parse_length_encoding(&bytes[index_parsed_bytes + 2 + parsed_bytes_hash_table_size..])
                .ok_or(anyhow!("Expected value for expiry hash table size!"))?;

        trace!("header parsing - expiry_hash_table_size: {:?}, parsed_bytes_expiry_hash_table_size: {:?}", &expiry_hash_table_size, &parsed_bytes_expiry_hash_table_size);

        let parsed_bytes = index_parsed_bytes
            + 2
            + parsed_bytes_hash_table_size
            + parsed_bytes_expiry_hash_table_size;

        return Ok((
            DatabaseSubSectionHeader {
                index,
                hash_table_size,
                expiry_hash_table_size,
            },
            parsed_bytes,
        ));
    }
}

impl KeyValueDataUnit {
    fn decode<T: AsRef<[u8]>>(input: T) -> Result<(KeyValueDataUnit, usize)> {
        let data = input.as_ref();

        let mut index = 0;
        let (expire_timestamp) = match data.get(index).ok_or(anyhow!("missing data"))? {
            0xFC => {
                let ms = u64::from_le_bytes(
                    // parses millis
                    data.get(1..9).ok_or(anyhow!("err"))?.try_into()?,
                );
                Some((UNIX_EPOCH + Duration::from_millis(ms), 9))
            }
            0xFD => {
                // parses seconds
                let seconds =
                    u32::from_le_bytes(data.get(1..5).ok_or(anyhow!("err"))?.try_into()?) as u64;
                Some((UNIX_EPOCH + Duration::from_secs(seconds), 5))
            }
            _ => None,
        };

        index = match expire_timestamp {
            Some((instant, bytes_parsed)) => bytes_parsed,
            None => index,
        };

        let key_value_data_unit = match data.get(index).unwrap() {
            0x00 => {
                index += 1;
                let (key_data_len, bytes_parsed) =
                    parse_length_encoding(data.get(index..).unwrap()).unwrap();
                index += bytes_parsed;
                let key_string_data_as_bytes =
                    data.get(index..index + key_data_len).ok_or(anyhow!(
                        "Data gave len {} for key but not enough bytes where present in the data!",
                        { key_data_len }
                    ))?;
                let key = str::from_utf8(key_string_data_as_bytes)?;
                index += key_data_len;

                let (value_data_len, bytes_parsed) =
                    parse_length_encoding(data.get(index..).unwrap()).unwrap();

                index += bytes_parsed;
                let value_string_data_as_bytes =
                    data.get(index..index + value_data_len).ok_or(anyhow!(
                    "Data gave len {} for value but not enough bytes where present in the data!",
                    { value_data_len }
                ))?;
                let value = str::from_utf8(value_string_data_as_bytes)?;
                index += value_data_len;

                KeyValueDataUnit {
                    key: key.into(),
                    value: value.into(),
                    expiry: expire_timestamp.map(|(v, size)| v),
                }
            }
            _ => unimplemented!("Only Value type 'string' is implemented!"),
        };

        return Ok((key_value_data_unit, index));
    }

    fn to_data_unit(&self) -> DataUnit {
        return DataUnit::new(
            self.key.clone(),
            self.value.clone(),
            self.expiry.map(|v| Expiry::Deadline(v)),
        );
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum LengthEncoding {
    NormalLength(usize),
    StringEncoding(usize),
}

/// parse length encoding as descibed here: https://rdb.fnordig.de/file_format.html#length-encoding
fn parse_length_encoding(buf: &[u8]) -> Option<(usize, usize)> {
    let b0 = buf.get(0)?;

    let (encoding, bytes_parsed) =
        match b0 >> 6 {
            // The next 6 bits represent the length
            0b00 => Some((LengthEncoding::NormalLength((b0 & 0x3F) as usize), 1)),
            // Read one additional byte. The combined 14 bits represent the length
            0b01 => buf.get(1).map(|&b1| {
                (
                    LengthEncoding::NormalLength(((b0 & 0x3F) as usize) << 8 | b1 as usize),
                    2,
                )
            }),
            // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            0b10 => buf.get(1..=4).map(|bytes| {
                (
                    LengthEncoding::NormalLength(
                        u32::from_be_bytes(bytes.try_into().unwrap()) as usize
                    ),
                    5,
                )
            }),
            // The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // May be used to store numbers or Strings, see https://rdb.fnordig.de/file_format.html#string-encoding
            0b11 => match b0 & 0b11 {
                0b00 => Some((LengthEncoding::StringEncoding(1), 1)),
                0b01 => Some((LengthEncoding::StringEncoding(2), 1)),
                0b10 => Some((LengthEncoding::StringEncoding(4), 1)),
                0b11 => unimplemented!("LZF compressed string - not implemented"),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }?;

    return match encoding {
        LengthEncoding::NormalLength(val) => return Some((val, bytes_parsed)),
        LengthEncoding::StringEncoding(len) => {
            let slice = buf.get(1..1 + len)?;

            let value = match len {
                1 => slice[0] as usize,
                2 => u16::from_le_bytes(slice.try_into().ok()?) as usize,
                4 => u32::from_le_bytes(slice.try_into().ok()?) as usize,
                _ => return None,
            };

            return Some((value, 1 + len));
        }
    };
}

#[cfg(test)]
mod test {

    #[cfg(test)]
    mod test_rdb_file {

        use crate::db::db_file::RdbFile;

        #[test]
        fn test_load_full_rdb_file() {
            let input = vec![
                // header bytes
                0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, // metadata bytes
                0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x06, 0x36, 0x2E,
                0x30, 0x2E, 0x31, 0x36, // database bytes
                0xFE, 0x00, 0xFB, 0x02, 0x01, // key 1
                0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // value type 1
                0x06, // value 1
                0x62, 0x61, 0x7A, 0x71, 0x75, 0x78, // expiry timestamp in seconds
                0xFD, 0x52, 0xED, 0x2A, 0x66, // value type 2
                0x00, // key 2
                0x03, 0x62, 0x61, 0x7A, // value 2
                0x03, 0x71, 0x75, 0x78, 0x00,
            ];

            let result = RdbFile::decode(input).unwrap();
        }
    }

    #[cfg(test)]
    mod test_parse_length {
        use crate::db::db_file::{parse_length_encoding, LengthEncoding};

        #[test]
        fn test_parse_length_encoding_0b00() {
            let (length, bytes_parsed) = parse_length_encoding(vec![0x0F].as_slice()).unwrap();

            assert_eq!(1, bytes_parsed);
            assert_eq!(15, length);
        }

        #[test]
        fn test_parse_length_encoding_0b01() {
            let (length, bytes_parsed) =
                parse_length_encoding(vec![0x42, 0xBC].as_slice()).unwrap();

            assert_eq!(2, bytes_parsed);
            assert_eq!(700, length);
        }

        #[test]
        fn test_parse_length_encoding_0b10() {
            let (length, bytes_parsed) =
                parse_length_encoding(vec![0x80, 0x00, 0x00, 0x42, 0x68].as_slice()).unwrap();

            assert_eq!(5, bytes_parsed);
            assert_eq!(17000, length);
        }

        #[test]
        fn test_parse_string_length_encoding_0xC0() {
            let (length, bytes_parsed) =
                parse_length_encoding(vec![0xC0, 0x7B].as_slice()).unwrap();

            assert_eq!(2, bytes_parsed);
            assert_eq!(123, length);
        }

        #[test]
        fn test_parse_string_length_encoding_0xC1() {
            let (length, bytes_parsed) =
                parse_length_encoding(vec![0xC1, 0x39, 0x30].as_slice()).unwrap();

            assert_eq!(3, bytes_parsed);
            assert_eq!(12345, length);
        }

        #[test]
        fn test_parse_string_length_encoding_0xC2() {
            let (length, bytes_parsed) =
                parse_length_encoding(vec![0xC2, 0x87, 0xD6, 0x12, 00].as_slice()).unwrap();

            assert_eq!(5, bytes_parsed);
            assert_eq!(1234567, length);
        }

        #[test]
        #[should_panic]
        fn test_parse_string_length_encoding_0xC3() {
            let result = parse_length_encoding(vec![0xC3].as_slice());
        }
    }

    #[cfg(test)]
    mod test_header {
        use crate::db::db_file::Header;

        #[test]
        fn test_decode_header() {
            let header = vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31];

            let header = Header::decode(header).unwrap();

            assert_eq!([0x52, 0x45, 0x44, 0x49, 0x53], header.magic_string);
            assert_eq!([0x30, 0x30, 0x31, 0x31], header.version)
        }
    }

    #[cfg(test)]
    mod test_metadata {

        use std::collections::HashMap;

        use crate::db::db_file::Metadata;

        #[test]
        fn test_metadata_decode() {
            let data = vec![
                0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x06, 0x36, 0x2E,
                0x30, 0x2E, 0x31, 0x36, 0xFE, 0xDE, 0xAD, 0xBE, 0xEF, 0x00,
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

    #[cfg(test)]
    mod test_database {
        use crate::db::db_file::Database;

        #[test]
        fn test_parse_database_no_key_value_data_but_two_subsections() {
            // padding needed at the end of thisvec
            let hex_value: Vec<u8> = vec![
                0xFE, 0x00, 0xFB, 0x00, 0x00, 0xFE, 0x01, 0xFB, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
            ];

            let (database, parsed_bytes) = Database::decode(hex_value).unwrap();

            assert_eq!(10, parsed_bytes);

            assert_eq!(2, database.subsections.len());
            assert_eq!(0, database.subsections[0].header.index);
            assert_eq!(0, database.subsections[0].header.hash_table_size);
            assert_eq!(0, database.subsections[0].header.expiry_hash_table_size);
            assert_eq!(1, database.subsections[1].header.index);
            assert_eq!(0, database.subsections[1].header.hash_table_size);
            assert_eq!(0, database.subsections[1].header.expiry_hash_table_size);
        }
    }

    #[cfg(test)]
    mod test_data_subsection {
        use std::time::{Duration, UNIX_EPOCH};

        use crate::db::db_file::DatabaseSubSection;

        #[test]
        fn db_sub_section_parsing_full_sub_section() {
            let target_time = UNIX_EPOCH + Duration::from_secs(1714089298); // value from bytes 1 to 5 in le
            let input: Vec<u8> = vec![
                0xFE, 0x00, 0xFB, 0x02, 0x01, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06,
                0x62, 0x61, 0x7A, 0x71, 0x75, 0x78, 0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62,
                0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
            ];

            let (subsection, parsed_bytes) = DatabaseSubSection::decode(input).unwrap();

            assert_eq!(34, parsed_bytes);

            assert_eq!(0, subsection.header.index);
            assert_eq!(2, subsection.header.hash_table_size);
            assert_eq!(1, subsection.header.expiry_hash_table_size);
            assert_eq!(2, subsection.key_value_data_units.len());

            assert_eq!(
                "foobar",
                subsection.key_value_data_units.get(0).unwrap().key
            );
            assert_eq!(
                "bazqux",
                subsection.key_value_data_units.get(0).unwrap().value
            );
            assert!(subsection
                .key_value_data_units
                .get(0)
                .unwrap()
                .expiry
                .is_none());

            assert_eq!("baz", subsection.key_value_data_units.get(1).unwrap().key);
            assert_eq!("qux", subsection.key_value_data_units.get(1).unwrap().value);
            assert!(subsection
                .key_value_data_units
                .get(1)
                .unwrap()
                .expiry
                .is_some());
            assert_eq!(
                target_time,
                subsection
                    .key_value_data_units
                    .get(1)
                    .unwrap()
                    .expiry
                    .unwrap()
            );
        }

        #[test]
        fn db_sub_section_parsing_no_key_value_data() {
            let input: Vec<u8> = vec![0xFE, 0x01, 0xFB, 0x00, 0x00];

            let (subsection, parsed_bytes) = DatabaseSubSection::decode(input).unwrap();

            assert_eq!(5, parsed_bytes);

            assert_eq!(1, subsection.header.index);
            assert_eq!(0, subsection.header.hash_table_size);
            assert_eq!(0, subsection.header.expiry_hash_table_size);
            assert_eq!(0, subsection.key_value_data_units.len());
        }
    }

    #[cfg(test)]
    mod test_data_subsection_header {
        use crate::db::db_file::DatabaseSubSectionHeader;

        #[test]
        fn db_header_parsing_header_1() {
            let hex_value: Vec<u8> = vec![0xFE, 0x00, 0xFB, 0x03, 0x02];

            let (header, bytes_parsed) = DatabaseSubSectionHeader::decode(hex_value).unwrap();

            assert_eq!(5, bytes_parsed);
            assert_eq!(0, header.index);
            assert_eq!(3, header.hash_table_size);
            assert_eq!(2, header.expiry_hash_table_size);
        }

        #[test]
        fn db_header_parsing_header_2() {
            let hex_value: Vec<u8> = vec![0xFE, 0x0F, 0xFB, 0x80, 0x72, 0xE7, 0x07, 0x8F, 0x02];

            let (header, bytes_parsed) = DatabaseSubSectionHeader::decode(hex_value).unwrap();

            assert_eq!(9, bytes_parsed);
            assert_eq!(15, header.index);
            assert_eq!(0x72E7078F, header.hash_table_size);
            assert_eq!(2, header.expiry_hash_table_size);
        }
    }

    #[cfg(test)]
    mod test_key_value_data_unit {
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

        use crate::db::db_file::KeyValueDataUnit;

        #[test]
        fn test_decode_no_expiry() {
            let input: Vec<u8> = vec![
                0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x06, 0x62, 0x61, 0x7A, 0x71, 0x75,
                0x78,
            ];

            let (key_value_data, bytes_parsed) = KeyValueDataUnit::decode(input).unwrap();

            assert_eq!(15, bytes_parsed);
            assert_eq!("foobar", key_value_data.key);
            assert_eq!("bazqux", key_value_data.value);
            assert!(key_value_data.expiry.is_none());
        }

        #[test]
        fn test_decode_expiry_milliseconds() {
            let target_time = UNIX_EPOCH + Duration::from_millis(1713824559637); // value from bytes 1 to 9 in le
            let input: Vec<u8> = vec![
                0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x66, 0x6F, 0x6F,
                0x03, 0x62, 0x61, 0x72,
            ];

            let (key_value_data, bytes_parsed) = KeyValueDataUnit::decode(input).unwrap();

            assert_eq!(18, bytes_parsed);
            assert_eq!("foo", key_value_data.key);
            assert_eq!("bar", key_value_data.value);
            assert!(key_value_data.expiry.is_some());
            assert_eq!(target_time, key_value_data.expiry.unwrap());
        }

        #[test]
        fn test_decode_expiry_seconds() {
            let target_time = UNIX_EPOCH + Duration::from_secs(1714089298); // value from bytes 1 to 5 in le
            let input: Vec<u8> = vec![
                0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
            ];

            let (key_value_data, bytes_parsed) = KeyValueDataUnit::decode(input).unwrap();

            assert_eq!(14, bytes_parsed);
            assert_eq!("baz", key_value_data.key);
            assert_eq!("qux", key_value_data.value);
            assert!(key_value_data.expiry.is_some());
            assert_eq!(target_time, key_value_data.expiry.unwrap());
        }
    }
}
