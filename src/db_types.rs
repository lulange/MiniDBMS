// types that enforce certain constraints through constructor functions and permissions

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::os::windows::fs::FileExt; // TODO replace code that depends on this with Seek trait stuff
use std::error::Error;

use crate::binary_search_tree::BST;
use crate::DBError;

pub struct Table {
    attributes: Vec<(Identifier, Domain)>,
    record_count: u32,
    meta_offset: u32,
    pub bst: Option<BST>,
    record_length: u32,
    file_path: String,
}

impl Table {
    pub fn build(name: &str, attributes: Vec<(Identifier, Domain)>, primary_key: bool, dir: &str) -> Result<Self, Box<dyn Error>> {
        let name = Identifier::from(name)?; // reject name if not an identifier
        let mut record_length: u32 = 0;
        let meta_offset = attributes.len() as u32 * 20 + 8; //  4 bytes at the beginning + 19 bytes per + 1 byte per + 4 bytes at the end
        for (_, domain) in attributes.iter() {
            record_length += Domain::size_in_bytes(domain);
        }

        let file_path = format!("{dir}/{}.dat", name.name());
        let mut file = File::create_new(&file_path)?;

        // attribute list size
        file.write_all(&(attributes.len() as u32).to_be_bytes())?; // 4 bytes

        for (attribute, domain) in attributes.iter() {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
        }

        // table_size
        file.write_all(&0_u32.to_be_bytes())
            .expect("Should be able to write to file."); // u32 default

        Ok(Table {
            attributes,
            record_count: 0,
            meta_offset,
            bst: if primary_key { Some(BST::new()) } else { None },
            record_length,
            file_path,
        })
    }

    pub fn attributes(&self) -> &Vec<(Identifier, Domain)> {
        &self.attributes
    }

    pub fn read_from_file(name: &str, dir: &str) -> Result<Self, Box<dyn Error>> {
        let name = Identifier::from(name)?.name; // does copy the string but more importantly fails if name is not an identifier
        let file_path = format!("{dir}/{name}.dat");
        let bst_file_path = format!("{dir}/{name}.index");
        let bst = match BST::read_from_file(&bst_file_path) {
            Ok(bst) => Some(bst),
            Err(_) => None // fails if the file is not there or in a bad format
        };

        let mut file = File::open(&file_path)?;
        let mut attribute_list_len: [u8; 4] = [0; 4];
        file.read_exact(&mut attribute_list_len)?;
        let attribute_list_len = u32::from_be_bytes(attribute_list_len);
        let meta_offset = attribute_list_len * 20 + 8;
        let mut attributes_bytes: Vec<u8> = vec![0; attribute_list_len as usize * 20];
        file.read_exact(&mut attributes_bytes)?;
        let mut record_count: [u8; 4] = [0; 4];
        file.read_exact(&mut record_count)?;
        let record_count = u32::from_be_bytes(record_count);
        let mut attributes: Vec<(Identifier, Domain)> = Vec::with_capacity(attribute_list_len as usize);

        let mut record_length: u32 = 0;
        for _ in 0..attribute_list_len {
            let domain = attributes_bytes.remove(attributes_bytes.len()-1);
            let domain = match domain {
                0 => Domain::Integer,
                1 => Domain::Text,
                2 => Domain::Float,
                _ => return Err(Box::new(DBError::FileFormatError("Incorrect Domain type read.")))
            };
            record_length += Domain::size_in_bytes(&domain);
            let identifier = attributes_bytes.split_off(attributes_bytes.len()-19); // 19 is identifier size written
            let identifier = String::from_utf8(identifier)?;
            let identifier = Identifier::from(&identifier.trim())?;
            attributes.push((identifier, domain));
        }
        attributes.reverse(); // because read in reverse order... consider just reading this in reverse in other places and omitting this

        Ok(Table {
            attributes,
            record_count,
            meta_offset,
            bst,
            record_length,
            file_path,
        })
    }

    pub fn write_record_count(&self) -> Result<(), Box<dyn Error>> {
        let file = OpenOptions::new().write(true).append(false).open(&self.file_path)?;
        file.seek_write(&self.record_count.to_be_bytes(), self.meta_offset as u64 - 4)?; // 4 bytes at the end of the meta
        Ok(())
    }

    pub fn print_attributes(&self) {
        let mut attri_iter = self.attributes.iter();
        if self.bst.is_some() {
            let (attribute, domain) = attri_iter.next().expect("Tables should always have at least one attribute.");
            println!("{}\t{} PRIMARY KEY", attribute.name(), domain.to_string());
        }
        for (attribute, domain) in attri_iter {
            println!("{}\t{}", attribute.name(), domain.to_string());
        }
    }

    fn write_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new().append(true).open(&self.file_path)?;
        let mut record_bytes: Vec<u8> = Vec::new();
        for (data, (_, domain)) in record.iter().zip(self.attributes.iter()) {
            match (data, domain) {
                (Data::Integer(int), Domain::Integer) => record_bytes.append(&mut int.to_bytes().to_vec()),
                (Data::Float(float), Domain::Float) => record_bytes.append(&mut float.to_bytes().to_vec()),
                (Data::Text(text), Domain::Text) => record_bytes.append(&mut text.to_bytes().to_vec()),
                _ => return Err(Box::new(DBError::ConstraintError("Cannot write record with invalid data order.")))
            }
        }
        self.record_count += 1;
        if let Some(ref mut bst) = self.bst {
            let key = match &record[0] {
                Data::Integer(int) => int.value().to_string(),
                Data::Float(float) => format!("{}.{}", float.int.value().to_string(), float.digits.to_string()),
                Data::Text(text) => text.content().to_string(),
            };
            bst.insert(key, self.record_count)?;
        }

        file.write_all(&record_bytes)?;
        // TODO maybe remove from bst if this fails and then return error
        Ok(())
    }

    pub fn write_single_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
        self.write_record(record)?;
        self.write_record_count()
    }

    pub fn read_record(&self, record_num: u32) -> Result<Vec<(&Identifier, Data)>, Box<dyn Error>> {
        if record_num > self.record_count {
            return Err(Box::new(DBError::ConstraintError("Cannot find a record which is out of the table's bounds")))
        }
        let file = File::open(&self.file_path)?;
        let mut record_bytes: Vec<u8> = vec![0; self.record_length as usize];
        println!("{}", self.meta_offset);
        file.seek_read(&mut record_bytes, (self.meta_offset + (record_num-1) * self.record_length) as u64)?;
        let mut record: Vec<(&Identifier, Data)> = Vec::with_capacity(self.attributes.len());
        let mut offset = 0;
        for (identifier, domain) in self.attributes.iter() {
            let data = match domain {
                Domain::Float => {
                    offset += 5; // TODO maybe hardcode this value in as Float::byte_len() and the same for int and text
                    Data::Float(Float::from_bytes(&record_bytes[offset-5..offset])?)
                },
                Domain::Integer => {
                    offset += 4;
                    Data::Integer(Integer::from_bytes(&record_bytes[offset-4..offset])?)
                },
                Domain::Text => {
                    offset += 100;
                    Data::Text(Text::from_bytes(&record_bytes[offset-100..offset])?)
                }
            };
            record.push((identifier, data))
        }
        Ok(record)
    }

    pub fn rename_attributes(&self, new_attributes: Vec<Identifier>) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new().write(true).open(&self.file_path)?;

        // just overwrite all front meta since seek-writing wouldn't greatly improve performance at all
        file.write_all(&(self.attributes.len() as u32).to_be_bytes())?; // 4 bytes
        
        for (attribute, (_, domain)) in new_attributes.iter().zip(&self.attributes) {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
        }
        Ok(())
    }
}


pub enum Data { // same as Domain except contains values
    Integer(Integer),
    Text(Text),
    Float(Float)
}

// TODO change from to build for the below functions
pub struct Identifier {
    name: String,
}

// TODO disallow identifiers from being commands
impl Identifier {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        Identifier::from(&String::from_utf8(bytes.to_vec())?.trim())
    }

    pub fn from(name: &str) -> Result<Self, Box<dyn Error>> {
        if name.len() > 19 {
            return Err(Box::new(DBError::ParseError("Identifer name cannot be longer than 19 characters")));
        } else if name == "" {
            return Err(Box::new(DBError::ParseError("Identifer cannot be an empty string")));
        }

        for c in name.chars() {
            if !c.is_ascii_alphanumeric() {
                return Err(Box::new(DBError::ParseError("Identifier is not alphanumeric.")));
            }
        }

        Ok(Identifier {
            name: String::from(name),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn write_to_file(&self, mut file: &File) -> Result<(), std::io::Error> {
        let mut buf_to_write: [u8; 19] = [b' '; 19]; // write spaces that can be trimmed
        self.name.as_bytes().into_iter().enumerate().for_each(|(i, byte)| {
            buf_to_write[i] = *byte;
        });
        file.write_all(&buf_to_write)
    }
}

pub struct Text {
    content: String,
}

impl Text {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        if bytes.len() != 100 { // should never happen since we should only ever read from valid locations but...
            return Err(Box::new(DBError::ParseError("Text should have been stored as 100 characters.")));
        }
        Text::from(&String::from_utf8(bytes.to_vec())?.trim())
    }

    pub fn to_bytes(&self) -> [u8; 100] {
        let mut buf_to_write: [u8; 100] = [b' '; 100]; // write spaces that can be trimmed
        self.content.as_bytes().into_iter().enumerate().for_each(|(i, byte)| {
            buf_to_write[i] = *byte;
        });
        buf_to_write
    }

    pub fn from(content: &str) -> Result<Self, Box<dyn Error>> {
        if content.len() > 100 {
            return Err(Box::new(DBError::ParseError("Text longer than 100 characters.")));
        }

        Ok(Text {
            content: String::from(content),
        })
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

pub struct Integer {
    value: i32,
}

impl Integer {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let four_bytes: [u8; 4] = bytes.try_into()?;
        Ok(Integer {value: i32::from_be_bytes(four_bytes)})
    }

    pub fn to_bytes(&self) -> [u8; 4] {
        self.value.to_be_bytes()
    }

    pub fn from(value: &str) -> Result<Self, Box<dyn Error>> {
        let value = value.parse()?;

        Ok(Integer { value })
    }

    pub fn value(&self) -> &i32 {
        &self.value
    }
}

pub struct Float {
    int: Integer,
    digits: u8,
}

impl Float {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let four_bytes: [u8; 4] = bytes[0..4].try_into()?;
        Ok(Float {
            int: Integer {value: i32::from_be_bytes(four_bytes)},
            digits: if bytes[4] < 100 {
                bytes[4]
            } else {
                return Err(Box::new(DBError::ParseError("Failed to parse Float decimal value.")));
            }
        })
    }

    pub fn to_bytes(&self) -> [u8; 5] {
        let i = self.int.to_bytes();
        [i[0], i[1], i[2], i[3], self.digits]
    }

    pub fn from(float: &str) -> Result<Self, Box<dyn Error>> {
        let (int, float) = match float.split_once(".") {
            Some((int, float)) => (int, float),
            None => (float, ""),
        };

        let int: Integer = Integer::from(int)?;

        let digits: u8 = if float == "" {
            0
        } else {
            let digits = Integer::from(float)?.value;
            if digits > 99 {
                return Err(Box::new(DBError::ParseError("Failed to parse Float decimal value.")));
            }
            digits as u8
        };

        Ok(Float { int, digits })
    }
}

#[derive(Copy, Clone)]
pub enum Domain {
    Integer,
    Text,
    Float,
}

impl Domain {
    pub fn from(descriptor: &str) -> Result<Self, Box<dyn Error>> {
        match descriptor {
            "Text" => Ok(Domain::Text),
            "Integer" => Ok(Domain::Integer),
            "Float" => Ok(Domain::Float),
            _ => Err(Box::new(DBError::ParseError("Invalid Domain type {descriptor}"))),
        }
    }

    pub fn size_in_bytes(domain: &Domain) -> u32 {
        match domain {
            Domain::Float => 5, // i32 + u8 fraction
            Domain::Integer => 4, // i32
            Domain::Text => 100
        }
    }

    pub fn write_to_file(&self, mut file: &File) -> Result<(), std::io::Error> {
        file.write_all(&[*self as u8])
    }

    pub fn to_string(&self) -> &'static str{
        match self {
            Domain::Float => "Float",
            Domain::Integer => "Integer",
            Domain::Text => "Text"
        }
    }
}

// TODO delete if unused
pub enum RelOp {
    Equals,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}


mod tests {
//     use crate::db_cmds::{create_cmd::Create, exit_cmd::Exit, use_cmd::Use, Command};
//     use std::collections::HashMap;
//     use crate::Database;

//    use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");

    // #[test]
    // fn insert_into_table() -> Result<(), Box<dyn Error>> {
    //     let mut db = Database {
    //         path: String::new(),
    //         table_map: HashMap::new(),
    //     };
    //     Use::execute("foo", &mut db)?;
    //     // Create::execute("TABLE mega (1234567890123456789 TEXT PRIMARY KEY, intTest INTEGER)", &mut db)?;
    //     let table = db.table_map.get_mut("mega").unwrap();
    //     table.write_single_record(vec![
    //     Data::Text(
    //         Text {
    //             content: String::from("This is the text to be written...")
    //         }
    //     ),
    //     Data::Integer(Integer::from("5")?)
    //     ])?;
    //     let record = table.read_record(1)?;
    //     // let to_print = match &record[0].1 {
    //     //     Data::Float(val) => val.digits,
    //     //     _ => 1,
    //     // };
    //     // println!("{}", to_print);
    //     Exit::execute("", &mut db)
    // }
}