// types that enforce certain constraints through constructor functions and permissions

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::error::Error;

use crate::DBError;

pub struct Table {
    // TODO iron out what needs to be in table and what is fluff.
    name: Identifier,
    attributes: Vec<(Identifier, Domain)>,
    record_count: i32,
    primary_key: bool,
    offset: i32,
    record_length: i32,
    file: Option<File>,
}

impl Table {
    pub fn new(
        name: Identifier,
        attributes: Vec<(Identifier, Domain)>,
        primary_key: bool,
    ) -> Result<Self, Box<dyn Error>> {
        // TODO calculate offset & record_length

        Ok(Table {
            name,
            attributes,
            record_count: 0,
            primary_key,
            offset: 0,
            record_length: 0,
            file: None,
        })
    }

    // TODO implement reading and writing logic for tables to a file.
    pub fn read_from_file(path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Table {
            name: Identifier::from("")?,
            attributes: Vec::new(),
            record_count: 0,
            primary_key: true,
            offset: 0,
            record_length: 0,
            file: None,
        })
    }

    pub fn print_details(&self) {
        println!("table deets");
    }

    // TODO make dir a more restrictive type or somethin
    pub fn write_meta(&mut self, dir: &str) -> Result<(), Box<dyn Error>> {
        let mut file = File::create_new(format!("{dir}{}.dat", self.name.name()))?;

        // TODO clean up / standardize this writing process... also create a reading process

        // attribute list size
        file.write_all(&self.attributes.len().to_be_bytes())
            .expect("Should be able to write to file.");

        for (attribute, domain) in self.attributes.iter() {
            file.write_all(&(attribute.name().len() as u8).to_be_bytes())
                .expect("Should be able to write to file."); // always less than 20

            file.write_all(attribute.name().as_bytes())
                .expect("Should be able to write to file.");

            file.write_all(&[*domain as u8])
                .expect("Should be able to write to file.");
        }

        // table_size
        file.write_all(&0_i32.to_be_bytes())
            .expect("Should be able to write to file."); // i32 default

        self.file = Some(file);

        Ok(())
    }
}

// TODO make all of these types below have write and read to file functions that allow easier record storage
// TODO maybe make a record type which contains a format for a table record and calls write and read for these
// TODO remove all overwrites of the from function and change them to create_from_str or something like that
pub struct Identifier {
    name: String,
}

impl Identifier {
    pub fn from(name: &str) -> Result<Self, Box<dyn Error>> {
        if name.len() > 19 {
            return Err(Box::new(DBError::ParseError("Identifer name cannot be longer than 19 characters")));
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
}

pub struct Text {
    content: String,
}

impl Text {
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
}
