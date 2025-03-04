// types that enforce certain constraints through constructor functions and permissions

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct Table {
    // TODO iron out what needs to be in table and what is fluff.
    name: Identifier,
    path: String,
    attributes: Vec<(Identifier, Domain)>,
    record_count: i32,
    offset: i32,
    record_length: i32,
    file: Option<File>,
}

impl Table {
    pub fn new(
        path: String,
        name: Identifier,
        attributes: Vec<(Identifier, Domain)>,
    ) -> Result<Self, String> {
        // TODO calculate offset & record_length

        let table_path = path + "/" + name.name() + ".dat";

        Ok(Table {
            name,
            path: table_path,
            attributes,
            record_count: 0,
            offset: 0,
            record_length: 0,
            file: None,
        })
    }

    // TODO implement reading and writing logic for tables to a file.
    pub fn read_from_file(path: &Path) -> Result<Self, String> {
        Ok(Table {
            name: Identifier::from("")?,
            path: String::new(),
            attributes: Vec::new(),
            record_count: 0,
            offset: 0,
            record_length: 0,
            file: None,
        })
    }

    pub fn print_details(&self) {
        println!("table deets");
    }

    pub fn write_meta(&mut self) -> Result<(), String> {
        self.file = match File::create_new(&self.path) {
            Ok(file) => Some(file),
            Err(_) => {
                return Err("Failed to create file for table allocation.
            This is likely because this table already exists."
                    .to_owned())
            }
        };

        let file = self.file.as_mut().unwrap();

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
    pub fn from(name: &str) -> Result<Self, String> {
        if name.len() > 19 {
            return Err("Identifier longer than 19 characters.".to_owned());
        }

        for c in name.chars() {
            if !c.is_ascii_alphanumeric() {
                return Err("Identifier is not alphanumeric.".to_owned());
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
    pub fn from(content: &str) -> Result<Self, String> {
        if content.len() > 100 {
            return Err("Text longer than 100 characters.".to_owned());
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
    pub fn from(value: &str) -> Result<Self, String> {
        let value = match value.parse() {
            Ok(n) => n,
            Err(_) => return Err("Failed to parse Integer value.".to_owned()),
        };

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
    pub fn from(float: &str) -> Result<Self, String> {
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
                return Err("Failed to parse Float decimal value.".to_owned());
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
    pub fn from(descriptor: &str) -> Result<Self, String> {
        match descriptor {
            "Text" => Ok(Domain::Text),
            "Integer" => Ok(Domain::Integer),
            "Float" => Ok(Domain::Float),
            _ => Err(format!("Invalid Domain type {descriptor}")),
        }
    }
}
