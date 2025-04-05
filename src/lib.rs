use relation::Table;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::fs;

pub use db_cmds::run_cmd;
pub use db_cmds::run_exit;

mod binary_search_tree;
mod db_cmds;
mod relation;
mod logic;
mod base;

pub struct Database {
    path: String,
    table_map: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            path: String::new(),
            table_map: HashMap::new(),
        }
    }

    pub fn build(path: String) -> Result<Self, Box<dyn Error>> {
        let mut table_map = HashMap::new();

        let db_files: fs::ReadDir = match fs::read_dir(&path) {
            Ok(read_dir) => read_dir,
            Err(_) => {
                return Err(Box::new(DBError::ParseError(
                    "Failed to read files in database directory.",
                )))
            }
        };

        eprintln!("\tReading tables in database directory...");

        for file in db_files {
            let file = file?;
            let file_name = String::from(
                file.file_name()
                    .to_str()
                    .expect("File name cannnot use non-ascii characters."),
            );
            let file_name_split = file_name
                .rsplit_once(".")
                .expect("File name should have a dot separated identifier.");

            if let (table_name, "dat") = file_name_split {
                table_map.insert(
                    String::from(table_name),
                    Table::read_from_file(table_name, &path)?,
                );
            }
        }

        Ok(Database { path, table_map })
    }
}

// TODO only bubble errors that should allow future commands to be run and then just print messages and continue
#[derive(Debug)]
pub enum DBError {
    ParseError(&'static str),
    ConstraintError(&'static str),
    FileFormatError(&'static str),
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            DBError::ParseError(s) => write!(f, "Failed to parse a line: {s}"),
            DBError::ConstraintError(s) => write!(f, "Invalid value given for a type: {s}"),
            DBError::FileFormatError(s) => write!(f, "Incorrect file format: {s}"),
        }
    }
}

impl Error for DBError {}

#[cfg(test)]
mod tests {
    // use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");
}
