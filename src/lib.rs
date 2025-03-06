use binary_search_tree::BST;
use db_cmds::db_types::Table;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::io;

pub mod binary_search_tree;
pub mod db_cmds;

pub struct Database {
    pub path: String,
    pub bst_map: HashMap<String, BST>,
    pub table_map: HashMap<String, Table>,
}

#[derive(Debug)]
pub enum DBError {
    ParseError(&'static str),
    ConstraintError(&'static str),
    FileFormatError(&'static str)
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



pub fn run() -> Result<(), Box<dyn Error>> {
    // a mutable reference will get passed around and treated like a singleton
    let mut db = Database {
        path: String::new(),
        bst_map: HashMap::new(),
        table_map: HashMap::new(),
    };

    let stdin: io::Stdin = io::stdin();
    loop {
        let mut line = String::new();
        stdin.read_line(&mut line)?;
        execute_cmds_from(line, &mut db)?;
    }
}

fn execute_cmds_from(cmds: String, db: &mut Database) -> Result<(), Box<dyn Error>> {
    for cmd in cmds.split_terminator('\n') {
        db_cmds::execute(cmd, db)?;
    }

    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");

    #[test]
    fn execute_blank() {
        execute_cmds_from(
            "".to_owned(),
            &mut Database {
                path: String::new(),
                bst_map: HashMap::new(),
                table_map: HashMap::new(),
            },
        )
        .unwrap();
    }
}
