use crate::{binary_search_tree, DBError, Database};
use binary_search_tree::BST;
use db_types::*;
use std::{fs, path};
use std::error::Error;

use create::Create;

pub mod db_types;
pub mod create;

pub fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (directive, cmd) = match cmd.split_once(' ') {
        Some((directive, cmd)) => (directive, cmd),
        None => (cmd, ""),
    };

    match directive.trim() {
        "CREATE" => Create::execute(cmd, db),
        "USE" => Use::execute(cmd, db),
        "DESCRIBE" => Describe::execute(cmd, db),
        "SELECT" => Select::execute(cmd, db),
        "LET" => Let::execute(cmd, db),
        "INSERT" => Insert::execute(cmd, db),
        "UPDATE" => Update::execute(cmd, db),
        "DELETE" => Delete::execute(cmd, db),
        "INPUT" => Input::execute(cmd, db),
        "EXIT" => Exit::execute(cmd, db),
        _ => return Err(Box::new(DBError::ParseError("Failed to read command directive.")))
    }
}

// TODO consider moving all types that implement Command into sub-modules of their name sake
pub trait Command {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>>;
}

pub struct Use;

impl Command for Use {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
        db.path = "./".to_owned() + db_name.name();
        db.bst_map.clear();
        db.table_map.clear();

        let db_files = match fs::read_dir(&db.path) {
            Ok(read_dir) => read_dir,
            Err(_) => return Err(Box::new(DBError::ParseError("Failed to read files in database directory."))),
        };

        for file in db_files {
            let file = file?;
            let file_name = String::from(
                file.file_name()
                    .to_str()
                    .expect("file name cannnot use non-ascii characters"),
            );
            let file_name_split = file_name
                .rsplit_once(".")
                .expect("File does not have a dot in the name...?");

            if let (file_name, "index") = file_name_split {
                db.bst_map.insert(
                    String::from(file_name),
                    BST::read_from_file(file.path().as_path())?,
                );
            }
            if let (file_name, "dat") = file_name_split {
                db.table_map.insert(
                    String::from(file_name),
                    Table::read_from_file(file.path().as_path())?,
                );
            }
        }

        Ok(())
    }
}

pub struct Describe;

impl Command for Describe {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        let cmd = cmd.trim();
        for (table_name, table) in db.table_map.iter() {
            if cmd == "ALL" || cmd == table_name {
                println!("{table_name}");
                table.print_details(); // TODO assess whether this is only needed here and flush this logic out
            }
        }
        Ok(())
    }
}

pub struct Select;

impl Command for Select {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create select command
        Ok(())
    }
}

pub struct Let;

impl Command for Let {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create let command
        Ok(())
    }
}

pub struct Insert;

impl Command for Insert {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create insert command
        Ok(())
    }
}

pub struct Update;

impl Command for Update {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create update command
        Ok(())
    }
}

pub struct Delete;

impl Command for Delete {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create delete command
        Ok(())
    }
}

pub struct Input;

impl Command for Input {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO create input command
        Ok(())
    }
}

// TODO make struct for RENAME and implement command ??

pub struct Exit;

impl Command for Exit {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        // TODO needs to save BST's back to index files
        std::process::exit(0);
    }
}
