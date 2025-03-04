use crate::{binary_search_tree, Database};
use binary_search_tree::BST;
use db_types::*;
use std::{fs, path};

pub mod db_types;

pub fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
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
        _ => {
            return Err("Could not process command directive.".to_owned());
        }
    }
}

// TODO consider moving all types that implement Command into sub-modules of their name sake
pub trait Command {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String>;
}

pub struct Create;

// TODO extract some of this logic to use when parsing lists in parenthesis more generally.
// Put that function at the top level somewhere
impl Command for Create {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        let (cmd_0, cmd) = cmd.split_once(' ').unwrap();

        if cmd_0 == "DATABASE" {
            let db_name = Identifier::from(cmd.trim())?;

            db.path = "./".to_owned() + db_name.name();

            if !path::Path::new(&db.path).is_dir() {
                println!("Creating database directory...");
                fs::create_dir(&db.path)
                    .expect("Unable to create a directory for database storage");
                println!("Success!");
            } else {
                println!("database with that name exists already...");
            }
        } else if cmd_0 == "TABLE" {
            if db.path == "" {
                return Err("Database path not set. USE command required to initialize.".to_owned());
            }

            let (table_name, cmd) = cmd.split_once(' ').unwrap();
            let table_name = Identifier::from(table_name)?;

            let mut attribute_list: Vec<(Identifier, Domain)> = Vec::new();

            let (check, cmd) = cmd.split_once('(').unwrap();

            if check.trim() != "" {
                return Err(
                    "Table names do not support spaces. Make sure the attribute list
                directly follows your table name and is surrounding by parenthesis."
                        .to_owned(),
                );
            }

            let (cmd, check) = cmd.rsplit_once(')').unwrap();

            if check.trim() != "" {
                return Err("Could not process text following attribute list. Please enter new commands on new lines.".to_owned());
            }

            let mut primary_attribute = -1;

            for (index, attribute) in cmd.split(',').enumerate() {
                if let Some((name, domain)) = attribute.trim().split_once(' ') {
                    let name = Identifier::from(name)?;
                    if let Some((domain, primary_key_mod)) = domain.trim().split_once(' ') {
                        if primary_key_mod.trim() == "PRIMARY KEY" && primary_attribute == -1 {
                            primary_attribute = index as i32;
                            let domain = Domain::from(domain)?;
                            attribute_list.push((name, domain));
                        } else {
                            return Err("Can not have multiple primary keys in a table. Primary key already set.".to_owned());
                        }
                    } else {
                        let domain = Domain::from(domain)?;
                        attribute_list.push((name, domain));
                    }
                } else if attribute.trim() != "" {
                    return Err(
                        "Invalid attribute found. Make sure these follow the pattern of 
                    'Identifier Domain PRIMARY KEY'"
                            .to_owned(),
                    );
                }
            }

            if primary_attribute == -1 {
                return Err("No primary key given. Please set one of the attributes as a primary key with 'PRIMARY KEY'
                following its Domain.".to_owned());
            }

            // setup table struct to use its builtin formatting
            let mut table = Table::new(db.path.clone(), table_name, attribute_list)?;

            table.write_meta()?;
        }

        Ok(())
    }
}

pub struct Use;

impl Command for Use {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
        db.path = "./".to_owned() + db_name.name();
        db.bst_map.clear();
        db.table_map.clear();

        let db_files = match fs::read_dir(&db.path) {
            Ok(read_dir) => read_dir,
            Err(_) => return Err("Failed to read files in database directory.".to_owned()),
        };

        for file in db_files {
            let file = file.expect("Couldn't read file from directory");
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
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
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
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create select command
        Ok(())
    }
}

pub struct Let;

impl Command for Let {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create let command
        Ok(())
    }
}

pub struct Insert;

impl Command for Insert {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create insert command
        Ok(())
    }
}

pub struct Update;

impl Command for Update {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create update command
        Ok(())
    }
}

pub struct Delete;

impl Command for Delete {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create delete command
        Ok(())
    }
}

pub struct Input;

impl Command for Input {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO create input command
        Ok(())
    }
}

// TODO make struct for RENAME and implement command ??

pub struct Exit;

impl Command for Exit {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), String> {
        // TODO needs to save BST's back to index files
        std::process::exit(0);
    }
}
