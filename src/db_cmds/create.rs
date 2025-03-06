use super::{
    Command,
    Identifier,
    Domain,
    Table,
    DBError,
    super::Database
};
use std::{
    fs,
    path,
    error::Error
};

pub struct Create;

impl Create {
    fn create_database(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        let db_name = Identifier::from(cmd.trim())?;

        db.path = "./".to_owned() + db_name.name();

        if !path::Path::new(&db.path).is_dir() {
            println!("Creating database directory...");
            fs::create_dir(&db.path)
                .expect("Unable to create a directory for database storage");
            eprintln!("Success!");
        } else {
            eprintln!("A Database with the given name exists already...");
        }

        Ok(())
    }

    fn create_table(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        if db.path == "" {
            return Err(Box::new(DBError::ParseError("Database path not set. Run the USE command before table creation.")));
        }

        let (table_name, cmd) = match cmd.split_once(' ') {
            Some(tuple) => tuple,
            None => return Err(Box::new(DBError::ParseError("Not enough arguments for CREATE TABLE")))
        };

        let table_name = Identifier::from(table_name)?;

        let mut attribute_list: Vec<(Identifier, Domain)> = Vec::new();

        let mut attribute_iter = Create::iterate_list(cmd)?;

        let first_attribute = match attribute_iter.next() {
            Some(s) => s.trim(),
            None => return Err(Box::new(DBError::ParseError("Attribute list must not be empty")))
        };

        let mut first_attri_iter = first_attribute.split_whitespace();

        attribute_list.push( 
            (
                Identifier::from(first_attri_iter.next().unwrap_or(""))?, 
                Domain::from(first_attri_iter.next().unwrap_or(""))?
            )
        );

         let primary_key = match first_attri_iter.next() {
            None => false,
            Some("PRIMARY KEY") => true,
            Some(_) => return Err(Box::new(DBError::ParseError("Did not recognize third argument in attribute definition.")))
        };

        for attribute in attribute_iter {
            attribute_list.push( match attribute.trim().split_once(' ') {
                Some((name, domain)) => (
                    Identifier::from(name)?, 
                    Domain::from(domain)?
                ),
                None => return Err(Box::new(DBError::ParseError("Did not find a Domain for an Attribute in the list")))
            });
        }

        // setup table struct to use its builtin formatting
        let mut table = Table::new(table_name, attribute_list, primary_key)?;

        table.write_meta(&db.path)?;
        Ok(())
    }

    fn iterate_list<'a>(list: &'a str) -> Result<std::str::Split<'a, char>, Box<dyn Error>> {
        let (check, list) = match list.split_once('(') {
            Some(tuple) => tuple,
            None => return Err(Box::new(DBError::ParseError("Lists must be wrapped in parenthesis.")))
        };

        if check.trim() != "" {
            return Err(Box::new(DBError::ParseError("Cannot parse list that does not start with '('")));
        }

        let (list, check) = match list.split_once(')') {
            Some(tuple) => tuple,
            None => return Err(Box::new(DBError::ParseError("Lists must be wrapped in parenthesis.")))
        };

        if check.trim() != "" {
            return Err(Box::new(DBError::ParseError("Cannot parse list that does not end with ')'")));
        }

        return Ok(list.split(','));
    }
}

impl Command for Create {
    fn execute(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
        let (cmd_0, cmd) = match cmd.split_once(' ') {
            Some(tuple) => tuple,
            None => return Err(Box::new(DBError::ParseError("CREATE command requires arguments.")))
        };

        if cmd_0 == "DATABASE" {
            Create::create_database(cmd, db)
        } else if cmd_0 == "TABLE" {
            Create::create_table(cmd, db)
        } else {
            Err(Box::new(DBError::ParseError("Syntax error after directive CREATE.")))
        }
    }
}