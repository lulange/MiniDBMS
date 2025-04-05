use std::error::Error;
use crate::DBError;
use crate::Database;
use std::path;
use std::fs;
use crate::base::*; // TODO fix after restructure
use crate::relation::Table;

pub(super) fn iterate_list<'a>(list: &'a str) -> Result<std::str::Split<'a, char>, Box<dyn Error>> {
    let (check, list) = match list.split_once('(') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "Lists must be wrapped in parenthesis.",
            )))
        }
    };

    if check.trim() != "" {
        return Err(Box::new(DBError::ParseError(
            "Cannot parse list that does not start with '('",
        )));
    }

    let (list, check) = match list.split_once(')') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "Lists must be wrapped in parenthesis.",
            )))
        }
    };

    if check.trim() != "" {
        return Err(Box::new(DBError::ParseError(
            "Cannot parse list that does not end with ')'",
        )));
    }

    return Ok(list.split(','));
}

pub(super) fn create_database(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?;

    db.path = "./".to_owned() + db_name.name();

    if !path::Path::new(&db.path).is_dir() {
        println!("Creating database directory...");
        fs::create_dir(&db.path).expect("Unable to create a directory for database storage");
        eprintln!("\tSuccess!");
    } else {
        eprintln!("\tA Database with the given name exists already...");
    }

    Ok(())
}

pub(super) fn create_table(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    if db.path == "" {
        return Err(Box::new(DBError::ParseError(
            "Database path not set. Run the USE command before table creation.",
        )));
    }

    let (table_name, cmd) = match cmd.split_once(' ') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "Not enough arguments for CREATE TABLE",
            )))
        }
    };

    let mut attribute_list: Vec<(Identifier, Domain)> = Vec::new();

    let mut attribute_iter = iterate_list(cmd)?;

    let first_attribute = match attribute_iter.next() {
        Some(s) => s.trim(),
        None => {
            return Err(Box::new(DBError::ParseError(
                "Attribute list must not be empty",
            )))
        }
    };

    let mut first_attri_iter = first_attribute.split_whitespace();

    attribute_list.push((
        Identifier::from(first_attri_iter.next().unwrap_or(""))?,
        Domain::from(first_attri_iter.next().unwrap_or(""))?,
    ));

    let primary_key = match (first_attri_iter.next(), first_attri_iter.next()) {
        (None, None) => false,
        (Some("primary"), Some("key")) => true,
        _ => {
            return Err(Box::new(DBError::ParseError(
                "Did not recognize third argument in attribute definition.",
            )))
        }
    };

    for attribute in attribute_iter {
        attribute_list.push(match attribute.trim().split_once(' ') {
            Some((name, domain)) => (Identifier::from(name)?, Domain::from(domain)?),
            None => {
                return Err(Box::new(DBError::ParseError(
                    "Did not find a Domain for an Attribute in the list",
                )))
            }
        });
    }

    // setup table struct to use its builtin formatting
    db.table_map.insert(
        table_name.to_string(),
        Table::build(table_name, attribute_list, primary_key, &db.path)?,
    );

    eprintln!("\tSuccess!");
    Ok(())
}