use crate::{DBError, Database};
use super::db_types::*;
use std::error::Error;
use std::fs;
use std::path;

// TODO remove mut references to db that are not necessary

pub fn run_cmd(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (directive, cmd) = match cmd.split_once(' ') {
        Some((directive, cmd)) => (directive, cmd),
        None => (cmd, ""),
    };

    match directive.trim() {
        "CREATE" => run_create(cmd, db),
        "USE" => run_use(cmd, db),
        "DESCRIBE" => run_describe(cmd, db),
        "SELECT" => run_select(cmd, db),
        "LET" => run_let(cmd, db),
        "INSERT" => run_insert(cmd, db),
        "UPDATE" => run_update(cmd, db),
        "DELETE" => run_delete(cmd, db),
        "INPUT" => run_input(cmd, db),
        "RENAME" => run_rename(cmd, db),
        "EXIT" => {
            if !cmd.trim().is_empty() {
                eprintln!("\tEXIT command does not take arguments.");
            }
            run_exit(db)
        },
        "" => return Ok(()), // TODO work on removing this line
        _ => return Err(Box::new(DBError::ParseError("Failed to read command directive.")))
    }
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

fn run_select(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // AttrNameList FROM TableNameList WHERE condition
    let (attri_name_list, cmd) = match cmd.split_once("FROM") {
        Some(tuple) => tuple,
        None => return Err(Box::new(DBError::ParseError("SELECT command requires FROM clause.")))
    };

    let (table_name_list, condition) = match cmd.split_once("WHERE") {
        Some(tuple) => tuple,
        None => return Err(Box::new(DBError::ParseError("SELECT command requires WHERE clause.")))
    };

    let select_attributes: Vec<&str> = attri_name_list.split(",").map(|attri| -> &str {
        attri.trim()
    }).collect();



    // TODO finish select command processing
    // setup SELECT to be usable as a primary command and a recursive piece that prints output

    Ok(())
}

fn run_rename(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (table_name, attribute_names) = match cmd.split_once(' ') {
        Some((table_name, attribute_names)) => (table_name.trim(), attribute_names.trim()),
        None => return Err(Box::new(DBError::ParseError("SELECT command requires WHERE clause.")))
    };

    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => return Err(Box::new(DBError::ParseError("No table found with the given name.")))
    };

    let mut new_attributes: Vec<Identifier> = Vec::new();
    for attribute_name in iterate_list(attribute_names)? {
        new_attributes.push(Identifier::from(attribute_name)?);
    }
    if table.attributes().len() != new_attributes.len() {
        return Err(Box::new(DBError::ConstraintError("Incorrect number of attributes found to RENAME table.")))
    }
    table.rename_attributes(new_attributes)?;
    Ok(())
}

fn run_let(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // TODO create let command
    Ok(())
}

fn run_update(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // TODO create the UPDATE command
    Ok(())
}

fn run_delete(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // TODO create delete command
    Ok(())
}

fn run_input(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // TODO create input command
    Ok(())
}

fn run_use(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
    *db = Database::build("./".to_owned() + db_name.name() + "/")?;
    eprintln!("\tSuccess!");
    Ok(())
}

fn run_insert(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // table_name VALUES (data vec);
    let (table_name, cmd) = match cmd.split_once("VALUES") {
        Some((table_name, cmd)) => (table_name.trim(), cmd.trim()),
        None => return Err(Box::new(DBError::ParseError("Invalid arguments for INSERT")))
    };

    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => return Err(Box::new(DBError::ParseError("No table found with the given name.")))
    };

    let values: Vec<Result<Data, Box<dyn Error>>> = iterate_list(cmd)?.enumerate().map(|(index, value)| -> Result<Data, Box<dyn Error>> {
        let value = value.trim();
        let domain = table.attributes()[index].1;
        match domain {
            Domain::Integer => Ok(Data::Integer(Integer::from(value)?)),
            Domain::Float => Ok(Data::Float(Float::from(value)?)),
            Domain::Text => Ok(Data::Text(Text::from(value)?)),
        }
    }).collect();
    let mut record: Vec<Data> = Vec::with_capacity(values.len());
    for value in values.into_iter() {
        record.push(value?);
    }
    table.write_single_record(record)
}

pub fn run_exit(db: &mut Database) -> Result<(), Box<dyn Error>> {
    eprintln!("\tSaving indices and table sizes");
    for (table_name, table) in db.table_map.iter() {
        table.write_record_count()?;
        if let Some(ref bst) = table.bst {
            let bst_path = format!("{}{}.index", db.path, table_name);
            bst.write_to_file(&bst_path)?;
        }
    }
    eprintln!("\tSuccess!");
    std::process::exit(0);
}

fn run_describe(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let table_name = cmd.trim();
    if table_name.is_empty() {
        return Err(Box::new(DBError::ParseError("DESCRIBE requires at least one argument.")))
    }
    if table_name != "ALL" {
        let table = match db.table_map.get(table_name) {
            Some(table) => table,
            None => return Err(Box::new(DBError::ParseError("Table name not found in current database.")))
        };
        println!("{table_name}");
        table.print_attributes();
        return Ok(())
    }
    for (table_name, table) in db.table_map.iter() {
        println!("{table_name}");
        table.print_attributes();
    }
    Ok(())
}

fn create_database(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?;

    db.path = "./".to_owned() + db_name.name();

    if !path::Path::new(&db.path).is_dir() {
        println!("Creating database directory...");
        fs::create_dir(&db.path)
            .expect("Unable to create a directory for database storage");
        eprintln!("\tSuccess!");
    } else {
        eprintln!("\tA Database with the given name exists already...");
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

    let mut attribute_list: Vec<(Identifier, Domain)> = Vec::new();

    let mut attribute_iter = iterate_list(cmd)?;

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

    let primary_key = match (first_attri_iter.next(), first_attri_iter.next()) {
        (None, None) => false,
        (Some("PRIMARY"), Some("KEY")) => true,
        _ => return Err(Box::new(DBError::ParseError("Did not recognize third argument in attribute definition.")))
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
    db.table_map.insert(table_name.to_string(),Table::build(table_name, attribute_list, primary_key, &db.path)?);

    eprintln!("\tSuccess!");
    Ok(())
}

fn run_create(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (cmd_0, cmd) = match cmd.split_once(' ') {
        Some(tuple) => tuple,
        None => return Err(Box::new(DBError::ParseError("CREATE command requires arguments.")))
    };

    if cmd_0 == "DATABASE" {
        create_database(cmd, db)
    } else if cmd_0 == "TABLE" {
        create_table(cmd, db)
    } else {
        Err(Box::new(DBError::ParseError("Syntax error after directive CREATE.")))
    }
}