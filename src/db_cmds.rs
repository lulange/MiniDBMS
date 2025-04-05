use crate::{logic::Condition, DBError, Database};
use std::error::Error;
use crate::base::{
    Identifier,
    Data,
    Domain,
    Integer,
    Float,
    Text
};

// helpers contains functions the run_* functions call to get their job done quicker. No real organization to them currently
mod helpers;

use helpers::*;

// TODO remove mut references to db that are not necessary

pub fn run_cmd(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let cmd = cmd.to_lowercase();
    let (directive, cmd) = match cmd.split_once(' ') {
        Some((directive, cmd)) => (directive, cmd),
        None => (&cmd[..], ""),
    };

    match directive.trim() {
        "create" => run_create(cmd, db),
        "use" => run_use(cmd, db),
        "describe" => run_describe(cmd, db),
        "select" => run_select(cmd, db),
        //"let" => run_let(cmd, db),
        "insert" => run_insert(cmd, db),
        //"update" => run_update(cmd, db),
        //"delete" => run_delete(cmd, db),
        //"input" => run_input(cmd, db),
        "rename" => run_rename(cmd, db),
        "exit" => {
            if !cmd.trim().is_empty() {
                eprintln!("\tEXIT command does not take arguments.");
            }
            run_exit(db)
        }
        "" => return Ok(()), // TODO work on removing this line
        _ => {
            return Err(Box::new(DBError::ParseError(
                "Failed to read command directive.",
            )))
        }
    }
}

fn run_select(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (attri_name_list, cmd) = match cmd.split_once("from") {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "SELECT command requires FROM clause.",
            )))
        }
    };

    let (table_name_list, condition) = match cmd.split_once("where") {
        Some((table_name_list, condition)) => (table_name_list, condition.trim()),
        None => (cmd, "") // "" here since no condition is always true
    };


    let select_attributes: Vec<&str> = attri_name_list
        .split(",")
        .map(|attri| -> &str { attri.trim() })
        .collect();

    let table_name_list: Vec<&str> = table_name_list
        .split(",")
        .map(|table| -> &str { table.trim() })
        .collect();

    let mut tables = Vec::with_capacity(table_name_list.len());
    for table_name in table_name_list {
        db.table_map.get(table_name).inspect(|table| {
            tables.push(*table);
        });
    }

    let cond = Condition::parse(condition)?;

    let mut select_table = cond.select(tables)?;

    select_table.project(select_attributes)?;
    select_table.print();

    Ok(())
}

fn run_rename(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (table_name, attribute_names) = match cmd.split_once(' ') {
        Some((table_name, attribute_names)) => (table_name.trim(), attribute_names.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "RENAME command requires attribute names",
            )))
        }
    };

    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => {
            return Err(Box::new(DBError::ParseError(
                "No table found with the given name.",
            )))
        }
    };

    let mut new_attributes: Vec<Identifier> = Vec::new();
    for attribute_name in iterate_list(attribute_names)? {
        new_attributes.push(Identifier::from(attribute_name)?);
    }
    if table.attributes().len() != new_attributes.len() {
        return Err(Box::new(DBError::ConstraintError(
            "Incorrect number of attributes found to RENAME table.",
        )));
    }
    table.rename_attributes(new_attributes)?;
    Ok(())
}

// fn run_let(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
//     // TODO create let command
//     todo!();
// }

// fn run_update(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
//     // TODO create the UPDATE command
//     todo!();
//     Ok(())
// }

// fn run_delete(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
//     // TODO create delete command
//     Ok(())
// }

// fn run_input(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
//     // TODO create input command
//     Ok(())
// }

fn run_use(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
    *db = Database::build("./".to_owned() + db_name.name() + "/")?;
    eprintln!("\tSuccess!");
    Ok(())
}

fn run_insert(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // table_name VALUES (data vec);
    let (table_name, cmd) = match cmd.split_once("values") {
        Some((table_name, cmd)) => (table_name.trim(), cmd.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "Invalid arguments for INSERT",
            )))
        }
    };

    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => {
            return Err(Box::new(DBError::ParseError(
                "No table found with the given name.",
            )))
        }
    };

    let values: Vec<Result<Data, Box<dyn Error>>> = iterate_list(cmd)?
        .enumerate()
        .map(|(index, value)| -> Result<Data, Box<dyn Error>> {
            let value = value.trim();
            let domain = table.attributes()[index].1;
            match domain {
                Domain::Integer => Ok(Data::Integer(Integer::from(value)?)),
                Domain::Float => Ok(Data::Float(Float::from(value)?)),
                Domain::Text => {
                    if value.starts_with('"') && value.ends_with('"') && value.len() > 1 {
                        // unwrap the double quotes before feeding it to Text
                        Ok(Data::Text(Text::from(&value[1..value.len()-1])?))
                    } else {
                        return Err(Box::new(DBError::ParseError("String literal expected. Wrap literals in double quotes.")))
                    }
                },
            }
        })
        .collect();
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
    eprintln!("\tPROGRAM END");
    std::process::exit(0);
}

fn run_describe(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let table_name = cmd.trim();
    if table_name.is_empty() {
        return Err(Box::new(DBError::ParseError(
            "DESCRIBE requires at least one argument.",
        )));
    }
    if table_name != "all" {
        let table = match db.table_map.get(table_name) {
            Some(table) => table,
            None => {
                return Err(Box::new(DBError::ParseError(
                    "Table name not found in current database.",
                )))
            }
        };
        println!("{table_name}");
        table.print_attributes();
        return Ok(());
    }
    for (table_name, table) in db.table_map.iter() {
        println!("{table_name}");
        table.print_attributes();
    }
    Ok(())
}

fn run_create(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (cmd_0, cmd) = match cmd.split_once(' ') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "CREATE command requires arguments.",
            )))
        }
    };

    if cmd_0 == "database" {
        create_database(cmd, db)
    } else if cmd_0 == "table" {
        create_table(cmd, db)
    } else {
        Err(Box::new(DBError::ParseError(
            "Syntax error after directive CREATE.",
        )))
    }
}
