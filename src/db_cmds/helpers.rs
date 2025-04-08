use std::collections::HashMap;
use std::error::Error;
use crate::logic::Constraint;
use crate::logic::RelOp;
use crate::relation::MemTable;
use crate::DBError;
use crate::Database;
use std::path;
use std::fs;
use crate::logic::Condition;
use crate::base::*; // TODO fix after restructure
use crate::relation::Table;
use crate::logic::Operand;


pub fn iterate_list<'a>(list: &'a str) -> Result<std::str::Split<'a, char>, Box<dyn Error>> {
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

pub fn create_database(cmd: &str) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?;

    let db_path = "./".to_owned() + db_name.name();

    if !path::Path::new(&db_path).is_dir() {
        fs::create_dir(&db_path).expect("Unable to create a directory for database storage");
    } else {
        eprintln!("\tA Database with the given name exists already...");
    }

    Ok(())
}

pub fn create_table(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    if db.path == "" {
        return Err(Box::new(DBError::ParseError(
            "Database path not set. Run the USE command before table creation.",
        )));
    }

    let (table_name, cmd) = match cmd.split_once(' ') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "Not enough arguments for CREATE TABLE.",
            )))
        }
    };

    if db.table_map.get(table_name).is_some() {
        return Err(Box::new(DBError::ParseError(
            "Table with name given already exists.",
        )))
    }

    let mut attribute_list: Vec<(Identifier, Domain)> = Vec::new();

    let mut attribute_iter = iterate_list(cmd)?;

    let first_attribute = match attribute_iter.next() {
        Some(s) => s.trim(),
        None => {
            return Err(Box::new(DBError::ParseError(
                "Attribute list must not be empty.",
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
                    "Did not find a Domain for an Attribute in the list.",
                )))
            }
        });
    }

    // setup table struct to use its builtin formatting
    db.table_map.insert(
        table_name.to_string(),
        Table::build(table_name, attribute_list, primary_key, &db.path)?,
    );

    Ok(())
}

pub fn delete_tuples(db: &mut Database, table: &str, cond: &str) -> Result<(), Box<dyn Error>> {
    let cond = Condition::parse(cond)?;
    match db.table_map.get_mut(table) {
        Some(table) => cond.delete(table),
        None => Err(DBError::ParseError("Could not find a table with that name to delete from."))?
    }
}

pub fn delete_table(db: &mut Database, table: &str) -> Result<(), Box<dyn Error>> {
    match db.table_map.remove(table) {
        Some(table) => {
            table.clean_up()?;
            Ok(())
        },
        None => Err(DBError::ParseError("Could not find a table with that name to delete."))?
    }
}

pub fn parse_new_attr_values(table: &Table, mut new_values: &str) -> Result<Vec<(Identifier, Data)>, Box<dyn Error>> {
    let tables = vec![table];
    let mut new_value_equalities = Vec::new();
    let mut constraint;
    loop {
        new_values = new_values.trim_start();
        (constraint, new_values) = Constraint::parse_split(new_values)?;
        if constraint.rel_op == RelOp::Equals {
            constraint.convert_with(&tables)?;
            new_value_equalities.push(constraint);
        } else {
            Err(DBError::ParseError("Found incorrect operator in UPDATE SET clause."))?
        }
        if !new_values.is_empty() {
            new_values = &new_values[1..]; // remove a comma if there is one
        } else {
            break;
        }
    }

    let mut new_values = Vec::with_capacity(new_value_equalities.len());
    let mut attributes_used = HashMap::new();

    for equality in new_value_equalities {
        let id = match equality.left_op {
            Operand::Attribute((_, j)) => {
                if attributes_used.insert(j, 0).is_some() {
                    Err(DBError::ParseError("Cannot set an attribute twice in an UPDATE SET clause."))?
                }
                table.attributes()[j].0.clone()
            }
            _ => Err(DBError::ParseError("Expected Attribute name in left operator for UPDATE SET clause."))?
        };

        let data = match equality.right_op {
            Operand::Value(data) => data,
            _ => Err(DBError::ParseError("Expected Data value in right operator for UPDATE SET clause."))?
        };

        new_values.push((id, data));
    }
    Ok(new_values)
}

pub fn select_from_tables(cmd: &str, db: &mut Database) -> Result<MemTable, Box<dyn Error>> {
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
        match db.table_map.get(table_name) {
            Some(table) => tables.push(table),
            None => Err(DBError::ParseError("Could not find one of the tables to SELECT from."))?
        }
    }

    let cond = Condition::parse(condition)?;

    let mut select_table = cond.select(tables)?;

    if select_attributes.len() != 1 || select_attributes[0] != "*" {
        select_table.project(select_attributes)?;
    }

    Ok(select_table)
}