use crate::{binary_search_tree::BST, logic::Condition, DBError, Database};
use std::{error::Error, fs::{File, OpenOptions}, io::{Read, Write}, vec};
use crate::base::{
    Identifier,
    Data,
    Domain,
    Integer,
    Float,
    Text
};
use crate::relation::Table;

mod helpers;

use helpers::*;

pub fn run_cmd(cmd: &str, db: &mut Database) -> Result<Vec<String>, Box<dyn Error>> {
    let cmd = cmd.to_lowercase();
    let (directive, cmd) = match cmd.split_once(' ') {
        Some((directive, cmd)) => (directive, cmd),
        None => (&cmd[..], ""),
    };

    match directive.trim() {
        "create" => {
            run_create(cmd, db)?;
            Ok(vec![])
        }
        "use" => {
            run_use(cmd, db)?;
            Ok(vec![])
        }
        "describe" => run_describe(cmd, db),
        "select" => run_select(cmd, db),
        "let" => {
            run_let(cmd, db)?;
            Ok(vec![])
        }
        "insert" => {
            run_insert(cmd, db)?;
            Ok(vec![])
        }
        "update" => {
            run_update(cmd, db)?;
            Ok(vec![])
        }
        "delete" => {
            run_delete(cmd, db)?;
            Ok(vec![])
        }
        "input" => {
            run_input(cmd, db)?;
            Ok(vec![])
        },
        "rename" => {
            run_rename(cmd, db)?;
            Ok(vec![])
        }
        "exit" => {
            if !cmd.trim().is_empty() {
                eprintln!("\tEXIT command does not take arguments.");
            }
            run_exit(db)?;
            Ok(vec![])
        }
        "" => return Ok(vec![]),
        _ => {
            return Err(Box::new(DBError::ParseError(
                "Failed to read command directive.",
            )))
        }
    }
}

fn run_select(cmd: &str, db: &mut Database) -> Result<Vec<String>, Box<dyn Error>> {
    let return_vec = select_from_tables(cmd, db)?.to_string_vec();
    eprintln!("\tSELECT Success!");
    Ok(return_vec)
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
    eprintln!("\tRENAME Success!");
    Ok(())
}

fn run_let(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    if db.path == "" {
        return Err(Box::new(DBError::ParseError(
            "Database path not set. Run the USE command before table creation.",
        )));
    }

    let (new_table_name, cmd) = match cmd.split_once("key") {
        Some((new_table_name, cmd)) => (new_table_name.trim(), cmd.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "LET command requires KEY clause.",
            )))
        }
    };

    if db.table_map.get(new_table_name).is_some() {
        return Err(Box::new(DBError::ParseError(
            "Table with name given already exists.",
        )))
    }

    let (key_attri, cmd) = match cmd.split_once("select") {
        Some((key_attri, cmd)) => (key_attri.trim(), cmd.trim()), // TODO trim these
        None => {
            return Err(Box::new(DBError::ParseError(
                "LET command requires SELECT clause.",
            )))
        }
    };

    let mut selected_table = select_from_tables(cmd, db)?;

    // swap attributes so that key is in front and check uniqueness
    selected_table.set_key(key_attri)?;

    let attribute_list = selected_table
        .get_projected_attribute_list()
        .into_iter()
        .map(|tuple|  {tuple.clone()})
        .collect();

    let mut table = Table::build(new_table_name, attribute_list, true, &db.path)?;

    // write projected records to new table
    for rec_num in 0..selected_table.records.len() {
        table.write_record(selected_table.get_projected_record(rec_num))?;
    }

    table.write_record_count()?;

    // balance the bst plus write it to file
    let mut bst_path = table.file_path.clone();
    bst_path.replace_range(table.file_path.len()-3.., "index");
    table.bst.unwrap().write_to_file(&bst_path)?;
    table.bst = Some(BST::read_from_file(&bst_path)?);

    // setup table struct to use its builtin formatting
    db.table_map.insert(
        new_table_name.to_string(),
        table,
    );

    eprintln!("\tLET Success!");
    Ok(())
}

fn run_update(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (table_name, cmd) = match cmd.split_once("set") {
        Some((table_name, cmd)) => (table_name.trim(), cmd.trim()),
        None => return Err(DBError::ParseError("UPDATE directive requires SET clause."))?
    };

    let (new_values, condition) = match cmd.split_once("where") {
        Some((new_values, condition)) => (new_values.trim(), condition.trim()),
        None => (cmd, "")
    };

    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => return Err(DBError::ParseError("Could not find a table with that name to update."))?
    };
    
    let cond = Condition::parse(condition)?;
    let new_values = parse_new_attr_values(&table, new_values)?;
    cond.update(table, new_values)?;
    eprintln!("\tUPDATE Success!");
    Ok(())
}

fn run_delete(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    match cmd.split_once("where") {
        Some((table_name, condition)) => delete_tuples(db, table_name.trim(), condition.trim())?,
        None => delete_table(db, cmd.trim())?
    }
    eprintln!("\tDELETE Success!");
    Ok(())
}

fn run_input(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let (file_name, output_name) = match cmd.split_once("output") {
        Some((file_name, output_name)) => (file_name.trim(), output_name.trim()),
        None => (cmd.trim(), "")
    };

    let mut input = match File::open(file_name) {
        Ok(file) => file,
        Err(_) => Err(DBError::ParseError("Could not find/read the given file for INPUT directive."))?
    };

    let mut output_file = if output_name.is_empty() {
        None
    } else {
        match OpenOptions::new().write(true).truncate(true).open(output_name) {
            Ok(file) => Some(file),
            Err(_) => Some(OpenOptions::new().create(true).write(true).open(output_name)?)
        }
    };

    let mut input_string = String::new();
    input.read_to_string(&mut input_string)?;

    for cmd in input_string.split_terminator(';') {
        let output = run_cmd(cmd.trim_start(), db)?;
        if let Some(ref mut file) = output_file {
            let to_file = output.iter().map(|out| {
                [out.as_bytes(),&[b'\n']].concat()
            }).collect::<Vec<Vec<u8>>>().concat();
            file.write_all(&to_file)?;
        } else {
            for out in output {
                println!("{out}");
            }
        }
    }
    eprintln!("\tINPUT Success!");
    Ok(())
}

fn run_use(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
    *db = Database::build("./".to_owned() + db_name.name() + "/")?;
    eprintln!("\tUSE Success!");
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
    table.write_single_record(record)?;
    eprintln!("\tINSERT Success!");
    Ok(())
}

pub fn run_exit(db: &Database) -> Result<(), Box<dyn Error>> {
    eprintln!("\tSaving Database state");
    // TODO maybe move this logic into a function in Table since it belongs there
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

fn run_describe(cmd: &str, db: &Database) -> Result<Vec<String>, Box<dyn Error>> {
    let table_name = cmd.trim();
    if table_name.is_empty() {
        return Err(Box::new(DBError::ParseError(
            "DESCRIBE requires at least one argument.",
        )));
    }

    let mut output = Vec::new();
    if table_name != "all" {
        let table = match db.table_map.get(table_name) {
            Some(table) => table,
            None => {
                return Err(Box::new(DBError::ParseError(
                    "Table name not found in current database.",
                )))
            }
        };
        output.push(format!("{table_name}"));
        output.append(&mut table.attributes_to_string_vec());
        return Ok(output);
    }
    for (table_name, table) in db.table_map.iter() {
        output.push(format!("{table_name}"));
        output.append(&mut table.attributes_to_string_vec());
    }

    eprintln!("\tDESCRIBE Success!");
    Ok(output)
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
        create_database(cmd)?;
    } else if cmd_0 == "table" {
        create_table(cmd, db)?;
    } else {
        Err(Box::new(DBError::ParseError(
            "Syntax error after directive CREATE.",
        )))?
    }

    eprintln!("\tCREATE Success!");
    Ok(())
}