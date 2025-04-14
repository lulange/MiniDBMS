use crate::base::{Data, Domain, Float, Identifier, Integer, Text};
use crate::relation::Table;
use crate::{binary_search_tree::BST, logic::Condition, CmdIterator, DBError, Database};
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{Read, Write},
};

/// Contains procedures outside of the main commands which are needed for one or more commands
mod helpers;

// helper's functions will only and all be used in this module
use helpers::*;

/// Attempts to parse and run the command given by reading the first keyword and delegating.
/// Returns a result that either contains String outputs passed from the command run or an
/// Err contianing info about why the given command failed.
///
/// # Errors
///
/// Fails anytime the command given is invalid or when the command requires filesystem access
/// but does not have it.
pub fn run_cmd(cmd: &str, db: &mut Database) -> Result<Vec<String>, Box<dyn Error>> {
    let cmd = cmd.to_lowercase(); // lowercase everything so remove case-sensitivity here

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
        "describe" => run_describe(cmd, db), // only the commands which return output can be returned directly.
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
        }
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

/// Attempts to parse and run the SELECT command. Returns a result containing either
/// a successful print out of the selection found or one of many parsing/file Errors
///
/// # Errors
///
/// Fails when the SELECT command string given cannot be parsed or when file system
/// access fails.
fn run_select(cmd: &str, db: &mut Database) -> Result<Vec<String>, Box<dyn Error>> {
    // select processing is offloaded in a helper function since LET command also uses it
    let return_vec = select_from_tables(cmd, db)?.to_string_vec();
    eprintln!("\tSELECT Success!");
    Ok(return_vec)
}

/// Attempts to parse and run the RENAME command. Returns a result indicating either
/// a success or a parsing/file Error
///
/// # Errors
///
/// Fails when cannot parse command or not enough attributes given in the list.
/// Also fails when the file cannot be written to.
fn run_rename(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // split the table_name and the attribute_name
    let cmd = cmd.trim_start();
    let (table_name, attribute_names) = match cmd.split_once(' ') {
        Some((table_name, attribute_names)) => (table_name.trim(), attribute_names.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "RENAME command requires attribute names",
            )))
        }
    };

    // get handle on the Table to rename
    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => {
            return Err(Box::new(DBError::ParseError(
                "No table found with the given name.",
            )))
        }
    };

    // read attributes out of the given list
    let mut new_attributes: Vec<Identifier> = Vec::new();
    for attribute_name in iterate_list(attribute_names)? {
        // iterate list is a helper function for parenthesis surrounded lists
        new_attributes.push(Identifier::from(attribute_name.trim())?);
    }

    // ensure correct number of attributes
    if table.attributes().len() != new_attributes.len() {
        return Err(Box::new(DBError::ConstraintError(
            "Incorrect number of attributes found to RENAME table.",
        )));
    }

    // call Table's builtin rename
    table.rename_attributes(new_attributes)?;
    eprintln!("\tRENAME Success!");
    Ok(())
}

/// Attempts to parse and run the LET command. Returns a result indicating either
/// a success or a parsing/file Error
///
/// # Errors
///
/// Fails when cannot parse command or cannot set new key given.
/// Also fails when the file cannot be written to or the database path is not set.
fn run_let(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // check db.path since table creation requires a valid path
    if db.path == "" {
        return Err(Box::new(DBError::ParseError(
            "Database path not set. Run the USE command before table creation.",
        )));
    }

    // get new_table_name out of the cmd
    let (new_table_name, cmd) = match cmd.split_once(" key ") {
        Some((new_table_name, cmd)) => (new_table_name.trim(), cmd.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "LET command requires KEY clause.",
            )))
        }
    };

    // ensure the table name is unique
    if db.table_map.get(new_table_name).is_some() {
        return Err(Box::new(DBError::ParseError(
            "Table with name given already exists.",
        )));
    }

    // remove the select keyword since this is what run_cmd ordinarily does and the helper function expects
    let (key_attri, cmd) = match cmd.split_once(" select ") {
        Some((key_attri, cmd)) => (key_attri.trim(), cmd.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "LET command requires SELECT clause.",
            )))
        }
    };

    // call the helper function which returns a MemTable
    let selected_table = select_from_tables(cmd, db)?;

    // copy out the projected attributes for new table
    let attribute_list: Vec<(Identifier, Domain)> = selected_table
        .get_projected_attribute_list() // required since projection is implemented with no immediate deletions
        .into_iter()
        .map(|tuple| tuple.clone())
        .collect();

    // look for the new primary key in the projected attributes
    let mut primary_key = None;
    for (i, attribute) in attribute_list.iter().enumerate() {
        if key_attri == attribute.0.name() {
            primary_key = Some(i);
        }
    }

    // allow for none to be specified to express the selection having no key
    if primary_key.is_none() && key_attri != "none" {
        return Err(Box::new(DBError::ParseError(
            "KEY given must be one of the selected attributes.",
        )));
    }

    // create the table - this creates the new table file and the bst file
    let mut table = Table::build(new_table_name, attribute_list, primary_key, &db.path)?;

    // write projected records to new table
    for rec_num in 0..selected_table.records.len() {
        match table.write_record(selected_table.get_projected_record(rec_num)) {
            Ok(()) => (),
            Err(err) => {
                table.clean_up()?; // delete the table if the key specfied has duplicates
                return Err(err);
            }
        };
    }

    // Table.write_record updates record count in memory but not in the file
    table.write_record_count()?;

    if let Some(ref bst) = table.bst {
        // balance the bst plus write it to file
        let mut bst_path = table.file_path.clone();
        bst_path.replace_range(table.file_path.len() - 3.., "index");
        bst.write_to_file(&bst_path)?;
        table.bst = Some(BST::read_from_file(&bst_path)?);
    }

    // insert the new table into the database map
    db.table_map.insert(new_table_name.to_string(), table);

    eprintln!("\tLET Success!");
    Ok(())
}

/// Attempts to parse and run the UPDATE command. Returns a result indicating either
/// a success or a parsing/file Error
///
/// # Errors
///
/// Fails when cannot parse command or when the file cannot be written to.
fn run_update(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // split out table_name
    let (table_name, cmd) = match cmd.split_once(" set ") {
        Some((table_name, cmd)) => (table_name.trim(), cmd.trim()),
        None => return Err(DBError::ParseError("UPDATE directive requires SET clause."))?,
    };

    // split out new_values list
    let (new_values, condition) = match cmd.split_once(" where") {
        Some((new_values, condition)) => {
            if condition.starts_with(' ') || condition.starts_with('(') {
                (new_values.trim(), condition.trim())
            } else {
                Err(DBError::ParseError(
                    "Could not parse clause after SET clause in UPDATE.",
                ))?
            }
        },
        None => (cmd, ""), // empty condition = always true
    };

    // get a handle to the table to update
    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => {
            Err(DBError::ParseError(
                "Could not find a table with that name to update.",
            ))?
        }
    };

    // create a Condition based on the WHERE clause
    let cond = Condition::parse(condition)?;
    // read out the new attribute values
    let new_values = parse_new_attr_values(&table, new_values)?;
    // update the table with the new values - Condition calls Table's update functions after
    // connecting its attribute names to the table's attribute list
    cond.update(table, new_values)?;
    eprintln!("\tUPDATE Success!");
    Ok(())
}

/// Attempts to parse and run the DELETE command. Returns a result indicating either
/// a success or a parsing/file Error
///
/// # Errors
///
/// Fails when cannot parse command or when the file cannot be written to.
fn run_delete(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // if a condition is given the table will be culled from, else delete the whole table
    // uses one of two helper functions for each case
    match cmd.split_once(" where") {
        Some((table_name, condition)) => {
            if condition.starts_with(' ') || condition.starts_with('(') {
                delete_tuples(db, table_name.trim(), condition.trim())?
            } else {
                Err(DBError::ParseError(
                    "Could not parse clause after table name in DELETE.",
                ))?
            }
        },
        None => delete_table(db, cmd.trim())?,
    }
    eprintln!("\tDELETE Success!");
    Ok(())
}

/// Attempts to parse and run the INPUT command. Returns a result indicating either
/// a success or a parsing/file Error.
///
/// # Errors
///
/// Fails when cannot parse command or when any command in the file fails.
/// Also fails when the file cannot be read or when output file cannot be written to.
fn run_input(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // check whether or not there is an output file specified.
    // specifying 'output\s;' will be read as no file specified.
    let (file_name, output_name) = match cmd.split_once(" output ") {
        Some((file_name, output_name)) => (file_name.trim(), output_name.trim()),
        None => (cmd.trim(), ""),
    };

    // open the input file
    let mut input = match File::open(file_name) {
        Ok(file) => file,
        Err(_) => Err(DBError::ParseError(
            "Could not find/read the given file for INPUT directive.",
        ))?,
    };

    // open or create output file
    let mut output_file = if output_name.is_empty() {
        None
    } else {
        Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_name)?,
        )
    };

    // read input file to string
    let mut input_string = String::new();
    input.read_to_string(&mut input_string)?;

    // iterate through all cmds in file... bubble any errors up to main loop
    for cmd in CmdIterator::over(&input_string) {
        let output = run_cmd(cmd.trim_start(), db)?;
        if let Some(ref mut file) = output_file {
            // if output file specified, write to file
            let to_file = output
                .iter()
                .map(|out| [out.as_bytes(), &[b'\n']].concat())
                .collect::<Vec<Vec<u8>>>()
                .concat();
            file.write_all(&to_file)?;
        } else {
            // else println! like normal
            for out in output {
                println!("{out}");
            }
        }
    }
    eprintln!("\tINPUT Success!");
    Ok(())
}

/// Attempts to parse and run the USE command. Returns a result indicating either
/// a success or a parsing/file Error.
///
/// # Errors
///
/// Fails when cannot parse command or when the files in the database fail to read correctly.
fn run_use(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let db_name = Identifier::from(cmd.trim())?; // Once this succeeds we know the database name could be valid
    *db = Database::build("./".to_owned() + db_name.name() + "/")?; // reads all .dat files and any mathing .index files
    eprintln!("\tUSE Success!");
    Ok(())
}

/// Attempts to parse and run the INSERT command. Returns a result indicating either
/// a success or a parsing/file Error.
///
/// # Errors
///
/// Fails when cannot parse command or cannot insert a key given.
/// Also fails when the file cannot be written to.
fn run_insert(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    // split out table_name
    let (table_name, cmd) = match cmd.split_once(" values") {
        Some((table_name, cmd)) => (table_name.trim(), cmd.trim()),
        None => {
            return Err(Box::new(DBError::ParseError(
                "Invalid arguments for INSERT",
            )))
        }
    };

    // get a handle to the Table we want to insert into
    let table = match db.table_map.get_mut(table_name) {
        Some(table) => table,
        None => {
            return Err(Box::new(DBError::ParseError(
                "No table found with the given name.",
            )))
        }
    };

    // iterate the new values out of the remaining cmd string
    let values: Vec<Result<Data, Box<dyn Error>>> = iterate_list(cmd)?
        .enumerate() // get the numbers for each to match in Table
        .map(|(index, value)| -> Result<Data, Box<dyn Error>> {
            let value = value.trim(); // trim the data
            let domain = match table.attributes().get(index) {
                Some(tuple) => tuple.1,
                None => {
                    return Err(Box::new(DBError::ParseError(
                        "Too many attributes in value list to INSERT",
                    )));
                }
            }; // check domain -- this yields an error if values list is too long for table
            match domain {
                // try to parse the datatype expected for each attribute
                Domain::Integer => Ok(Data::Integer(Integer::from(value)?)),
                Domain::Float => Ok(Data::Float(Float::from(value)?)),
                Domain::Text => {
                    if value.starts_with('"') && value.ends_with('"') && value.len() > 1 {
                        // unwrap the double quotes before feeding it to Text
                        Ok(Data::Text(Text::from(&value[1..value.len() - 1].trim())?))
                    // all text values are trimmed before stored
                    } else {
                        return Err(Box::new(DBError::ParseError(
                            "String literal expected. Wrap literals in double quotes.",
                        )));
                    }
                }
            }
        })
        .collect();
    let mut record: Vec<Data> = Vec::with_capacity(values.len());
    for value in values.into_iter() {
        record.push(value?); // unwrap the Results that were not possible in the closure.
                             // this is only neccessary since the '?' op cannot be used at the end of each map closure call
    }

    // once the record is verified and created in memory, try to write it to the table
    table.write_single_record(record)?;
    eprintln!("\tINSERT Success!");
    Ok(())
}

/// Attempts to parse and run the EXIT command. Returns a result to fit with the type system.
/// However, this function calls std::process::exit(0); after a save of values that generally live in memory
///
/// # Errors
///
/// Fails when the files cannot be written to.
pub fn run_exit(db: &Database) -> Result<(), Box<dyn Error>> {
    eprintln!("\tSaving Database state");
    // store the bst and record count for all tables
    for (_, table) in db.table_map.iter() {
        table.write_record_count()?;
        table.write_bst()?;
    }
    eprintln!("\tPROGRAM END");
    std::process::exit(0);
}

/// Attempts to parse and run the DESCRIBE command. Returns a result containing the successful
/// description of the desired table(s) or a parsing error. This function could easily only return DBError, but
/// it is easier to match the other cmd functions so that run_cmd has a certain standard
///
/// # Errors
///
/// Fails when cannot parse command.
fn run_describe(cmd: &str, db: &Database) -> Result<Vec<String>, Box<dyn Error>> {
    let table_name = cmd.trim();
    if table_name.is_empty() {
        return Err(Box::new(DBError::ParseError(
            "DESCRIBE requires at least one argument.",
        )));
    }

    let mut output = Vec::new();
    if table_name != "all" {
        // if you only wanted one table print that one out
        let table = match db.table_map.get(table_name) {
            Some(table) => table,
            None => {
                return Err(Box::new(DBError::ParseError(
                    "Table name not found in current database.",
                )))
            }
        };
        output.push(format!("{}", table_name.to_uppercase()));
        output.append(&mut table.attributes_to_string_vec());
        output.push(String::from(""));
        return Ok(output);
    }

    // otherwise default to print out all the tables
    for (table_name, table) in db.table_map.iter() {
        output.push(format!("{}", table_name.to_uppercase()));
        output.append(&mut table.attributes_to_string_vec());
        output.push(String::from(""));
    }

    eprintln!("\tDESCRIBE Success!");
    Ok(output)
}

/// Attempts to parse and run the CREATE command. Returns a result indicating either
/// a success or a parsing/file Error.
///
/// # Errors
///
/// Fails when cannot parse command or when the file cannot be written to.
fn run_create(cmd: &str, db: &mut Database) -> Result<(), Box<dyn Error>> {
    let cmd = cmd.trim_start();
    // split out first word given in command
    let (cmd_0, cmd) = match cmd.split_once(' ') {
        Some(tuple) => tuple,
        None => {
            return Err(Box::new(DBError::ParseError(
                "CREATE command requires arguments.",
            )))
        }
    };

    // call one of two helpers which handle creation of a database or a table
    if cmd_0 == "database" {
        create_database(cmd)?;
    } else if cmd_0 == "table" {
        create_table(cmd, db)?;
    } else {
        Err(DBError::ParseError("Syntax error after directive CREATE."))?
    }

    eprintln!("\tCREATE Success!");
    Ok(())
}
