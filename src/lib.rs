use binary_search_tree::BST;
use db_cmds::db_types::Table;
use std::collections::HashMap;
use std::io;

pub mod binary_search_tree;
pub mod db_cmds;

pub struct Database {
    pub path: String,
    pub bst_map: HashMap<String, BST>,
    pub table_map: HashMap<String, Table>,
}

pub fn run() -> Result<(), String> {
    // a mutable reference will get passed around and treated like a singleton
    let mut db = Database {
        path: String::new(),
        bst_map: HashMap::new(),
        table_map: HashMap::new(),
    };

    let stdin: io::Stdin = io::stdin();
    loop {
        let mut line = String::new();
        stdin
            .read_line(&mut line)
            .expect("Unable to read from console input.");
        match execute_cmds_from(line, &mut db) {
            Ok(()) => (), // program exits with an EXIT command that calls 'std::process::exit(0)'
            Err(why) => return Err(why),
        }
    }
}

fn execute_cmds_from(cmds: String, db: &mut Database) -> Result<(), String> {
    for cmd in cmds.split_terminator('\n') {
        if let Err(e) = db_cmds::execute(cmd, db) {
            return Err(e);
        }
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
