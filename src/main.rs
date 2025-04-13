use dbms::BSTInsertErr;
use dbms::{CmdIterator, DBError, Database};
use std::io;
use std::io::Write;

/// Program entry point. Sets up a Database instance and begins input loop.
fn main() {
    // a mutable reference to this Database will get passed around and treated like a singleton
    // this is easier than trying to maintain some global state in this case
    let mut db = Database::new();

    let stdin: io::Stdin = io::stdin();
    let mut cmds = String::new();
    let mut cmd_run = false; // used to detect whether at least one command was run in an iteration of the 'read loop

    'read: loop {
        // prompt the user
        print!("dbms> ");
        io::stdout().flush().expect("Failed to write to output."); // necessary to output immediately with print!()
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(_) => (),
            Err(_) => {
                eprintln!("Failed to read input line.");
                std::process::exit(1);
            }
        }
        cmds.push_str(&line); // always ends with '\n' so comments will be valid

        // iterate over all commands currently in cmds
        for cmd in CmdIterator::over(&cmds) {
            match dbms::run_cmd(cmd.trim_start(), &mut db) {
                Err(err) => {
                    eprintln!("\t{}", err);
                    // these error types are acceptable and come with more helpful messages for the user
                    if err.is::<DBError>() || err.is::<BSTInsertErr>() {
                        cmds.clear();
                        continue 'read;
                    }

                    // save table sizes and bst indices since those are otherwise kept in memory
                    dbms::run_exit(&mut db).expect("FAILED TO SAVE ON EXIT");
                }

                Ok(output) => {
                    // println!() is here instead of in internal functions so that output can be redirected on INPUT command
                    for out in output {
                        println!("{out}");
                    }
                    cmd_run = true;
                }
            }
        }

        // only clear if a command has run since sometimes commands will be half-typed and not run
        if cmd_run {
            cmds.clear();
            cmd_run = false;
        }
    }
}
