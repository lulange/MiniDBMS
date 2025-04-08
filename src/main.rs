use dbms::{DBError, Database};
use std::io;
use std::io::Write;
use dbms::binary_search_tree::BSTError;

// TODO get rid of unnecessary pub keywords
fn main() {
    // a mutable reference will get passed around and treated like a singleton
    let mut db = Database::new();

    let stdin: io::Stdin = io::stdin();
    let mut cmd = String::new();
    loop {
        print!("dbms> ");
        io::stdout().flush().expect("Failed to write to output.");
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(_) => (),
            Err(_) => {
                eprintln!("Failed to read input line.");
                std::process::exit(1);
            }
        }
        cmd.push_str(&line);
        if !cmd.contains(';') {
            continue;
        }

        for cmd in cmd.split_terminator(';') {
            match dbms::run_cmd(cmd.trim_start(), &mut db) {
                Err(err) => {
                    eprintln!("\t{}", err);
                    if err.is::<DBError>() || err.is::<BSTError>() {
                        continue;
                    }
                    
                    dbms::run_exit(&mut db).expect("FAILED TO SAVE ON EXIT");
                }
                Ok(output) => {
                    for out in output {
                        println!("{out}");
                    }
                }
            }
        }
        cmd.clear();
    }
}




// BIG TODOS
/*
1. Complete base todos written throughout the code so that the program is complete in theory
3. Comment all functions, modules, structs, etc to some specific format
4. Create tests to ensure all logic works correctly / maybe split some functions for testing
5. Last pass, check over everything line by line and do real-world testing
6. Come up with some cool name for it and run it by the professor and ask for feedback
7. Fix stuff for him and turn it in. Count up lines of code for programmer clout
*/
