use std::io;
use dbms::Database;

// TODO remove case-sensitivity for all text processing...
// TODO make this wait till a semicolon is entered to execute a command... implement multi-line commands
// TODO replace println! stuff with result<String> so that I can print out at top level or pipe to a file at top level
fn main() {
    // a mutable reference will get passed around and treated like a singleton
    let mut db = Database::new();

    let stdin: io::Stdin = io::stdin();
    loop {
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(_) => (),
            Err(_) => {
                eprintln!("Failed to read input line.");
                std::process::exit(1);
            }
        }
        for cmd in line.split_terminator(';') {
            if let Err(err) = dbms::run_cmd(cmd, &mut db) {
                eprintln!("{}", err);
                dbms::run_exit(&mut db).expect("FAILED TO SAVE ON EXIT");
            }
        }
    }
}

// BIG TODOS
/*
1. Complete base todos written throughout the code so that the program is complete in theory
2. Change error handling to return a custom error type and implement all error handling logic fully
3. Comment all functions, modules, structs, etc to some specific format
4. Create tests to ensure all logic works correctly / maybe split some functions for testing
5. Last pass, check over everything line by line and do real-world testing
6. Come up with some cool name for it and run it by the professor and ask for feedback
7. Fix stuff for him and turn it in. Count up lines of code for programmer clout
*/
