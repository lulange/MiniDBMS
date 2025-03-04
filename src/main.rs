use dbms::run;

fn main() {
    if let Err(why) = run() {
        println!("{why}");
    };
}

// BIG TODOS
/*
1. Complete base todos written throughout the code so that the program is complete in theory
2. Change error handling to return a custom error type and implement all error handling logic
3. Comment all functions, modules, structs, etc to some specific format
4. Create tests to ensure all logic works correctly / maybe split some functions for testing
5. Last pass, check over everything line by line and do real-world testing
6. Come up with some cool name for it and run it by the professor and ask for feedback
7. Fix stuff for him and turn it in. Count up lines of code for programmer clout
*/
