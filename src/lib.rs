use relation::Table;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::fs;

// public exports for the only commands neccessary to run all commands
pub use db_cmds::run_cmd;
pub use db_cmds::run_exit;
// bring all error types into the same scope
pub use binary_search_tree::BSTInsertErr;

/// Contains base types which help with data standardization
mod base;
/// Contains a Binary Search Tree that uses Data as a key and usize as a data payload
mod binary_search_tree;
/// Contains functions for all semantic database commands
mod db_cmds;
/// Contains code to aid with selecting/updating/deleting from tables
mod logic;
/// Contains Table and MemTable structs which abstract over interactions with database tables/relations
mod relation;

/// A master reference to the current database the program is working with
pub struct Database {
    /// The path to the Database can only be set if path is valid
    path: String,
    /// Loaded from all .dat files in the db directory
    table_map: HashMap<String, Table>,
}

impl Database {
    /// Creates a new Database with empty path and table_map values.
    pub fn new() -> Self {
        Database {
            path: String::new(),
            table_map: HashMap::new(),
        }
    }

    /// Creates a new Database with the given path and attempts to load
    /// all .dat files in as tables.
    ///
    /// # Errors
    ///
    /// Will return an error if
    /// - The path given was invalid
    /// - The files in the database failed to load
    pub fn build(path: String) -> Result<Self, Box<dyn Error>> {
        let mut table_map = HashMap::new();

        let db_files = match fs::read_dir(&path) {
            Ok(read_dir) => read_dir,
            Err(_) => {
                return Err(Box::new(DBError::ParseError(
                    "Failed to read files in database directory.",
                )))
            }
        };

        for file in db_files {
            let file = file?;
            let file_name = String::from(
                file.file_name()
                    .to_str()
                    .expect("File name cannnot use non-ascii characters."),
            );

            let file_name_split = file_name
                .rsplit_once(".")
                .expect("File name should have a dot separated identifier.");

            // if file is a table file
            if let (table_name, "dat") = file_name_split {
                table_map.insert(
                    String::from(table_name),
                    Table::read_from_file(table_name, &path)?, // table will check for a matching .index file
                );
            }
        }

        Ok(Database { path, table_map })
    }
}

/// General program erros which do not require a shut down
#[derive(Debug)]
pub enum DBError {
    /// Most common DBError variant since this is a blanket for all invalid syntax
    ParseError(&'static str),
    /// Constraint errors most commonly come from mismatching domains in Conditions
    ConstraintError(&'static str),
    /// Rarely used since most file related errors are treated as full-stop issues
    FileFormatError(&'static str),
}

impl Display for DBError {
    /// Adds some default prefixes to DBErrors
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            DBError::ParseError(s) => write!(f, "Failed to parse a line: {s}"),
            DBError::ConstraintError(s) => write!(f, "Invalid value given for a type: {s}"),
            DBError::FileFormatError(s) => write!(f, "Incorrect file format: {s}"),
        }
    }
}

// semantic extension of Display Trait
impl Error for DBError {}

/// An Iterator that returns semicolon terminated statements in a &str.
/// Allows for comments that start with '#' and end with '\n'.
pub struct CmdIterator<'a> {
    pos: usize,
    text: &'a str,
    cmd: String,
}

impl<'a> CmdIterator<'a> {
    /// Creates a CmdIterator with a lifetime connected to the given str reference.
    /// Iterator ignores comments starting with '#' and ending with '\n'.
    /// Iterator also ignores semicolon terminators inside a double quoted sub-string.
    /// Does not trim whitespace from returned Strings.
    pub fn over(text: &'a str) -> Self {
        CmdIterator {
            pos: 0,
            text,
            cmd: String::from(""),
        }
    }
}

impl<'a> Iterator for CmdIterator<'a> {
    type Item = String; // returns String instead of &str since it needs to concatenate &str's that exclude comments

    /// Returns the next semicolon terminated String found in self.text.
    /// Iterator ignores comments starting with '#' and ending with '\n'.
    /// Iterator also ignores semicolon terminators inside a double quoted sub-string.
    /// Does not trim whitespace from returned Strings.
    ///
    /// # Examples
    ///
    /// ```
    /// use dbms::CmdIterator;
    /// let mut cmd_iter = CmdIterator::over("one \" ; \"; two # comment \n; three;");
    /// assert_eq!(cmd_iter.next(), Some(String::from("one \" ; \"")));
    /// assert_eq!(cmd_iter.next(), Some(String::from(" two \n")));
    /// assert_eq!(cmd_iter.next(), Some(String::from(" three")));
    /// assert_eq!(cmd_iter.next(), None);
    /// ```
    fn next(&mut self) -> Option<Self::Item> {
        let mut double_quotes = false; // is true when waiting for an end double quote
        let mut comment = false; // is true when waiting for the end of a comment
        let range = &self.text[self.pos..];
        let mut since_last_push = 0; // number of chars traveled in self.text since last push to self.cmd
        for c in range.chars() {
            // start a comment
            if c == '#' && !double_quotes && !comment {
                comment = true;
                self.cmd
                    .push_str(&self.text[self.pos..self.pos + since_last_push]);
            // end a comment
            } else if c == '\n' && comment {
                comment = false;
                self.pos += since_last_push;
                since_last_push = 0;
            // start/end a quote
            } else if c == '"' && !comment {
                double_quotes = !double_quotes;
            // end a command
            } else if c == ';' && !double_quotes && !comment {
                self.cmd.push_str(
                    &self.text[self.pos..self.pos + since_last_push] // replacements are necessary for parsing ease without Regex
                        .replace('\r', " ") // for windows -- this and next line can be combined if guaranteed on windows
                        .replace('\n', " "), // for mac and windows
                );
                self.pos += since_last_push + 1; // +1 to ignore the semicolon
                let cmd = std::mem::take(&mut self.cmd);
                return Some(cmd);
            }
            since_last_push += 1; // not equivalent to a count of the loop since is set to 0 sometimes
        }

        None // no more chars in self.text past self.pos
    }
}
