use super::base::{Data, Domain, Float, Identifier, Integer, Text};
use super::relation::{MemTable, Table};
use crate::binary_search_tree::BST;
use crate::DBError;
use std::collections::HashMap;
use std::error::Error;
use std::vec;

/// A representation of the AND and OR logical operators
#[derive(Copy, Clone, PartialEq)]
enum LogOp {
    And,
    Or,
}

/// An abstraction over actual constraints and parenthesis enclosed groups of constraints (Conditions)
enum BoolEval {
    Constraint(Constraint),
    Condition(Condition),
}

/// An abstraction over value operands and variable ones that come from tables
/// The Identifier variant is a place holder to be converted to Attribute when a Table is specified
#[derive(Debug)]
pub enum Operand {
    Identifier(Identifier), // an identifier which has not been converted to an Attribute yet
    Attribute((usize, usize)), // coordinates in the joined_record (table, attri_num)
    Value(Data),            // int, float, text (with certain extra restrictions from parsing)
}

impl Operand {
    /// Attempts to parse an operand out of a string reference. Reads integer first so '3' will map to Integer always
    /// and '3.' will map to a Float
    ///
    /// # Errors
    ///
    /// Fails when not an integer, float, or string (double-quoted) value in the string reference.
    fn parse(op: &str) -> Result<Operand, DBError> {
        if op.starts_with('"') && op.ends_with('"') && op.len() > 1 && op.len() < 33 {
            // 0 to 30 characters in stringConst
            return Ok(Operand::Value(Data::Text(
                Text::from(&op[1..op.len() - 1]).unwrap(),
            )));
        }

        if let Ok(int) = op.parse::<i32>() {
            return Ok(Operand::Value(Data::Integer(Integer::wrap(int))));
        }

        if let Ok(float) = op.parse::<f64>() {
            return Ok(Operand::Value(Data::Float(Float::wrap(float)))); // gotta love wrapper types
        }

        if let Ok(identifier) = Identifier::from(op) {
            return Ok(Operand::Identifier(identifier));
        }

        return Err(DBError::ParseError("Could not parse Operand."));
    }
}

/// Represents the left, right, and relative operator for any constraint.
/// In a string this looks like 'attribute_name > 5'.
pub struct Constraint {
    pub left_op: Operand,
    pub rel_op: RelOp,
    pub right_op: Operand,
}

impl Constraint {
    /// Attempts to read a Constraint out of the front of a string reference.
    /// Returns a result with the constraint read and the rest of the string if Ok variant.
    ///
    /// # Errors
    ///
    /// Fails when a full Constraint cannot be read out of the string.
    pub fn parse_split(prop: &str) -> Result<(Self, &str), Box<dyn Error>> {
        let prop = prop.trim();

        let (left_op, prop) = split_word(prop);
        let prop = prop.trim_start();
        let (rel_op, prop) = split_rel_op(prop)?;
        let prop = prop.trim_start();
        let (right_op, prop) = split_word(prop);

        let left_op = Operand::parse(left_op)?;
        let right_op = Operand::parse(right_op)?;

        Ok((
            Constraint {
                left_op,
                rel_op,
                right_op,
            },
            prop,
        ))
    }

    /// Evaluates a Constraint after conversion with a specific table. Returns
    /// true if the condition is true for the given joined_record.
    /// Note this is a joined record since a condition is made to work
    /// over a cartesian product of multiple tables.
    ///
    /// # Panics
    ///
    /// Panics when two incompatible data types are asked to be compared.
    fn eval(&self, joined_record: &Vec<&Vec<Data>>) -> bool {
        let left_data = match self.left_op {
            Operand::Value(ref data) => data,
            Operand::Attribute((table, attri)) => &joined_record[table][attri],
            _ => panic!(
                "Can't evaluate constraint before converting identifier operands to attributes."
            ),
        };

        let right_data = match self.right_op {
            Operand::Value(ref data) => data,
            Operand::Attribute((table, attri)) => &joined_record[table][attri],
            _ => panic!(
                "Can't evaluate constraint before converting identifier operands to attributes."
            ),
        };

        match (left_data, right_data) {
            (Data::Float(f1), Data::Float(f2)) => self.rel_op.cmp(f1.value(), f2.value()),
            (Data::Integer(i1), Data::Integer(i2)) => self.rel_op.cmp(i1.value(), i2.value()),
            (Data::Text(t1), Data::Text(t2)) => self.rel_op.cmp(t1.content(), t2.content()),
            _ => panic!("Incompatible data types cannot be compared"), // Errors of this type should be found during the parsing of commands
        }
    }

    /// Attempts to convert this Constraint so that any Identifier variant Operands change to
    /// Attribute Operands. Returns a result to indicate success or bubble the Error.
    ///
    /// # Errors
    ///
    /// Fails if an Operand cannot change types.
    pub fn convert_with(&mut self, tables: &Vec<&Table>) -> Result<(), Box<dyn Error>> {
        'outer: {
            if let Operand::Identifier(id) = &self.left_op {
                for (i, table) in tables.iter().enumerate() {
                    for (j, (attribute, _)) in table.attributes().iter().enumerate() {
                        if attribute.name() == id.name() {
                            self.left_op = Operand::Attribute((i, j));
                            break 'outer;
                        }
                    }
                }
                return Err(Box::new(DBError::ConstraintError("Could not find an attribute in the table with the name given in the Condition.")));
            }
        }

        'outer: {
            if let Operand::Identifier(id) = &self.right_op {
                for (i, table) in tables.iter().enumerate() {
                    for (j, (attribute, _)) in table.attributes().iter().enumerate() {
                        if attribute.name() == id.name() {
                            self.right_op = Operand::Attribute((i, j));
                            break 'outer;
                        }
                    }
                }
                return Err(Box::new(DBError::ConstraintError("Could not find an attribute in the table with the name given in the Condition.")));
            }
        }

        match (&self.left_op, &self.right_op) {
            (Operand::Attribute((i1,j1)), Operand::Attribute((i2, j2))) => {
                if tables[*i1].attributes()[*j1].1 != tables[*i2].attributes()[*j2].1 {
                    return Err(Box::new(DBError::ConstraintError("Attributes with incompatible Domains cannot be compared.")))
                }
                Ok(())
            }
            (Operand::Attribute((i, j)), Operand::Value(value))|
            (Operand::Value(value), Operand::Attribute((i, j))) => {
                let value_domain = match value {
                    Data::Float(_) => Domain::Float,
                    Data::Integer(_) => Domain::Integer,
                    Data::Text(_) => Domain::Text
                };
                if tables[*i].attributes()[*j].1 != value_domain {
                    Err(DBError::ParseError("Attribute compared with value from incorrect domain."))?
                }
                Ok(())
            }
            _ => Err(Box::new(DBError::ParseError("Comparisons between two constants are not allowed as they are either always true or always false.")))
        }
    }

    /// Returns a usize value which represents which table in the list of tables converted with
    /// that this Constraint references if and only if this Constraint only references that Table
    fn refs_single_table(&self) -> Option<usize> {
        match (&self.left_op, &self.right_op) {
            (Operand::Attribute((i1, _)), Operand::Attribute((i2, _))) => {
                if i1 == i2 {
                    Some(*i1)
                } else {
                    None
                }
            }
            (Operand::Attribute((i, _)), _) => Some(*i),
            (_, Operand::Attribute((i, _))) => Some(*i),
            _ => panic!("Should never call refs_single_table method before converting constraint."),
        }
    }

    /// Returns the key value this Constraint contains if it is a key constraint in
    /// the form 'key_attri = value'
    fn get_key(&self, tables: &Vec<&Table>) -> Option<Data> {
        if let Constraint {
            left_op: Operand::Attribute((i, j)),
            rel_op: RelOp::Equals,
            right_op: Operand::Value(data),
        } = self
        {
            if tables[*i].key_attri_num == Some(*j) {
                return Some(data.clone());
            }
        } else if let Constraint {
            left_op: Operand::Value(data),
            rel_op: RelOp::Equals,
            right_op: Operand::Attribute((i, j)),
        } = self
        {
            if tables[*i].key_attri_num == Some(*j) {
                return Some(data.clone());
            }
        }

        None
    }
}

/// An wrapper for a list of alternating LogOps and BoolEvals. Also, contains all the main methods
/// useful for selecting, updating, and deleting.
pub struct Condition {
    bool_evals: Vec<(LogOp, BoolEval)>, // first LogOp is a placeholder that should always be AND
}

impl Condition {
    /// Attempts to read a Condition out of a string reference.
    ///
    /// # Errors
    ///
    /// Fails if a Constraint is missing operands or if
    /// there is no valid Logical Operator between Constraints/Conditions
    pub fn parse(mut cond: &str) -> Result<Self, Box<dyn Error>> {
        let mut bool_evals: Vec<(LogOp, BoolEval)> = Vec::new();

        // used for when where clause is omitted
        if cond.is_empty() {
            return Ok(Condition { bool_evals });
        }

        // default value depended on later when checking if things are possible to load a table based on them
        let mut last_log_op = LogOp::And;

        loop {
            cond = cond.trim_start();
            // try reading off a chunk in parenthesis - Condition
            if let Ok((chunk, short_cond)) = split_parenthesis_chunk(cond) {
                cond = short_cond; // short_cond was the remaining piece of cond
                bool_evals.push((
                    last_log_op,
                    BoolEval::Condition(
                        Condition::parse(chunk)?, // recursively parse anything in parenthesis
                    ),
                ));
            // try reading off a Constraint
            } else if let Ok((constraint, short_cond)) = Constraint::parse_split(cond) {
                cond = short_cond;
                bool_evals.push((last_log_op, BoolEval::Constraint(constraint)));
            } else {
                Err(DBError::ParseError(
                    "Did not find valid constraint or parenthesis chunk.",
                ))?
            }

            cond = cond.trim_start();

            let log_op;
            (log_op, cond) = split_word(cond);

            last_log_op = if log_op == "and" {
                LogOp::And
            } else if log_op == "or" {
                LogOp::Or
            } else {
                cond = cond.trim();
                if cond.is_empty() {
                    break;
                } else {
                    Err(DBError::ParseError("Did not find valid logical operator."))?
                }
            }
        }

        Ok(Condition { bool_evals })
    }

    /// Attempts to convert this Condition so that any Identifier variant Operands in its Constraints change to
    /// Attribute Operands. Returns a result to indicate success or bubble the Error.
    ///
    /// # Errors
    ///
    /// Fails if an Operand cannot change types.
    fn convert_with(&mut self, tables: &Vec<&Table>) -> Result<(), Box<dyn Error>> {
        for (_, bool_eval) in self.bool_evals.iter_mut() {
            match bool_eval {
                BoolEval::Condition(cond) => cond.convert_with(tables)?,
                BoolEval::Constraint(constraint) => constraint.convert_with(tables)?,
            }
        }

        Ok(())
    }

    /// Removes parts of the Condition that are guaranteed to only apply to one Table and
    /// fills the given hashmap with a Condition for each table that it can so that
    /// each Condition only relates to the table with the usize value that cooresponds to it.
    /// Returns an optional usize which is used for recursive purposes - the Option<usize> keeps
    /// track of whether or not the sub-Condition was all only related to a single table (Option)
    /// and what table that was (usize). The always_true option should always be initiated with a
    /// value of true.
    fn split_load_helpers(
        &mut self,
        helpers: &mut HashMap<usize, Condition>,
        mut always_true: bool,
    ) -> Option<usize> {
        // set always_true based on if there are any Or's present. If not, then
        // any sub-section can be split out of the condition and evaluated separately.
        for (log_op, _) in self.bool_evals.iter() {
            if *log_op == LogOp::Or {
                always_true = false;
            }
        }

        // parts of the Condition that can be removed must both be always
        // true for records in the selection and only related to one table
        let mut single_table = true;
        let mut last_table = None;

        // use a while loop so that removal from the bool_evals list can be adjusted for
        let mut i = 0;
        while i < self.bool_evals.len() {
            match self.bool_evals[i].1 {
                BoolEval::Condition(ref mut cond) => {
                    if let Some(table_num) = cond.split_load_helpers(helpers, always_true) {
                        if always_true {
                            // swap_remove is allowed since order does not matter with And connected Conditions
                            let bool_eval = self.bool_evals.swap_remove(i);
                            match helpers.get_mut(&table_num) {
                                Some(condition) => condition.bool_evals.push(bool_eval),
                                None => {
                                    helpers.insert(
                                        table_num,
                                        Condition {
                                            bool_evals: vec![bool_eval],
                                        },
                                    );
                                }
                            }
                            continue;
                        }
                    }
                }
                BoolEval::Constraint(ref mut constraint) => {
                    if let Some(table_num) = constraint.refs_single_table() {
                        if let Some(other_num) = last_table {
                            if table_num != other_num {
                                single_table = false;
                            }
                        }
                        last_table = Some(table_num);
                        if always_true {
                            let bool_eval = self.bool_evals.swap_remove(i);
                            match helpers.get_mut(&table_num) {
                                Some(condition) => condition.bool_evals.push(bool_eval),
                                None => {
                                    helpers.insert(
                                        table_num,
                                        Condition {
                                            bool_evals: vec![bool_eval],
                                        },
                                    );
                                }
                            }
                            continue;
                        }
                    }
                }
            }
            i += 1;
        }

        if single_table {
            last_table
        } else {
            None
        }
    }

    /// Returns a list of the record_nums in BST order or uses the BST to search for a key if the
    /// Condition requires it only be one. Note that this should only be used on Conditions
    /// known to only relate to one Table - for instance those from split_load_helpers().
    ///
    /// # Panics
    ///
    /// Could panic if called on a Condition that references multiple tables or
    /// a table that does not own the given bst.
    fn get_record_nums_from_bst(&mut self, bst: &BST, tables: &Vec<&Table>) -> Vec<usize> {
        for (log_op, _) in self.bool_evals.iter() {
            if *log_op == LogOp::Or {
                return bst.get_data(); // all are possible
            }
        }

        // try to get a single key out of the Condition
        let mut key: Option<Data> = None;

        // use a while loop since we want to remove the key constraint and be able to adjust for that
        let mut i = 0;
        while i < self.bool_evals.len() {
            let constraint = match &self.bool_evals[i].1 {
                BoolEval::Condition(_) => {
                    i += 1;
                    continue;
                } // ignore Conditions - don't worry about this recursively.
                BoolEval::Constraint(constraint) => constraint,
            };
            let new_key = match constraint.get_key(tables) {
                None => {
                    i += 1;
                    continue;
                } // ignore non-key constraints
                Some(new_key) => new_key,
            };
            // remove key constraints
            self.bool_evals.swap_remove(i);
            // set key based on the existence of one already and new_key
            match &key {
                Some(key) => {
                    if *key == new_key {
                        continue;
                    } else {
                        return vec![];
                    }
                }
                None => key = Some(new_key),
            }
        }

        match &key {
            Some(key) => match bst.find(key) {
                Some(rec_num) => vec![*rec_num],
                None => vec![],
            },
            None => bst.get_data(), // if can't get a key, then at least use BST order
        }
    }

    /// Returns a filtered list of valid table coordinates based on the given Condition.
    /// Note this should only be used on Conditions known to only relate to one Table.
    ///
    /// # Panics
    ///
    /// Could panic if given a Condition that relates to more than one table or a condition
    /// that relates to a table other than the one indicated by bst and table_num
    pub fn filter_table_coords(
        mut self,
        mem_tables: &Vec<MemTable>,
        table_num: usize,
        bst: &Option<BST>,
        tables: &Vec<&Table>,
    ) -> Vec<usize> {
        let mut selected: Vec<usize> = Vec::with_capacity(mem_tables[table_num].records.len());

        let records_coords: Vec<usize> = match bst {
            Some(bst) => self.get_record_nums_from_bst(bst, tables),
            None => (0..mem_tables[table_num].records.len()).collect(),
        };

        let binding = vec![]; // this is not the most efficient way to go about doing this for a single table but it works
        let mut joined_record: Vec<&Vec<Data>> = vec![&binding; mem_tables.len()];
        for coord in records_coords.into_iter() {
            joined_record[table_num] = &mem_tables[table_num].records[coord];
            vec![&mem_tables[table_num].records[coord]];
            if self.eval(&joined_record) {
                selected.push(coord);
            }
        }

        selected
    }

    /// Returns a filtered list of valid coordinates based on the given Condition. Note that
    /// this should only be called with the tables used when converting this Condition.
    ///
    /// # Panics
    ///
    /// Could panic if the wrong reference is given for tables so that table_coords do not match up
    /// to the actual attribute list lengths.
    fn eval_coords(
        &self,
        table_coords: Vec<Vec<usize>>,
        tables: &Vec<MemTable>,
    ) -> Vec<Vec<usize>> {
        let mut selected: Vec<Vec<usize>> = Vec::with_capacity(table_coords.len());

        for coord in table_coords {
            let mut joined_record = Vec::new();
            for (table_num, record_num) in coord.iter().enumerate() {
                joined_record.push(&tables[table_num].records[*record_num]);
            }
            if self.eval(&joined_record) {
                selected.push(coord);
            }
        }

        selected
    }

    /// Returns how this Condition evaluated on the given joined_record. Requires the condition
    /// to have been converted.
    fn eval(&self, joined_record: &Vec<&Vec<Data>>) -> bool {
        let mut curr_evaluation = true;

        for (log_op, bool_eval) in self.bool_evals.iter() {
            match (log_op, bool_eval) {
                (LogOp::And, BoolEval::Condition(cond)) => {
                    if curr_evaluation == false {
                        continue;
                    }
                    if !cond.eval(joined_record) {
                        curr_evaluation = false;
                    }
                }
                (LogOp::Or, BoolEval::Condition(cond)) => {
                    if curr_evaluation == true {
                        return true;
                    } else {
                        curr_evaluation = cond.eval(joined_record);
                    }
                }
                (LogOp::And, BoolEval::Constraint(constraint)) => {
                    if curr_evaluation == false {
                        continue;
                    }
                    if !constraint.eval(joined_record) {
                        curr_evaluation = false;
                    }
                }
                (LogOp::Or, BoolEval::Constraint(constraint)) => {
                    if curr_evaluation == true {
                        return true;
                    } else {
                        curr_evaluation = constraint.eval(joined_record);
                    }
                }
            }
        }

        curr_evaluation
    }

    /// Returns a MemTable which represents the selection out of the cartesian product of the Tables reffered to by tables.
    /// This does not require you to convert the Condition first since it will attempt that first.
    ///
    /// # Errors
    ///
    /// Fails when cannot convert Constraints or when cannot read tables into memory.
    pub fn select(mut self, tables: Vec<&Table>) -> Result<MemTable, Box<dyn Error>> {
        if tables.len() == 0 {
            Err(DBError::ConstraintError("Must select from a table."))?
        }
        // Replace all attributes in bool_evals list with table coordinates
        self.convert_with(&tables)?;

        // Get all single_table and Always true things as a separate condition
        let mut helpers = HashMap::new();
        self.split_load_helpers(&mut helpers, true);

        // load MemTables
        let mut mem_tables = Vec::new();
        let mut new_attributes = Vec::new();
        for table in tables.iter() {
            let mem_table = MemTable::build(table)?;
            new_attributes.push(mem_table.attributes.clone());
            mem_tables.push(mem_table);
        }

        let new_attributes = new_attributes.concat();

        // Filter memtables by single_table/always_trues condition (get coords for each table that match)
        let mut record_nums_vec = Vec::new();
        for (i, _) in mem_tables.iter().enumerate() {
            record_nums_vec.push(match helpers.remove(&i) {
                Some(helper) => helper.filter_table_coords(&mem_tables, i, &tables[i].bst, &tables),
                None => Condition::parse("")?.filter_table_coords(
                    &mem_tables,
                    i,
                    &tables[i].bst,
                    &tables,
                ),
            });
        }

        for record_nums in record_nums_vec.iter() {
            if record_nums.len() == 0 {
                return Ok(MemTable::build_from_records(vec![], new_attributes)?);
            }
        }

        let mut table_coords = Vec::new();

        // count up
        let mut cart_prod_key: Vec<usize> = vec![0; mem_tables.len()];
        let mut cart_prod_elem: Vec<usize> = vec![0; mem_tables.len()];

        'outer: loop {
            for (i, key) in cart_prod_key.iter().enumerate() {
                cart_prod_elem[i] = record_nums_vec[i][*key];
            }

            table_coords.push(cart_prod_elem.clone());

            // increment cart_prod_key
            for (i, key) in cart_prod_key.iter_mut().enumerate().rev() {
                *key += 1;
                if *key == record_nums_vec[i].len() {
                    if i == 0 {
                        break 'outer;
                    } else {
                        *key = 0;
                    }
                } else {
                    break;
                }
            }
        }

        let selected = self.eval_coords(table_coords, &mem_tables);

        let mut records = Vec::with_capacity(selected.len());

        for table_coord in selected {
            let mut new_rec = Vec::new();
            for (i, rec_num) in table_coord.iter().enumerate() {
                for data in mem_tables[i].records[*rec_num].iter() {
                    new_rec.push(data.clone());
                }
            }
            records.push(new_rec);
        }

        Ok(MemTable::build_from_records(records, new_attributes)?)
    }

    /// Will update every record in the table that matches the given Condition.
    /// Returns a result which indicates whether or not the table you passed in was updated properly.
    /// This does not require you to convert the Condition first since it will attempt that first.
    ///
    /// # Errors
    ///
    /// Fails when cannot convert Constraints or when cannot read/write tables into memory.
    pub fn update(
        mut self,
        table: &mut Table,
        new_values: Vec<(Identifier, Data)>,
    ) -> Result<(), Box<dyn Error>> {
        self.convert_with(&vec![table])?;
        table.update_all(self, new_values)
    }

    /// Will delete every record in the table that matches the given Condition.
    /// Returns a result which indicates whether or not the table you passed in was deleted from properly.
    /// This does not require you to convert the Condition first since it will attempt that first.
    ///
    /// # Errors
    ///
    /// Fails when cannot convert Constraints or when cannot read/write tables into memory.
    pub fn delete(mut self, table: &mut Table) -> Result<(), Box<dyn Error>> {
        self.convert_with(&vec![table])?;
        table.delete_all(self)
    }
}

/// An abstraction over Relative operators with a generalized .cmp method
#[derive(PartialEq)]
pub enum RelOp {
    Equals,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl RelOp {
    /// Takes in the RelOp and compares any items which implement the PartialEq and PartialOrd traits.
    /// Returns whether or not the RelOp is true for those two items where val1 is the left operator and val2 is the right.
    fn cmp<T: PartialEq + PartialOrd>(&self, val1: T, val2: T) -> bool {
        match self {
            RelOp::Equals => val1 == val2,
            RelOp::GreaterThan => val1 > val2,
            RelOp::LessThan => val1 < val2,
            RelOp::GreaterThanOrEqual => val1 >= val2,
            RelOp::LessThanOrEqual => val1 <= val2,
            RelOp::NotEqual => val1 != val2,
        }
    }
}

/// Splits off a specified set of characters (mostly ascii-alphanumeric) until it sees a character not in the set.
/// Returns the split up strings as a tuple.
fn split_word(given: &str) -> (&str, &str) {
    for (i, c) in given.char_indices() {
        if !c.is_ascii_alphanumeric() && c != '"' && c != '.' && c != '-' {
            return (&given[..i], &given[i..]);
        }
    }

    return (&given, "");
}

/// Splits off the characters which represent a RelOp and returns the RelOp paired with the rest of the string
/// or an error if it does not find a RelOp.
///
/// # Errors
///
/// Fails when there is no RelOp to read
fn split_rel_op(cond: &str) -> Result<(RelOp, &str), DBError> {
    let (char1, char2);
    if cond.len() < 2 {
        char1 = &cond[..];
        char2 = "";
    } else {
        char1 = &cond[..1];
        char2 = &cond[1..2];
    }

    match (char1, char2) {
        (">", "=") => Ok((RelOp::GreaterThanOrEqual, &cond[2..])),
        ("<", "=") => Ok((RelOp::LessThanOrEqual, &cond[2..])),
        ("!", "=") => Ok((RelOp::NotEqual, &cond[2..])),
        ("=", _) => Ok((RelOp::Equals, &cond[1..])),
        (">", _) => Ok((RelOp::GreaterThan, &cond[1..])),
        ("<", _) => Ok((RelOp::LessThan, &cond[1..])),
        _ => Err(DBError::ParseError("Did not find a valid RelOp.")),
    }
}

/// Splits off the a parenthesis surrounded chunk if possible. Returns an Err variant if it cannot find one
/// at the start of the string given. Returns a wrapped tuple of the split string otherwise.
///
/// # Errors
///
/// Fails when there is no parenthesis chunk
fn split_parenthesis_chunk(cond: &str) -> Result<(&str, &str), ()> {
    if !cond.starts_with('(') {
        return Err(());
    }

    let mut open_count = 0;
    for (i, c) in cond.char_indices() {
        if c == '(' {
            open_count += 1;
        } else if c == ')' {
            open_count -= 1;
        }
        if open_count == 0 {
            return Ok((&cond[1..i], &cond[i + 1..])); // remove open and closing of this chunk through selective indicies
        }
    }

    Err(())
}
