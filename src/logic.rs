use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::vec;
use super::base::{Float, Integer, Text, Identifier, Data, Domain};
use super::relation::{Table, MemTable};
use crate::DBError;
use crate::binary_search_tree::BST;

#[derive(Copy, Clone, PartialEq)]
enum LogOp {
    And,
    Or
}

enum BoolEval {
    Constraint(Constraint),
    Condition(Condition)
}

#[derive(Debug)]
enum Operand {
    Identifier(Identifier), // an identifier which has not been converted to an Attribute yet
    Attribute((usize, usize)), // coordinates in the joined_record (table, attri_num)
    Value(Data) // int, float, text (with certain extra restrictions from parsing)
}

impl Operand {
    fn parse(op: &str) -> Result<Operand, DBError> {
        if op.starts_with('"') && op.ends_with('"') && op.len() > 1 && op.len() < 33 { // 0 to 30 characters in stringConst
            return Ok(Operand::Value(Data::Text(Text::wrap(&op[1..op.len()-1]))))
        }

        if let Ok(int) = op.parse::<i32>() {
            return Ok(Operand::Value(Data::Integer(Integer::wrap(int))))
        }

        if let Ok(float) = op.parse::<f64>() {
            return Ok(Operand::Value(Data::Float(Float::wrap(float)))) // gotta love wrapper types
        }

        if let Ok(identifier) = Identifier::from(op) {
            return Ok(Operand::Identifier(identifier))
        }

        return Err(DBError::ParseError("Could not parse Operand."))
    }
}

pub struct Constraint {
    left_op: Operand,
    rel_op: RelOp,
    right_op: Operand,
}

impl Constraint {
    fn parse_split(prop: &str) -> Result<(Self, &str), Box<dyn Error>> {
        let prop = prop.trim();

        let (left_op, prop) = split_word(prop);
        let prop = prop.trim_start();
        let (rel_op, prop) = split_rel_op(prop)?;
        let prop  = prop.trim_start();
        let (right_op, prop) = split_word(prop);

        let left_op = Operand::parse(left_op)?;
        let right_op = Operand::parse(right_op)?;

        Ok((Constraint {
            left_op,
            rel_op,
            right_op,
        }, prop))
    }

    fn eval(&self, joined_record: &Vec<&Vec<Data>>) -> bool {
        let left_data = match self.left_op {
            Operand::Value(ref data) => data,
            Operand::Attribute((table, attri)) => &joined_record[table][attri],
            _ => panic!("Can't evaluate constraint before converting identifier operands to attributes.")
        };

        let right_data = match self.right_op {
            Operand::Value(ref data) => data,
            Operand::Attribute((table, attri)) => &joined_record[table][attri],
            _ => panic!("Can't evaluate constraint before converting identifier operands to attributes.")
        };

        match (left_data, right_data) {
            (Data::Float(f1), Data::Float(f2)) => self.rel_op.cmp(f1.value(), f2.value()),
            (Data::Integer(i1), Data::Integer(i2)) => self.rel_op.cmp(i1.value(), i2.value()),
            (Data::Text(t1), Data::Text(t2)) => self.rel_op.cmp(t1.content(), t2.content()),
            _ => panic!("Incompatible data types cannot be compared") // Errors of this type should be found during the parsing of commands
        }
    }

    // will take a list of tables and fill in the appropriate coordinates for each operand identifier found
    // usize identifies which tables this constraint points to
    fn convert_with(&mut self, tables: &Vec<&Table>) -> Result<(), Box<dyn Error>> {
        'outer: { if let Operand::Identifier(id) = &self.left_op {
            for (i, table) in tables.iter().enumerate() {
                for (j, (attribute, _)) in table.attributes().iter().enumerate() {
                    if attribute.name() == id.name() {
                        self.left_op = Operand::Attribute((i, j));
                        break 'outer;
                    }
                }
            }
            return Err(Box::new(DBError::ConstraintError("Could not find an attribute in the table with the name given in the Condition.")))
        }}

        'outer: { if let Operand::Identifier(id) = &self.right_op {
            for (i, table) in tables.iter().enumerate() {
                for (j, (attribute, Domain)) in table.attributes().iter().enumerate() {
                    if attribute.name() == id.name() {
                        self.right_op = Operand::Attribute((i, j));
                        break 'outer;
                    }
                }
            }
            return Err(Box::new(DBError::ConstraintError("Could not find an attribute in the table with the name given in the Condition.")))
        }}

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

    fn refs_single_table(&self) -> Option<usize> {
        match (&self.left_op, &self.right_op) {
            (Operand::Attribute((i1,j1)), Operand::Attribute((i2, j2))) => if i1 == i2 {Some(*i1)} else {None},
            (Operand::Attribute((i, _)), _) => Some(*i),
            (_, Operand::Attribute((i, _))) => Some(*i),
            _=> panic!("Should never call refs_single_table method before converting constraint.")
        }
    }

    fn get_key(&self) -> Option<Data> {
        if let Constraint {
            left_op: Operand::Attribute((_, 0)),
            rel_op: RelOp::Equals,
            right_op: Operand::Value(data) 
        } = self {
            return Some(data.clone())
        } else if let Constraint {
            left_op: Operand::Value(data),
            rel_op: RelOp::Equals,
            right_op: Operand::Attribute((_, 0))
        } = self {
            return Some(data.clone())
        }

        None
    }
}

pub struct Condition {
    bool_evals: Vec<(LogOp, BoolEval)> // first LogOp is a placeholder that should always be AND
}


impl Condition {
    pub fn parse(cond: &str) -> Result<Self, Box<dyn Error>> {
        let mut cond = cond;
        let mut bool_evals: Vec<(LogOp, BoolEval)> = Vec::new();

        let mut last_log_op = LogOp::And;

        loop {
            cond = cond.trim_start();
            if let Ok((chunk, short_cond)) = split_parenthesis_chunk(cond) {
                cond = short_cond;
                bool_evals.push((
                    last_log_op,
                    BoolEval::Condition(
                        Condition::parse(chunk)?
                    )
                ));
            } else if let Ok((constraint, short_cond)) = Constraint::parse_split(cond) {
                cond = short_cond;
                bool_evals.push((
                    last_log_op,
                    BoolEval::Constraint(constraint)
                ));
            } else {
                Err(DBError::ParseError("Did not find valid constraint or parenthesis chunk."))?
            }

            cond = cond.trim_start();
            
            let log_op;
            (log_op, cond) = split_word(cond);

            last_log_op = if log_op == "and" { LogOp::And } else if log_op == "or" { LogOp::Or } else {
                cond = cond.trim();
                if cond.is_empty() {
                    break;
                } else {
                    Err(DBError::ParseError("Did not find valid logical operator."))?
                }
            }
        }

        Ok(Condition {bool_evals})

    }

    fn convert_with(&mut self, tables: &Vec<&Table>) -> Result<(), Box<dyn Error>> {
        for (_, bool_eval) in self.bool_evals.iter_mut() {
            match bool_eval {
                BoolEval::Condition(cond) => cond.convert_with(tables)?,
                BoolEval::Constraint(constraint) => constraint.convert_with(tables)?
            }
        }

        Ok(())
    }

    fn split_load_helpers(&mut self, helpers: &mut HashMap<usize, Condition>, mut always_true: bool) -> Option<usize> {
        for (log_op, _) in self.bool_evals.iter() {
            if *log_op == LogOp::Or {
                always_true = false;
            }
        }

        let mut single_table = true;
        let mut last_table = None;

        let mut i = 0;
        while i < self.bool_evals.len() {
            match self.bool_evals[i].1 {
                BoolEval::Condition(ref mut cond) => {
                    if let Some(table_num)  = cond.split_load_helpers(helpers, always_true) {
                        if always_true {
                            let bool_eval = self.bool_evals.swap_remove(i);
                            match helpers.get_mut(&table_num) {
                                Some(condition) => condition.bool_evals.push(bool_eval),
                                None => {helpers.insert(table_num, Condition { bool_evals: vec![bool_eval] });}
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
                                None => {helpers.insert(table_num, Condition { bool_evals: vec![bool_eval] });}
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

    fn get_record_nums_from_bst(&mut self, bst: &BST) -> Vec<usize> {
        for (log_op, _) in self.bool_evals.iter() {
            if *log_op == LogOp::Or {
                return bst.get_data(); // all are possible
            }
        }
        
        let mut key: Option<Data> = None;
        let mut i = 0;
        while i < self.bool_evals.len() {
            match &self.bool_evals[i].1 {
                BoolEval::Condition(_) => (),
                BoolEval::Constraint(constraint) => {
                    let new_key = constraint.get_key();
                    match new_key {
                        None => (),
                        Some(new_key) => {
                            self.bool_evals.swap_remove(i);
                            match &key {
                                Some(key) => {
                                    if *key == new_key {
                                        continue;
                                    } else {
                                        return vec![];
                                    }
                                }
                                None => key = Some(new_key)
                            }
                            continue;
                        }
                    }
                }
            }
            i+=1;
        }

        match &key {
            Some(key) => {
                match bst.find(key) {
                    Some(rec_num) => vec![*rec_num],
                    None => vec![]
                }
            }
            None => {
                bst.get_data()
            }
        }
    }

    fn filter_table_coords(mut self, mem_tables: &Vec<MemTable>, table_num: usize, bst: &Option<BST>) -> Vec<usize> {
        let mut selected: Vec<usize> = Vec::with_capacity(mem_tables[table_num].records.len());

        let records_coords: Vec<usize> = match bst {
            Some(bst) => self.get_record_nums_from_bst(bst),
            None => (0..mem_tables[table_num].records.len()).collect()
        };

        let binding = vec![]; // this is not the most efficient way to go about doing this for a single table but it works
        let mut joined_record:Vec<&Vec<Data>> = vec![&binding; mem_tables.len()];
        for coord in records_coords.into_iter() {
            joined_record[table_num] = &mem_tables[table_num].records[coord];
            vec![&mem_tables[table_num].records[coord]];
            if self.eval(&joined_record) {
                selected.push(coord);
            }
        }
        
        selected
    }

    fn eval_coords(&self, table_coords: Vec<Vec<usize>>, tables: &Vec<MemTable>) -> Vec<Vec<usize>> {
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
                        return true
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
                        return true
                    } else {
                        curr_evaluation = constraint.eval(joined_record);
                    }
                }
            }
        }

        curr_evaluation
    }

    pub fn select(mut self, tables: Vec<&Table>) -> Result<MemTable, Box<dyn Error>> {
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
        for (i, table) in mem_tables.iter().enumerate() {
            record_nums_vec.push( match helpers.remove(&i) {
                Some(helper) => helper.filter_table_coords(&mem_tables, i, &tables[i].bst),
                None => (0..table.records.len()).collect()
            });
        }

        for record_nums in record_nums_vec.iter() {
            if record_nums.len() == 0 {
                return Ok(MemTable::build_from_records(vec![], new_attributes))
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

        Ok(MemTable::build_from_records(records, new_attributes)) // TODO make a method on mem_tables that works for this type of thing
    }

    pub fn update(mut self, table: &mut Table, new_values: Vec<(Identifier, Data)>) -> Result<(), Box<dyn Error>> {
        // replace attributes in bool_eval with table coordinates
        self.convert_with(&vec![table])?;

        // load MemTable
        let mem_table = MemTable::build(table)?;

        // Filter using self as the condition
        let filtered = self.filter_table_coords(&vec![mem_table], 0, &table.bst);

        // for each record in filtered coordinates update the record in table which has that record_num
        table.update_all(filtered, new_values)
    }

    pub fn delete (mut self, table: &mut Table) -> Result<(), Box<dyn Error>> {
        // replace attributes in bool_eval with table coordinates
        self.convert_with(&vec![table])?;

        // load MemTable
        let mem_table = MemTable::build(table)?;

        // Filter using self as the condition
        let filtered = self.filter_table_coords(&vec![mem_table], 0, &table.bst);

        // for each record in filtered coordinates update the record in table which has that record_num
        table.delete_all(filtered)
    }
}

pub enum RelOp {
    Equals,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl RelOp {
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

fn split_word(given: &str) -> (&str, &str) {
    for (i, c) in given.char_indices() {
        if !c.is_ascii_alphanumeric() && c != '"' && c != '.' && c != '-' {
            return (&given[..i], &given[i..])
        }
    }

    return (&given, "");
}

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
        _ => Err(DBError::ParseError("Did not find a valid RelOp."))
    }
}

fn split_parenthesis_chunk(cond: &str) -> Result<(&str, &str), ()> {
    if !cond.starts_with('(') {
        return Err(())
    }

    let mut open_count = 0;
    for (i, c) in cond.char_indices() {
        if c == '(' {
            open_count += 1;
        } else if c == ')' {
            open_count -= 1;
        }
        if open_count == 0 {
            return Ok((&cond[1..i], &cond[i+1..])); // remove open and closing of this chunk through selective indicies
        }
    }

    Err(())
}


#[cfg(test)]
mod tests {
    use crate::db_cmds;
    use crate::Database;
    use std::fs::File;
    use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");

    #[test]
    fn selecting_from_a_table() -> Result<(), Box<dyn Error>> {
        let mut db = Database::new();
        db_cmds::run_cmd("create database test", &mut db)?;
        db_cmds::run_cmd("use test", &mut db)?;
        // db_cmds::run_cmd("create table one (key1 integer primary key)", &mut db)?;
        // db_cmds::run_cmd("create table two (key2 integer primary key)", &mut db)?;
        // db_cmds::run_cmd("insert one values (3)", &mut db)?;
        // db_cmds::run_cmd("insert one values (4)", &mut db)?;
        // db_cmds::run_cmd("insert one values (5)", &mut db)?;
        // db_cmds::run_cmd("insert one values (6)", &mut db)?;
        // db_cmds::run_cmd("insert one values (7)", &mut db)?;
        // db_cmds::run_cmd("insert one values (8)", &mut db)?;
        // db_cmds::run_cmd("insert two values (458)", &mut db)?;
        // db_cmds::run_cmd("insert two values (4)", &mut db)?;
        // db_cmds::run_cmd("insert two values (5)", &mut db)?;
        // db_cmds::run_cmd("insert two values (6)", &mut db)?;
        // db_cmds::run_cmd("insert two values (7)", &mut db)?;
        // db_cmds::run_cmd("insert two values (8)", &mut db)?;
        // db_cmds::run_cmd("insert one values (-8)", &mut db)?;
        for record in Condition::parse("key1 = 3")?
        .select(vec![
            db.table_map.get("one").unwrap(),
            db.table_map.get("two").unwrap()
            ])?.records {
            for data in record {
                dbg!(data);
            }
        }
        //db_cmds::run_exit(&mut db)?;
        
        
        Ok(())
    }

    #[test]
    fn reading_records_into_mem_table() -> Result<(), Box<dyn Error>> {
        let mut db = Database::new();
        db_cmds::run_cmd("create database test", &mut db)?;
        db_cmds::run_cmd("use test", &mut db)?;
        //db_cmds::run_cmd("create table foo (key integer primary key)", &mut db)?;
        //db_cmds::run_cmd("insert foo values (3)", &mut db)?;

        MemTable::build(db.table_map.get("two").unwrap())?.records.iter().for_each(|record| {
            for data in record {
                dbg!(data);
            }
        });
        db_cmds::run_exit(&mut db)?;
        
        
        Ok(())
    }
}