use crate::base::{Data, Domain, Float, Identifier, Integer, Text};
use crate::binary_search_tree::{BSTInsertErr, BST};
use crate::logic::Condition;
use crate::DBError;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek};
use std::io::{SeekFrom, Write};
use std::vec;

/// An object providing table management in files.
/// Tables will save themselves in .dat files and save
/// their bsts in .index files.
pub struct Table {
    attributes: Vec<(Identifier, Domain)>,
    record_count: usize,
    meta_offset: usize,
    pub bst: Option<BST>,
    pub key_attri_num: Option<usize>,
    record_length: u32,
    pub file_path: String,
}

impl Table {
    /// Create an instance of Table and save its metadata to a file. Also
    /// create the file for the bst if specified.
    /// The files created will use the format '{dir}{name}.{ext}'
    ///
    /// # Errors
    ///
    /// Fails when cannot write to the filesystem or
    /// when there are two attributes given in the list that have the
    /// same name. Also requires that the primary_key, if specified,
    /// must be within the bounds of the table's attributes.
    pub fn build(
        name: &str,
        attributes: Vec<(Identifier, Domain)>,
        primary_key: Option<usize>,
        dir: &str,
    ) -> Result<Self, Box<dyn Error>> {
        for (i, attri1) in attributes.iter().enumerate() {
            for (j, attri2) in attributes.iter().enumerate() {
                if i != j && attri1.0.name() == attri2.0.name() {
                    return Err(Box::new(DBError::ConstraintError(
                        "Cannot have two attributes with the same Identifier in a table.",
                    )));
                }
            }
        }

        let name = Identifier::from(name)?; // reject name if not an identifier
        let mut record_length: u32 = 0;
        let meta_offset = attributes.len() * 20 + 24; //  16 bytes at the beginning + 19 bytes per + 1 byte per + 8 bytes at the end
        for (_, domain) in attributes.iter() {
            record_length += Domain::size_in_bytes(domain);
        }

        let file_path = format!("{dir}{}.dat", name.name());
        let mut file = File::create_new(&file_path)?;

        if let Some(prim_key_num) = primary_key {
            if prim_key_num < attributes.len() {
                file.write_all(&prim_key_num.to_be_bytes())?;
            } else {
                return Err(Box::new(DBError::ConstraintError(
                    "Primary Key attribute index cannot be larger than the number of attributes.",
                )));
            }
        } else {
            file.write_all(&0_usize.to_be_bytes())?;
        }

        // attribute list size
        file.write_all(&attributes.len().to_be_bytes())?; // 8 bytes

        for (attribute, domain) in attributes.iter() {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
        }

        // table_size
        file.write_all(&0_usize.to_be_bytes())
            .expect("Should be able to write to file."); // usize default

        let (bst, key_attri_num) = if let Some(prim_key_num) = primary_key {
            let bst_path = format!("{dir}{}.index", name.name());
            let bst = BST::new();
            bst.write_to_file(&bst_path)?; // this way the table will know it has a primary key based on the existence of this file
            (Some(bst), Some(prim_key_num))
        } else {
            (None, None)
        };

        Ok(Table {
            attributes,
            record_count: 0,
            meta_offset,
            bst,
            key_attri_num,
            record_length,
            file_path,
        })
    }

    /// Attempts to remove the table and its bst from the filesystem and consume the instance.
    ///
    /// # Errors
    ///
    /// Fails when cannot find or delete the files.
    pub fn clean_up(self) -> Result<(), Box<dyn Error>> {
        fs::remove_file(&self.file_path)?;
        match self.bst {
            Some(_) => {
                let mut bst_path = self.file_path.clone();
                bst_path.replace_range(self.file_path.len() - 3.., "index");
                fs::remove_file(&bst_path)?; // use full match to use the ? op
            }
            None => (),
        }
        Ok(())
    }

    /// Returns a reference to the Table's attributes list
    pub fn attributes(&self) -> &Vec<(Identifier, Domain)> {
        &self.attributes
    }

    /// Attempts to read the metadata of a Table from the file given by
    /// '{dir}{name}.dat'. Note, this will search for a file in the form
    /// '{dir}{name}.index' that represents a bst for this table and load that as well.
    /// Returns the instance of Table read if successful.
    ///
    /// # Errors
    ///
    /// Fails when cannot read from a file or when
    /// Table metadata has incorrect formatting due to
    /// corruption or by the file not being created
    /// by an instance of Table
    pub fn read_from_file(name: &str, dir: &str) -> Result<Self, Box<dyn Error>> {
        let _ = Identifier::from(name)?; // does copy the string but more importantly fails if name is not an identifier
        let name = name.to_string();
        let file_path = format!("{dir}{name}.dat");
        let bst_file_path = format!("{dir}{name}.index");
        let bst = match BST::read_from_file(&bst_file_path) {
            Ok(bst) => Some(bst),
            Err(_) => None, // fails if the file is not there or in a bad format
        };

        let mut file = File::open(&file_path)?;

        let mut key_attri_num: [u8; 8] = [0; 8];
        file.read_exact(&mut key_attri_num)?;
        let key_attri_num = usize::from_be_bytes(key_attri_num);

        let key_attri_num = if let Some(_) = bst {
            Some(key_attri_num)
        } else {
            None
        };

        let mut attribute_list_len: [u8; 8] = [0; 8];
        file.read_exact(&mut attribute_list_len)?;
        let attribute_list_len = usize::from_be_bytes(attribute_list_len);

        let meta_offset = attribute_list_len * 20 + 24;

        let mut attributes_bytes: Vec<u8> = vec![0; attribute_list_len as usize * 20];
        file.read_exact(&mut attributes_bytes)?;

        let mut record_count: [u8; 8] = [0; 8];
        file.read_exact(&mut record_count)?;
        let record_count = usize::from_be_bytes(record_count);

        let mut attributes: Vec<(Identifier, Domain)> =
            Vec::with_capacity(attribute_list_len as usize);

        let mut record_length: u32 = 0;
        for _ in 0..attribute_list_len {
            let domain = attributes_bytes.remove(attributes_bytes.len() - 1);
            let domain = match domain {
                0 => Domain::Integer,
                1 => Domain::Text,
                2 => Domain::Float,
                _ => {
                    return Err(Box::new(DBError::FileFormatError(
                        "Incorrect Domain type read.",
                    )))
                }
            };
            record_length += Domain::size_in_bytes(&domain);
            let identifier = attributes_bytes.split_off(attributes_bytes.len() - 19); // 19 is identifier size written
            let identifier = String::from_utf8(identifier)?;
            let identifier = Identifier::from(&identifier.trim())?;
            attributes.push((identifier, domain));
        }
        attributes.reverse(); // because read in reverse order... consider just reading this in reverse in other places and omitting this

        Ok(Table {
            attributes,
            record_count,
            meta_offset,
            bst,
            key_attri_num,
            record_length,
            file_path,
        })
    }

    /// Attempts to write the record_count of a Table instance to the file
    /// the table was read from or originally wrote itself to. This is necessary
    /// since record count does not need to be written every time a record is written
    /// in the case of mass deletion or updates.
    ///
    /// # Errors
    ///
    /// Fails when cannot write to the file.
    pub fn write_record_count(&self) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(false)
            .open(&self.file_path)?;
        file.seek(SeekFrom::Start(self.meta_offset as u64 - 8))?; // 8 bytes at the end of the meta
        file.write(&self.record_count.to_be_bytes())?;
        Ok(())
    }

    /// Creates a string representation for each attribute and pushes
    /// each representation to a vector. This is used to get formatted
    /// information to describe the table to the user.
    ///
    /// Indicates the name and domain for each attribute as well as
    /// the location of the primary key.
    pub fn attributes_to_string_vec(&self) -> Vec<String> {
        let mut output = Vec::new();
        let attri_iter = self.attributes.iter();
        for (i, (attribute, domain)) in attri_iter.enumerate() {
            let tabs = if attribute.name().len() > 15 {
                "\t"
            } else if attribute.name().len() > 7 {
                "\t\t"
            } else {
                "\t\t\t"
            };
            if self.key_attri_num == Some(i) {
                output.push(format!(
                    "{}{tabs}{}\tPRIMARY KEY",
                    attribute.name().to_uppercase(),
                    domain.to_string()
                ));
            } else {
                output.push(format!(
                    "{}{tabs}{}",
                    attribute.name().to_uppercase(),
                    domain.to_string()
                ));
            }
        }
        output
    }

    /// Attempts to write the given record to the file
    /// the table was read from or originally wrote itself to.
    /// Does not write the updated record_count although it does
    /// add one to it in memory. Records are always written at the
    /// end of the table.
    ///
    /// # Errors
    ///
    /// Fails when cannot write to the file or
    /// when the record does not match the format of
    /// the Table instance.
    pub fn write_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new().append(true).open(&self.file_path)?;
        let mut record_bytes: Vec<u8> = Vec::new();
        for (data, (_, domain)) in record.iter().zip(self.attributes.iter()) {
            match (data, domain) {
                (Data::Integer(int), Domain::Integer) => {
                    record_bytes.append(&mut int.to_bytes().to_vec())
                }
                (Data::Float(float), Domain::Float) => {
                    record_bytes.append(&mut float.to_bytes().to_vec())
                }
                (Data::Text(text), Domain::Text) => {
                    record_bytes.append(&mut text.to_bytes().to_vec())
                }
                _ => {
                    return Err(Box::new(DBError::ConstraintError(
                        "Cannot write record with invalid data order.",
                    )))
                }
            }
        }
        if let Some(ref mut bst) = self.bst {
            let key = record[self.key_attri_num.unwrap()].clone();
            bst.insert(key, self.record_count)?;
        }
        self.record_count += 1;

        file.write_all(&record_bytes)?;
        Ok(())
    }

    /// An alias for calling write_record and write_record_count successively.
    ///
    /// # Errors
    ///
    /// Fails when cannot write to the file or
    /// when the record does not match the format of
    /// the Table instance.
    pub fn write_single_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
        self.write_record(record)?;
        self.write_record_count()
    }

    /// Attempts to read a record back from the file.
    /// When successful, returns the record wrapped in an Ok variant.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be read from or
    /// when the file is in a bad format.
    pub fn read_record(&self, record_num: usize) -> Result<Vec<Data>, Box<dyn Error>> {
        if record_num > self.record_count {
            return Err(Box::new(DBError::ConstraintError(
                "Cannot find a record which is out of the table's bounds",
            )));
        }
        let mut file = File::open(&self.file_path)?;
        let mut record_bytes: Vec<u8> = vec![0; self.record_length as usize];

        file.seek(SeekFrom::Start(
            (self.meta_offset + record_num * self.record_length as usize) as u64,
        ))?;
        file.read_exact(&mut record_bytes)?;
        let mut record: Vec<Data> = Vec::with_capacity(self.attributes.len());
        let mut offset = 0;
        for (_, domain) in self.attributes.iter() {
            let data = match domain {
                Domain::Float => {
                    offset += Float::byte_len();
                    Data::Float(Float::from_bytes(
                        &record_bytes[offset - 5..offset].try_into().unwrap(),
                    )?)
                }
                Domain::Integer => {
                    offset += Integer::byte_len();
                    Data::Integer(Integer::from_bytes(
                        &record_bytes[offset - 4..offset].try_into().unwrap(),
                    ))
                }
                Domain::Text => {
                    offset += 100;
                    Data::Text(Text::from_bytes(&record_bytes[offset - 100..offset])?)
                }
            };
            record.push(data)
        }
        Ok(record)
    }

    /// Attempts to write this Table's bst to the file.
    /// If the Table has no bst, it will simply return an
    /// empty Ok variant immediately.
    ///
    /// # Errors
    ///
    /// Fails when cannot write to the file.
    pub fn write_bst(&self) -> Result<(), std::io::Error> {
        if let Some(ref bst) = self.bst {
            let mut bst_path = self.file_path.clone();
            bst_path.replace_range(self.file_path.len() - 3.., "index");
            bst.write_to_file(&bst_path)
        } else {
            Ok(())
        }
    }

    /// Attempts to read every record in the order stored in the file.
    ///
    /// # Errors
    ///
    /// Fails if cannot read from the file or
    /// when a record is found in a bad format.
    fn read_all_data(&self) -> Result<Vec<Vec<Data>>, Box<dyn Error>> {
        let mut file = File::open(&self.file_path)?;
        let mut records_bytes: Vec<u8> = vec![0; self.record_length as usize * self.record_count];
        file.seek(SeekFrom::Start(self.meta_offset as u64))?;
        file.read_exact(&mut records_bytes)?;
        let mut records: Vec<Vec<Data>> = Vec::new();
        let mut offset = 0;
        for _ in 0..self.record_count {
            let mut record: Vec<Data> = Vec::with_capacity(self.attributes.len());
            for (_, domain) in self.attributes.iter() {
                let data = match domain {
                    Domain::Float => {
                        offset += Float::byte_len();
                        Data::Float(Float::from_bytes(
                            &records_bytes[offset - 5..offset].try_into().unwrap(),
                        )?)
                    }
                    Domain::Integer => {
                        offset += Integer::byte_len();
                        Data::Integer(Integer::from_bytes(
                            &records_bytes[offset - 4..offset].try_into().unwrap(),
                        ))
                    }
                    Domain::Text => {
                        offset += 100; // the max text byte length and the length we always store it as for tables
                        Data::Text(Text::from_bytes(&records_bytes[offset - 100..offset])?)
                    }
                };
                record.push(data)
            }
            records.push(record);
        }
        Ok(records)
    }

    /// Attempts to overwrite the attribute names in the file
    /// and in memory. Note, this will not change the in memory
    /// storage if it cannot write to the file.
    ///
    /// # Errors
    ///
    /// Fails when given a new set of attributes with duplicated names
    /// or when cannot write to the file.
    pub fn rename_attributes(
        &mut self,
        new_attributes: Vec<Identifier>,
    ) -> Result<(), Box<dyn Error>> {
        for (i, attri1) in new_attributes.iter().enumerate() {
            for (j, attri2) in new_attributes.iter().enumerate() {
                if i != j && attri1.name() == attri2.name() {
                    return Err(Box::new(DBError::ConstraintError(
                        "Cannot have two attributes with the same Identifier in a table",
                    )));
                }
            }
        }
        let mut file = OpenOptions::new().write(true).open(&self.file_path)?;

        // just overwrite all front meta since seek-writing wouldn't greatly improve performance at all
        file.write_all(&self.key_attri_num.unwrap_or_else(|| 0_usize).to_be_bytes())?;
        file.write_all(&self.attributes.len().to_be_bytes())?; // 8 bytes

        let mut zipped_attris = Vec::with_capacity(self.attributes.len());
        for (attribute, (_, domain)) in new_attributes.into_iter().zip(&self.attributes) {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
            zipped_attris.push((attribute, domain.clone()));
        }

        self.attributes = zipped_attris;
        Ok(())
    }

    /// Similar to write_record except that this writes over a specific location
    /// in the file to replace an existing record and does not update the record_count.
    /// Also updates the attached bst when a key value is changed.
    ///
    /// # Errors
    ///
    /// Fails when the new_values for the record do not match the Table's attributes or
    /// when the file cannot be written to. Also requires that the record_num
    /// given is within the Table's record_count.
    fn update_record(
        &mut self,
        record_num: usize,
        new_values: &Vec<(Identifier, Data)>,
    ) -> Result<(), Box<dyn Error>> {
        let prev_record = self.read_record(record_num)?;
        let mut record = prev_record.clone();

        for (i, (identifier, _)) in self.attributes.iter().enumerate() {
            for (check_id, new_data) in new_values {
                if check_id.name() == identifier.name() {
                    record[i] = new_data.clone();
                }
            }
        }

        // consider extrating this logic into a separate function since it's used a couple times
        let mut record_bytes: Vec<u8> = Vec::new();
        for (data, (_, domain)) in record.iter().zip(self.attributes.iter()) {
            match (data, domain) {
                (Data::Integer(int), Domain::Integer) => {
                    record_bytes.append(&mut int.to_bytes().to_vec())
                }
                (Data::Float(float), Domain::Float) => {
                    record_bytes.append(&mut float.to_bytes().to_vec())
                }
                (Data::Text(text), Domain::Text) => {
                    record_bytes.append(&mut text.to_bytes().to_vec())
                }
                _ => {
                    return Err(Box::new(DBError::ConstraintError(
                        "Cannot write record with invalid data order.",
                    )))
                }
            }
        }

        let mut file = OpenOptions::new().write(true).open(&self.file_path)?;
        file.seek(SeekFrom::Start(
            (self.meta_offset + record_num * self.record_length as usize) as u64,
        ))?;

        if let Some(ref mut bst) = self.bst {
            if prev_record[0] != record[0] {
                bst.remove(&prev_record[0]);
                let key = record[0].clone();
                match bst.insert(key, record_num) {
                    Err(BSTInsertErr) => Err(DBError::ConstraintError(
                        "Cannot set a key to the value of another key in the table.",
                    ))?,
                    _ => (),
                }
            }
        }

        file.write_all(&record_bytes)?;
        Ok(())
    }

    /// Calls update_record for each record selected by the converted condition. This function is used
    /// by Condition itself, so using Condition.update is preferred over using this. Puts labelled values in
    /// new_values into each record it writes over.
    ///
    /// # Errors
    ///
    /// Fails if the Condition provided has not been converted to use the given Table - i.e. called by
    /// something other than Condition.update. Also fails for all the reasons update_record fails including
    /// file access issues and incorrect record formatting.
    ///
    /// In addition, this ensures that a user does not update more than one key value in the table to the same
    /// value.
    pub fn update_all(
        &mut self,
        cond: Condition,
        new_values: Vec<(Identifier, Data)>,
    ) -> Result<(), Box<dyn Error>> {
        // load MemTable
        let mem_table = MemTable::build(self)?;
        let record_nums: Vec<usize> =
            cond.filter_table_coords(&vec![mem_table], 0, &self.bst, &vec![self]);

        // check if updating a key more than once - which is illegal
        if self.bst.is_some() && record_nums.len() > 1 {
            for (id, _) in new_values.iter() {
                if id.name() == self.attributes[0].0.name() {
                    // updating a key more than once
                    Err(DBError::ConstraintError(
                        "Cannot set more than one key value at a time.",
                    ))?
                }
            }
        }

        for record_num in record_nums {
            self.update_record(record_num, &new_values)?;
        }
        Ok(())
    }

    /// Removes each record selected by the converted condition. This function is used
    /// by Condition itself, so using Condition.delete is preferred over using this. Note
    /// that this does not preserve the order of the records in the file. Instead, loads
    /// all records, looks for the records to delete through the Condition and
    /// swap-removes them from highest to lowest position in the file. The bst is then
    /// reconstructed after deletion.
    ///
    /// # Errors
    ///
    /// Fails when cannot read/write to files or when the Condition provided has not been
    /// converted to use the given Table - i.e. called by something other than Condition.delete
    pub fn delete_all(&mut self, cond: Condition) -> Result<(), Box<dyn Error>> {
        let mut mem_tables = vec![MemTable::build(self)?];
        let mut record_nums = cond.filter_table_coords(&mem_tables, 0, &self.bst, &vec![self]);
        let mut mem_table = mem_tables.remove(0);

        if record_nums.len() == 0 {
            return Ok(());
        }

        record_nums.sort(); // could be in bst order but we want highest to lowest table position
        for record_num in record_nums.into_iter().rev() {
            mem_table.records.swap_remove(record_num); // must swap remove highest numbers first
        }

        // clear the bst
        match self.bst {
            None => (),
            Some(ref mut bst) => *bst = BST::new(),
        }

        let file = OpenOptions::new().write(true).open(&self.file_path)?;
        file.set_len(self.meta_offset as u64)?;

        self.record_count = 0; // set to 0 since write_record_count will increment it

        for record in mem_table.records {
            self.write_record(record)?;
        }
        self.write_record_count()?;

        // recreate the bst
        match self.bst {
            None => (),
            Some(ref mut bst) => {
                let mut bst_path = self.file_path.clone();
                bst_path.replace_range(self.file_path.len() - 3.., "index");
                bst.write_to_file(&bst_path)?;
                *bst = BST::read_from_file(&bst_path)?; // balances the bst... not the best way to do this but it works for the purposes we need
            }
        }

        Ok(())
    }
}

/// A representation for Tables loaded into memory, equipped with the tools for selection type operations
pub struct MemTable {
    pub records: Vec<Vec<Data>>,
    pub attributes: Vec<(Identifier, Domain)>,
    projection: Vec<usize>,
}

impl MemTable {
    /// Attempt to create a MemTable based on a Table.
    ///
    /// # Errors
    ///
    /// Fails if the table's file cannot be loaded.
    pub fn build(table: &Table) -> Result<Self, Box<dyn Error>> {
        Ok(MemTable {
            records: table.read_all_data()?,
            attributes: table
                .attributes
                .iter()
                .map(|(identifier, domain)| ((*identifier).clone(), domain.clone()))
                .collect(),
            projection: (0..table.attributes.len()).collect(), // start with all attributes projected
        })
    }

    /// Attempts to create a MemTable from an attribute list and a list of records.
    /// Note that this does not check for the records being the same length or having
    /// the same datatype order. This is rather for situations in which bypassing those
    /// checks is a handy shortcut.
    ///
    /// # Errors
    ///
    /// Fails when the attributes list has duplicate Identifiers
    pub fn build_from_records(
        records: Vec<Vec<Data>>,
        attributes: Vec<(Identifier, Domain)>,
    ) -> Result<Self, Box<dyn Error>> {
        let attributes_len = attributes.len();
        let mem_table = MemTable {
            records,
            attributes,
            projection: (0..attributes_len).collect(), // start with all attributes projected
        };

        for (i, attri1) in mem_table.attributes.iter().enumerate() {
            for (j, attri2) in mem_table.attributes.iter().enumerate() {
                if i != j && attri1.0.name() == attri2.0.name() {
                    return Err(Box::new(DBError::ConstraintError(
                        "Cannot have two attributes with the same Identifier in a table",
                    )));
                }
            }
        }

        Ok(mem_table)
    }

    /// Culls the projection list based on the selected_attris list. Note that
    /// this does not actually change any of the records in the MemTable. Instead,
    /// it changes the attribute numbers remembered as projected for other functions.
    ///
    /// # Errors
    ///
    /// Fails if the selected_attris tries to project an attribute that does
    /// not exist in the current projection.
    pub fn project(&mut self, selected_attris: Vec<&str>) -> Result<(), DBError> {
        let mut new_projection = Vec::new();
        'outer: for selected in selected_attris.iter() {
            for attri_num in self.projection.iter() {
                if *selected == self.attributes[*attri_num].0.name() {
                    new_projection.push(*attri_num);
                    continue 'outer;
                }
            }
            return Err(DBError::ParseError(
                "Could not find attribute to project in the given table.",
            ));
        }
        self.projection = new_projection;
        Ok(())
    }

    /// Returns a vector of strings such that when each is printed with a \n attached
    /// the MemTable comes out nicely formatted. This pays attention to the projection list.
    pub fn to_string_vec(&self) -> Vec<String> {
        if self.records.len() == 0 {
            return vec![String::from("\nNothing Found.\n")];
        }

        let mut output: Vec<String> = Vec::with_capacity(self.records.len() * 2 + 3);

        let mut attribute_lengths = vec![0; self.projection.len() + 1]; // plus one for row numbers that will be added
        for (i, attri_num) in self.projection.iter().enumerate() {
            let (identifier, _) = &self.attributes[*attri_num];
            attribute_lengths[i] = identifier.name().len(); // two spaces and two pipe characters
        }

        for record in self.records.iter() {
            for (i, attri_num) in self.projection.iter().enumerate() {
                let data = &record[*attri_num];
                let data_len = data.string_len(); // two spaces and two pipe characters
                if data_len > attribute_lengths[i] {
                    attribute_lengths[i] = data_len;
                }
            }
        }

        let row_num_string_length = (self.records.len() + 1).to_string().len();

        // loop through all instances of each attribute and check lengths... set each to the longest needed

        let row_length: usize = attribute_lengths.iter().sum::<usize>()
            + attribute_lengths.len() * 3
            + 1
            + row_num_string_length;

        // println stuff

        let row_hyphens = vec!["-"; row_length].concat();
        output.push(format!("{row_hyphens}"));

        let mut top_line = Vec::with_capacity(self.attributes.len() * 3 + 1);
        let extra_spaces = vec![" "; row_num_string_length].concat();
        top_line.push(format!("| {extra_spaces} | "));
        for (i, attri_num) in self.projection.iter().enumerate() {
            let (identifier, _) = &self.attributes[*attri_num];
            let extra_spaces = vec![" "; attribute_lengths[i] - identifier.name().len()].concat();
            let id_string = [identifier.name().to_string(), extra_spaces].concat();
            top_line.push(id_string);
            top_line.push(" | ".to_string());
        }
        output.push(format!("{}", top_line.concat()));

        output.push(format!("{row_hyphens}"));

        // print each record
        for (i, record) in self.records.iter().enumerate() {
            let mut new_line = Vec::with_capacity(self.attributes.len() * 3 + 1);
            let num_string = (i + 1).to_string();
            let extra_spaces = vec![" "; row_num_string_length - num_string.len()].concat();
            let num_string = [num_string, extra_spaces].concat();
            new_line.push(format!("| {num_string} | "));
            for (i, attri_num) in self.projection.iter().enumerate() {
                let data = &record[*attri_num];
                let data_string = data.to_string(); // two spaces and two pipe characters
                let extra_spaces = vec![" "; attribute_lengths[i] - data_string.len()].concat();
                let data_string = [data_string, extra_spaces].concat();
                new_line.push(data_string);
                new_line.push(" | ".to_string());
            }
            output.push(format!("{}", new_line.concat()));
        }

        output.push(format!("{row_hyphens}\n"));

        output
    }

    /// Returns the list of attributes left in the projection list. Helpful for creating a new Table
    /// out of this MemTable
    pub fn get_projected_attribute_list(&self) -> Vec<&(Identifier, Domain)> {
        let mut project_attris = Vec::with_capacity(self.projection.len());

        for projection in self.projection.iter() {
            project_attris.push(&self.attributes[*projection]);
        }

        project_attris
    }

    /// Returns a given record with only the projected attributes.
    ///
    /// # Panics
    ///
    /// Panics if asked for a record out of the records list length.
    pub fn get_projected_record(&self, rec_num: usize) -> Vec<Data> {
        let mut projected_record = Vec::with_capacity(self.projection.len());
        for projection in self.projection.iter() {
            projected_record.push(self.records[rec_num][*projection].clone());
        }

        projected_record
    }
}
