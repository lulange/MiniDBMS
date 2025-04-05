use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek};
use std::io::{Write, SeekFrom};
use std::vec;
use crate::binary_search_tree::BST;
use crate::DBError;
use super::base::{
    Identifier,
    Data,
    Domain,
    Float,
    Integer,
    Text,
};

// TODO create a function so that MemTables can project certain attributes
// TODO MemTable to Table

pub struct Table {
    attributes: Vec<(Identifier, Domain)>,
    record_count: usize,
    meta_offset: usize,
    pub bst: Option<BST>,
    record_length: u32,
    file_path: String,
}

impl Table {
    pub fn build(
        name: &str,
        attributes: Vec<(Identifier, Domain)>,
        primary_key: bool,
        dir: &str,
    ) -> Result<Self, Box<dyn Error>> {
        for (i, attri1) in attributes.iter().enumerate() {
            for (j, attri2) in attributes.iter().enumerate() {
                if i != j && attri1.0.name() == attri2.0.name() {
                    return Err(Box::new(DBError::ConstraintError("Cannot have two attributes with the same Identifier in a table")))
                }
            }
        }

        let name = Identifier::from(name)?; // reject name if not an identifier
        let mut record_length: u32 = 0;
        let meta_offset = attributes.len() * 20 + 16; //  8 bytes at the beginning + 19 bytes per + 1 byte per + 8 bytes at the end
        for (_, domain) in attributes.iter() {
            record_length += Domain::size_in_bytes(domain);
        }

        let file_path = format!("{dir}/{}.dat", name.name());
        let mut file = File::create_new(&file_path)?;

        // attribute list size
        file.write_all(&attributes.len().to_be_bytes())?; // 8 bytes

        for (attribute, domain) in attributes.iter() {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
        }

        // table_size
        file.write_all(&0_usize.to_be_bytes())
            .expect("Should be able to write to file."); // usize default

        Ok(Table {
            attributes,
            record_count: 0,
            meta_offset,
            bst: if primary_key { Some(BST::new()) } else { None },
            record_length,
            file_path,
        })
    }

    pub fn attributes(&self) -> &Vec<(Identifier, Domain)> {
        &self.attributes
    }

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
        let mut attribute_list_len: [u8; 8] = [0; 8];
        file.read_exact(&mut attribute_list_len)?;
        let attribute_list_len = usize::from_be_bytes(attribute_list_len);
        let meta_offset = attribute_list_len * 20 + 16;
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
            record_length,
            file_path,
        })
    }

    pub fn write_record_count(&self) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(false)
            .open(&self.file_path)?;
        file.seek(SeekFrom::Start(self.meta_offset as u64 - 8))?; // 8 bytes at the end of the meta
        file.write(&self.record_count.to_be_bytes())?;
        Ok(())
    }

    pub fn print_attributes(&self) {
        let mut attri_iter = self.attributes.iter();
        if self.bst.is_some() {
            let (attribute, domain) = attri_iter
                .next()
                .expect("Tables should always have at least one attribute.");
            println!("{}\t{} PRIMARY KEY", attribute.name(), domain.to_string());
        }
        for (attribute, domain) in attri_iter {
            println!("{}\t{}", attribute.name(), domain.to_string());
        }
    }

    fn write_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
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
            let key = record[0].clone();
            bst.insert(key, self.record_count)?;
        }
        self.record_count += 1;

        file.write_all(&record_bytes)?;
        // TODO maybe remove from bst if this fails and then return error
        Ok(())
    }

    pub fn write_single_record(&mut self, record: Vec<Data>) -> Result<(), Box<dyn Error>> {
        self.write_record(record)?;
        self.write_record_count()
    }

    pub fn read_record(&self, record_num: usize) -> Result<Vec<Data>, Box<dyn Error>> {
        if record_num > self.record_count {
            return Err(Box::new(DBError::ConstraintError(
                "Cannot find a record which is out of the table's bounds",
            )));
        }
        let mut file = File::open(&self.file_path)?;
        let mut record_bytes: Vec<u8> = vec![0; self.record_length as usize];

        file.seek(
            SeekFrom::Start((self.meta_offset + record_num * self.record_length as usize) as u64)
        )?;
        file.read_exact(&mut record_bytes)?;
        let mut record: Vec<Data> = Vec::with_capacity(self.attributes.len());
        let mut offset = 0;
        for (_, domain) in self.attributes.iter() {
            let data = match domain {
                Domain::Float => {
                    offset += 5; // TODO maybe hardcode this value in as Float::byte_len() and the same for int and text
                    Data::Float(Float::from_bytes(&record_bytes[offset - 5..offset])?)
                }
                Domain::Integer => {
                    offset += 4;
                    Data::Integer(Integer::from_bytes(&record_bytes[offset - 4..offset])?)
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
                        offset += 5; // TODO maybe hardcode this value in as Float::byte_len() and the same for int and text
                        Data::Float(Float::from_bytes(&records_bytes[offset - 5..offset])?)
                    }
                    Domain::Integer => {
                        offset += 4;
                        Data::Integer(Integer::from_bytes(&records_bytes[offset - 4..offset])?)
                    }
                    Domain::Text => {
                        offset += 100;
                        Data::Text(Text::from_bytes(&records_bytes[offset - 100..offset])?)
                    }
                };
                record.push(data)
            }
            records.push(record);
        }
        Ok(records)
    }

    pub fn rename_attributes(&self, new_attributes: Vec<Identifier>) -> Result<(), Box<dyn Error>> {
        for (i, attri1) in new_attributes.iter().enumerate() {
            for (j, attri2) in new_attributes.iter().enumerate() {
                if i != j && attri1.name() != attri2.name() {
                    return Err(Box::new(DBError::ConstraintError("Cannot have two attributes with the same Identifier in a table")))
                }
            }
        }
        let mut file = OpenOptions::new().write(true).open(&self.file_path)?;

        // just overwrite all front meta since seek-writing wouldn't greatly improve performance at all
        file.write_all(&self.attributes.len().to_be_bytes())?; // 8 bytes

        for (attribute, (_, domain)) in new_attributes.iter().zip(&self.attributes) {
            attribute.write_to_file(&file)?;
            domain.write_to_file(&file)?;
        }
        Ok(())
    }

    fn update(&mut self, record_num: usize, new_values: &Vec<(Identifier, Data)>) -> Result<(), Box<dyn Error>> {
        let mut record = self.read_record(record_num)?;
        for (i, (identifier, domain)) in self.attributes.iter().enumerate() {
            for (check_id, new_data) in new_values {
                if check_id == identifier {
                    record[i] = new_data.clone();
                }
            }
        }

        self.write_record(record)
    }

    pub fn update_all(&mut self, record_nums: Vec<usize>, new_values: Vec<(Identifier, Data)>) -> Result<(), Box<dyn Error>> {
        for record_num in record_nums {
            self.update(record_num, &new_values)?;
        }
        Ok(())
    }

    pub fn delete_all(&mut self, mut record_nums: Vec<usize>) -> Result<(), Box<dyn Error>> {
        let mut mem_table = MemTable::build(self)?; // has some overhead for reading data that isn't necessary
        for record_num in record_nums {
            mem_table.records.swap_remove(record_num);
        }

        match self.bst {
            None => (),
            Some(ref mut bst) => *bst = BST::build(&mem_table)?
        }

        let file = OpenOptions::new().write(true).open(&self.file_path)?;
        file.set_len(self.meta_offset as u64)?;

        for record in mem_table.records {
            self.write_record(record)?;
        }
        self.write_record_count()?;
        Ok(())
    }
}

// If this were to ever work with tables larger than can reasonably be loaded all into memory,
// then it would be best to change a MemTable to give a stream/iterator which loads the records in blocks
// However, for the sake of this mini DBMS, loading the whole table is good enough.
pub struct MemTable {
    pub records: Vec<Vec<Data>>,
    pub attributes: Vec<(Identifier, Domain)>,
    projection: Vec<usize>,
}

impl MemTable {
    pub fn build(table: & Table) ->  Result<Self, Box<dyn Error>> {
        Ok( MemTable {
            records: table.read_all_data()?,
            attributes: table.attributes.iter()
                .map(|(identifier, domain)| {
                    ((*identifier).clone(), domain.clone())
                }).collect(),
            projection: (0..table.attributes.len()).collect() // start with all attributes projected
        })
    }

    pub fn build_from_records(records: Vec<Vec<Data>>, attributes: Vec<(Identifier, Domain)>) ->  Self {
        let attributes_len = attributes.len();
        MemTable {
            records,
            attributes,
            projection: (0..attributes_len).collect() // start with all attributes projected
        }
    }

    pub fn project(&mut self, selected_attris: Vec<&str>) -> Result<(), DBError> {
        let mut new_projection = Vec::new();
        'outer: for attri_num in self.projection.iter() {
            for selected in selected_attris.iter() {
                if *selected == self.attributes[*attri_num].0.name() {
                    new_projection.push(*attri_num);
                    continue 'outer;
                }
                return Err(DBError::ParseError("Could not find attribute to project in the given table."))
            }
        }
        self.projection = new_projection;
        Ok(())
    }

    pub fn print(&self) {
        let mut attribute_lengths = vec![0; self.projection.len()];
        for (i, attri_num) in self.projection.iter().enumerate() {
            let (identifier, _) = &self.attributes[*attri_num];
            attribute_lengths[i] = identifier.name().len() + 4; // two spaces and two pipe characters
        }

        for record in self.records.iter() {
            for (i, attri_num) in self.projection.iter().enumerate() {
                let data = &record[*attri_num];
                let data_len = data.string_len() + 4; // two spaces and two pipe characters
                if data_len > attribute_lengths[i] {
                    attribute_lengths[i] = data_len;
                }
            }
        }

        // loop through all instances of each attribute and check lengths... set each to the longest needed

        let row_length: usize = attribute_lengths.iter().sum();

        // println stuff

        let row_hyphens = vec!["-"; row_length+4].concat();
        println!("{row_hyphens}");

        let mut top_line = Vec::with_capacity(self.attributes.len()*3+1);
        top_line.push("| ".to_string());
        for (i, attri_num) in self.projection.iter().enumerate() {
            let (identifier, _) = &self.attributes[*attri_num];
            let extra_spaces = vec![" "; attribute_lengths[i]-identifier.name().len()].concat();
            let id_string = [identifier.name().to_string(), extra_spaces].concat();
            top_line.push(id_string);
            top_line.push(" | ".to_string());
        }
        top_line.push("\n".to_string());
        println!("{}", top_line.concat());

        println!("{row_hyphens}");

        // print each record
        for record in self.records.iter() {
            let mut new_line = Vec::with_capacity(self.attributes.len()*3+1);
            new_line.push("| ".to_string());
            for (i, attri_num) in self.projection.iter().enumerate() {
                let data = &record[*attri_num];
                let data_string = data.to_string(); // two spaces and two pipe characters
                let extra_spaces = vec![" "; attribute_lengths[i]-data_string.len()].concat();
                let data_string = [data_string, extra_spaces].concat();
                new_line.push(data_string);
                new_line.push(" | ".to_string());
            }
            new_line.push("\n".to_string());
            println!("{}", new_line.concat());
        }

        println!("{row_hyphens}");
    }
}