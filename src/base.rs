use std::cmp::Ordering;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use crate::DBError;

/// The Identifier type is a wrapper type for attribute names in tables and database/table names.
/// It enforces ascii-alphanumeracy, a length limit of 19, and an avoidance of restricted keywords.
#[derive(Debug, PartialEq, Clone)]
pub struct Identifier {
    /// name is required to fulfill certain restrictions before it can be set.
    name: String,
}

impl Identifier {
    /// Attempts to create a new Identifier from a str reference.
    /// Will copy the characters out of the given reference to store an owned value.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use dbms::base::Identifier;
    ///
    /// Identifier::from("name1234").unwrap();
    /// ```
    /// 
    /// # Errors
    /// 
    /// Fails when name
    /// - has length greater than 19
    /// - is empty
    /// - is not ascii-alphanumeric
    /// - is a reserved keyword
    pub fn from(name: &str) -> Result<Self, DBError> {
        let name = &name.to_lowercase()[..]; // should already be lowercase but just in case
        if name.len() > 19 {
            return Err(DBError::ParseError(
                "Identifer name cannot be longer than 19 characters",
            ));
        } else if name.is_empty() {
            return Err(DBError::ParseError(
                "Identifer cannot be an empty string",
            ));
        }

        for c in name.chars() {
            if !c.is_ascii_alphanumeric() {
                return Err(DBError::ParseError(
                    "Identifier is not alphanumeric.",
                ));
            }
        }

        match name {
            "create"|
            "database"|
            "select"|
            "use"|
            "describe"|
            "let"|
            "insert"|
            "update"|
            "delete"|
            "input"|
            "exit"|
            "rename"|
            "table"|
            "primary"|
            "key"|
            "where"|
            "from"|
            "all"|
            "values"|
            "set"|
            "output"|
            "none"
             => Err(DBError::ParseError("Cannot set an Identifier to a command name or reserved keyword")),
            _ => Ok(Identifier { name: String::from(name) })
        }
    }

    /// Get a reference to the String wrapped by this Identifier
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Format and write an Identifier to a file.
    /// This adds space characters so that all Identifiers take up the maximum space of 19 bytes.
    /// To aid in file formatting when storing tables.
    /// 
    /// # Errors
    /// 
    /// Fails when cannot write to the file given.
    pub fn write_to_file(&self, mut file: &File) -> Result<(), std::io::Error> {
        let mut buf_to_write: [u8; 19] = [b' '; 19]; // write spaces that can be trimmed
        self.name
            .as_bytes()
            .into_iter()
            .enumerate()
            .for_each(|(i, byte)| {
                buf_to_write[i] = *byte;
            });
        file.write_all(&buf_to_write)
    }
}

/// A base datatype of the overall database. Wraps Strings ensuring
/// that they have length less than 100 characters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Text {
    content: String,
}

impl Text {
    /// Reads an instance of Text back from a u8 slice.
    /// 
    /// # Errors
    /// 
    /// This function expects a byte array with length 100 or less.
    /// It fails when
    /// - The given slice contains more than 100 characters
    /// - The slice fails to be read to a String
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        if bytes.len() > 100 {
            // should never happen since we should only ever read from valid locations but...
            return Err(Box::new(DBError::ParseError(
                "Text should have been stored as 100 or fewer characters.",
            )));
        }
        Ok(Text::from(&String::from_utf8(bytes.to_vec())?.trim())?)
    }

    /// Returns the u8 byte array which represents this Text instance.
    /// These are formatted to always be 100 characters long by appending spaces.
    pub fn to_bytes(&self) -> [u8; 100] {
        let mut buf: [u8; 100] = [b' '; 100]; // write spaces that can be trimmed
        self.content
            .as_bytes()
            .into_iter()
            .enumerate()
            .for_each(|(i, byte)| {
                // overwrite buf with characters from self.content
                buf[i] = *byte;
            });
        buf
    }

    /// Copies a str reference into a Text instance.
    /// Guarantees that the String has length less than or equal to 100.
    /// 
    /// # Errors
    /// - Fails when content has length greater than 100
    pub fn from(content: &str) -> Result<Self, DBError> {
        if content.len() > 100 {
            return Err(DBError::ParseError("Text longer than 100 characters."));
        }

        Ok(Text {
            content: String::from(content),
        })
    }

    /// Returns a reference to the wrapped string in a Text instance
    pub fn content(&self) -> &str {
        &self.content
    }
}

/// A base datatype of the overall database. Wraps i32 values and
/// provides some helpful shortcuts for parsing from string references
#[derive(Clone, Debug, PartialEq)]
pub struct Integer {
    value: i32,
}

impl Integer {
    /// Returns a wrapped i32 from a string reference
    pub fn from_bytes(bytes: &[u8; 4]) -> Self {
        Integer { value: i32::from_be_bytes(*bytes) }
    }

    /// Shortcut to self.value().to_be_bytes()
    /// Returns the byte representation of the wrapped i32.
    pub fn to_bytes(&self) -> [u8; 4] {
        self.value.to_be_bytes()
    }

    /// Returns a constant value for use in file loading.
    pub fn byte_len() -> usize {
        4
    }

    /// Attempts to parse an i32 out of the str reference in value.
    /// 
    /// # Errors
    /// 
    /// Fails when value cannot be parsed into an i32
    pub fn from(value: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Integer { value: value.parse()? })
    }

    /// Returns a reference to the wrapped i32 value.
    pub fn value(&self) -> &i32 {
        &self.value
    }

    /// Returns an Integer reference that wraps the given i32 value.
    pub fn wrap(value: i32) -> Self {
        Integer { value }
    }
}

/// A base datatype of the overall database. Wraps f64 values and
/// provides some helpful shortcuts for parsing from string references
/// and writing to file. Note that this type also rounds off precision.
/// past 2 decimal places.
#[derive(Clone, Debug, PartialEq)]
pub struct Float {
    float: f64,
}

impl Float {
    /// Reads a Float instance from an array of 5 u8 values.
    /// The first 4 bytes are treated as an i32 while the last 1
    /// is treated as the positive or negative fractional addition.
    /// 
    /// # Errors
    /// 
    /// Fails if the 5th byte is greater than or equal to 200
    /// since that has no defined meaning in the format Float uses.
    pub fn from_bytes(bytes: &[u8; 5]) -> Result<Self, DBError> {
        let four_bytes: [u8; 4] = bytes[0..4].try_into().unwrap();
        let mut float = i32::from_be_bytes(four_bytes) as f64; // i32 portion
        let decimal = if bytes[4] >= 200 {
            return Err(DBError::FileFormatError("Float in incorrect format."))
        } else if bytes[4] > 100 { // if last byte > 100, then it is negative -- this is similar to using i8
            -((bytes[4]-100) as f64)
        } else {
            bytes[4] as f64
        };
        float += decimal / 100.; // fractional portion covers two decimal places
        Ok(Float { float })
    }

    /// Converts the Float instance to its 5 byte representation.
    /// This is 4 bytes for an i32 and 1 to store the decimal rounded to the hundredths.
    pub fn to_bytes(&self) -> [u8; 5] {
        let i = self.float as i32; // i32 portion
        let f = ((self.float - i as f64) * 100.).round(); // round off excess precision
        let f: u8 = if f < 0.0 { (-f + 100.0) as u8 } else { f as u8 }; // encode negativity
        let i = i.to_be_bytes();
        [i[0], i[1], i[2], i[3], f]
    }

    /// Attempts to read a Float out of a string reference. Rounds
    /// the internal value given to only contain two decimal places.
    /// 
    /// # Errors
    /// 
    /// Fails float &str given cannot be parsed as a float value.
    pub fn from(float: &str) -> Result<Self, Box<dyn Error>> {
        let mut float = float.parse::<f64>()?;
        float = (float*100.).round() / 100.; // remove extra precision
        Ok(Float{ float })
    }

    /// Returns a constant value for use in file loading.
    pub fn byte_len() -> usize {
        5
    }

    /// Returns a reference to the wrapped float value.
    pub fn value(&self) -> &f64 {
        &self.float
    }

    /// Returns a formatted string that cuts off floating point precision artifacts by rounding. 
    pub fn to_string(&self) -> String {
        let i = self.float as i32;
        let f = ((self.float - i as f64) * 100.).round(); // round to two decimal places
        if f < 0.0 {
            // print negative correctly when integer portion is 0
            let f = -f as u8;
            if i == 0 {
                format!("-{i}.{f}")
            } else {
                format!("{i}.{f}")
            }
        } else {
            let f = f as u8;
            format!("{i}.{f}")
        }
    }

    /// Returns an Integer reference that wraps the given i32 value.
    pub fn wrap(float: f64) -> Self {
        let float = (float*100.).round() / 100.; // remove extra precision
        Float {
            float
        }
    }
}

/// Contains a variant for each datatypes. Provides
/// methods for parsing strings and some other helpful actions
#[derive(Copy, Clone, PartialEq)]
pub enum Domain {
    Integer,
    Text,
    Float,
}

impl Domain {
    /// Attempt to read a domain out of a descriptor string reference.
    /// Only recognizes lowercase values.
    /// 
    /// # Errors
    /// 
    /// Fails when descriptor does not reference one of the three
    /// Domain variants.
    pub fn from(descriptor: &str) -> Result<Self, DBError> {
        match descriptor {
            "text" => Ok(Domain::Text),
            "integer" => Ok(Domain::Integer),
            "float" => Ok(Domain::Float),
            _ => Err(DBError::ParseError("Invalid Domain type.")),
        }
    }

    /// A convenience method which returns
    /// the byte size of the datatype for the Domain variant given
    pub fn size_in_bytes(&self) -> u32 {
        match self {
            Domain::Float => Float::byte_len() as u32,   // i32 + u8 fraction
            Domain::Integer => Integer::byte_len() as u32, // i32
            Domain::Text => 100, // Text has no byte_len() function but always stores itself as 100 bytes
        }
    }

    /// Takes a file and writes the integer which represents the Domain variant in self
    pub fn write_to_file(&self, mut file: &File) -> Result<(), std::io::Error> {
        file.write_all(&[*self as u8])
    }

    /// Returns a string representation for a given Domain variant.
    /// Note that this string cannot be directly read back by Domain::from()
    /// since it includes captialized letters. Must use String.to_lowercase() first.
    pub fn to_string(&self) -> &'static str {
        match self {
            Domain::Float => "Float",
            Domain::Integer => "Integer",
            Domain::Text => "Text",
        }
    }
}

/// A wrapper for Integer, Text, and Float.
/// Contains same variants as Domain, except that all
/// variants contain a payload of the datatype represented
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Integer(Integer),
    Text(Text),
    Float(Float),
}


impl Data {
    /// Attempts to read a Data variant from a slice of bytes.
    /// 
    /// # Errors
    /// 
    /// Fails when bytes cannot be parsed into any Data variant.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        match bytes.len() {
            4 => { // Integer::byte_len()
                Ok(Data::Integer(Integer::from_bytes(bytes.try_into().unwrap())))
            }
            5 => { // Float::byte_len()
                Ok(Data::Float(Float::from_bytes(bytes.try_into().unwrap())?))
            }
            _ => {
                Ok(Data::Text(Text::from_bytes(bytes)?))
            }
        }
    }

    /// Gets a byte representation for the given Data variant that can be read back by
    /// Data::from_bytes(). This differs from simply calling .to_bytes() on the wrapped
    /// datatype since Text values are returned with dynamic size. The Text.to_bytes() method
    /// provides static size storage while this provides dynamic.
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Data::Text(text) => {
                let mut text_vec = text.content.as_bytes().to_vec();
                while text_vec.len() < 5 {
                    text_vec.push(b' '); // to differentiate this from int or float values... these will be trimmed later
                }
                text_vec
            }
            Data::Float(float) => {
                float.to_bytes().to_vec()
            }
            Data::Integer(int) => {
                int.to_bytes().to_vec()
            }
        }
    }

    /// Returns an Ordering based on the comparison of the payload of
    /// two given Data variants.
    /// 
    /// # Panics
    /// 
    /// Panics when given incompatible/unequal variants.
    pub fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Data::Float(f1), Data::Float(f2)) => {
                f1.float.partial_cmp(&f2.float).expect("No Nan or other odd float values allowed")
            }
            (Data::Integer(i1), Data::Integer(i2)) => {
                i1.value.cmp(&i2.value)
            }
            (Data::Text(t1), Data::Text(t2)) => {
                t1.content.cmp(&t2.content)
            }
            _ => {
                panic!("Can't compare Data variants that are incompatible");
            }
        }
    }

    /// Returns the length of the string representation of the given Data variant.
    pub fn string_len(&self) -> usize {
        match self {
            Data::Float(float) => float.to_string().len(),
            Data::Integer(int) => int.value.to_string().len(),
            Data::Text(text) => text.content.len(),
        }
    }

    /// Returns the string representation of the given Data variant.
    pub fn to_string(&self) -> String {
        match self {
            Data::Float(float) => float.to_string(),
            Data::Integer(int) => int.value.to_string(),
            Data::Text(text) => text.content.to_string(),
        }
    }
}