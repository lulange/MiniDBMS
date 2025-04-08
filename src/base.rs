use core::panic;
use std::cmp::Ordering;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use crate::DBError;

#[derive(Debug, PartialEq, Clone)]
pub struct Identifier {
    name: String,
}

// TODO disallow identifiers from being commands
impl Identifier {
    pub fn from(name: &str) -> Result<Self, Box<dyn Error>> {
        if name.len() > 19 {
            return Err(Box::new(DBError::ParseError(
                "Identifer name cannot be longer than 19 characters",
            )));
        } else if name == "" {
            return Err(Box::new(DBError::ParseError(
                "Identifer cannot be an empty string",
            )));
        }

        for c in name.chars() {
            if !c.is_ascii_alphanumeric() {
                return Err(Box::new(DBError::ParseError(
                    "Identifier is not alphanumeric.",
                )));
            }
        }

        Ok(Identifier {
            name: String::from(name),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Text {
    content: String,
}

impl Text {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        if bytes.len() != 100 {
            // should never happen since we should only ever read from valid locations but...
            return Err(Box::new(DBError::ParseError(
                "Text should have been stored as 100 characters.",
            )));
        }
        Text::from(&String::from_utf8(bytes.to_vec())?.trim())
    }

    pub fn to_bytes(&self) -> [u8; 100] {
        let mut buf_to_write: [u8; 100] = [b' '; 100]; // write spaces that can be trimmed
        self.content
            .as_bytes()
            .into_iter()
            .enumerate()
            .for_each(|(i, byte)| {
                buf_to_write[i] = *byte;
            });
        buf_to_write
    }

    pub fn from(content: &str) -> Result<Self, Box<dyn Error>> {
        if content.len() > 100 {
            return Err(Box::new(DBError::ParseError(
                "Text longer than 100 characters.",
            )));
        }

        Ok(Text {
            content: String::from(content),
        })
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn wrap(content: &str) -> Self { // panics instead of returning an error
        if content.len() > 100 {
            panic!("Tried to wrap content into a Text instance with improper length.")
        }
        Text { content: String::from(content) }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Integer {
    value: i32,
}

impl Integer {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let four_bytes: [u8; 4] = bytes.try_into()?;
        Ok(Integer {
            value: i32::from_be_bytes(four_bytes),
        })
    }

    pub fn to_bytes(&self) -> [u8; 4] {
        self.value.to_be_bytes()
    }

    pub fn from(value: &str) -> Result<Self, Box<dyn Error>> {
        let value = value.parse()?;

        Ok(Integer { value })
    }

    pub fn value(&self) -> &i32 {
        &self.value
    }

    pub fn wrap(value: i32) -> Self {
        Integer { value }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Float {
    float: f64,
}

impl Float {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let four_bytes: [u8; 4] = bytes[0..4].try_into()?;
        let mut float = i32::from_be_bytes(four_bytes) as f64;
        let decimal = if bytes[4] > 100 {
            -((bytes[4]-100) as f64)
        } else {
            bytes[4] as f64
        };
        float += decimal / 100.;
        Ok(Float {
            float,
        })
    }

    pub fn to_bytes(&self) -> [u8; 5] {
        let i = self.float as i32;
        let f = ((self.float - i as f64) * 100.).round();
        let f: u8 = if f < 0.0 { (-f + 100.0) as u8 } else { f as u8 };
        let i = i.to_be_bytes();
        [i[0], i[1], i[2], i[3], f]
    }

    pub fn from(float: &str) -> Result<Self, Box<dyn Error>> {
        let mut float = float.parse::<f64>()?;
        float = (float*100.).round() / 100.; // remove extra precision
        Ok(Float{ float })
    }

    pub fn value(&self) -> &f64 {
        &self.float
    }

    pub fn to_string(&self) -> String {
        let i = self.float as i32;
        let f = ((self.float - i as f64) * 100.).round();
        if f < 0.0 {
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

    pub fn wrap(float: f64) -> Self {
        let float = (float*100.).round() / 100.; // remove extra precision
        Float {
            float
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum Domain {
    Integer,
    Text,
    Float,
}

impl Domain {
    pub fn from(descriptor: &str) -> Result<Self, Box<dyn Error>> {
        match descriptor {
            "text" => Ok(Domain::Text),
            "integer" => Ok(Domain::Integer),
            "float" => Ok(Domain::Float),
            _ => Err(Box::new(DBError::ParseError(
                "Invalid Domain type {descriptor}",
            ))),
        }
    }

    pub fn size_in_bytes(domain: &Domain) -> u32 {
        match domain {
            Domain::Float => 5,   // i32 + u8 fraction
            Domain::Integer => 4, // i32
            Domain::Text => 100,
        }
    }

    pub fn write_to_file(&self, mut file: &File) -> Result<(), std::io::Error> {
        file.write_all(&[*self as u8])
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            Domain::Float => "Float",
            Domain::Integer => "Integer",
            Domain::Text => "Text",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    // same as Domain except contains values
    Integer(Integer),
    Text(Text),
    Float(Float),
}

impl Eq for Data {}


impl Data {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        match bytes.len() {
            4 => {
                Ok(Data::Integer(Integer::from_bytes(bytes)?))
            }
            5 => {
                Ok(Data::Float(Float::from_bytes(bytes)?))
            }
            _ => {
                Ok(Data::Text(Text::from_bytes(bytes)?))
            }
        }
    }

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

    pub fn string_len(&self) -> usize {
        match self {
            Data::Float(float) => float.to_string().len(),
            Data::Integer(int) => int.value.to_string().len(),
            Data::Text(text) => text.content.len(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Data::Float(float) => float.to_string(),
            Data::Integer(int) => int.value.to_string(),
            Data::Text(text) => text.content.to_string(),
        }
    }

    pub fn domain(&self) -> Domain {
        match self {
            Data::Float(_) => Domain::Float,
            Data::Integer(_) => Domain::Integer,
            Data::Text(_) => Domain::Text
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");


    #[test]
    fn to_bytes_and_back() {
        let f = Float::wrap(-4.1);
        dbg!(&f);
        println!("{}", f.to_string());
        let g = Float::from_bytes(&f.to_bytes()).unwrap();
        dbg!(&g);
        println!("{}", g.to_string());
        assert_eq!(f, g);
    }
}