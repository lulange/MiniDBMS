# MiniDBMS
A mini database management system program. Created for a class project. Runs SQL-esque commands.


## Installing and Running

Currently, the only way to run the MiniDBMS is from source. Luckily, it is entirely written in rust.
Just clone the repository and use cargo to run it.

```console
git clone https://github.com/lulange/mini_dbms.git
cd mini_dbms
cargo run
```

If you don't have cargo, that can be found with the rest of the Rust ecosystem's tools
at https://www.rust-lang.org/tools/install

This generally points to using rustup. On a mac, it will direct you to run this.

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


## Command Specifications

All commands may be written without case-sensitivity.

Commands are (CREATE | USE | DESCRIBE | SELECT | LET | INSERT | UPDATE | DELETE | INPUT | EXIT | RENAME).

Each command is fully defined below:

```
CREATE DATABASE Dbname;
USE Dbname;
```

```
CREATE TABLE TableName ‘(‘ AttrName Domain [PRIMARY KEY] [,AttrName Domain]* ‘)’;
```

Dbname		=> Identifier

TableName	=> Identifier

AttrName	=> Identifier

Identifier	=> alphanumeric*

Domain		=> Integer | Text | Float

Float		=> Integer [. Digit [Digit]]

Text		=> 100 or fewer characters

Integer	    => 32-bit sized integer

*Identifiers may contain all characters ascii-alphanumeric or underscores as long as they are not only numeric.

Creates the given table name with the attributes and types. The first attribute may be specified as the primary key for the table. 
If primary key is specified, it builds a binary Search tree with the given index.


```
SELECT AttrNameList
FROM TableNameList
[WHERE Condition] ‘;’
```

AttrNameList  => AttrName [,AttrName]*

TableNameList => TableName [,TableName]*

RelOp         => <, >, <=, >=, =, !=

Constant      => IntConst | StringConst | FloatConst

IntConst      => 32-bit sized integer

StringConst   => ‘“’ [up to 30 characters] ‘”’

FloatConst	  => 64-bit sized float

Condition     =>  AttrName RelOp (Constant|AttrName) [(and|or) AttrName RelOp Constant|AttrName]*


Displays to the screen the rows (with column headers) that match the select condition or “Nothing found” when there is no match. 
The rows will be numbered e.g. 1., 2., etc.


```
DESCRIBE (ALL | TableName) ‘;’
```

Displays to the screen the listed table or ALL tables and their attributes and types. Also indicate the primary key attributes.

E.g.

STUDENT

NAME: 	 	Text

ID:		    Integer	PRIMARY KEY

EMPLOYEE

Name:		Text

SSN:		Integer	PRIMARY KEY

Salary:	    Float


```
LET TableName
KEY AttrName
<SELECT COMMAND>
```

Stores the result of the SELECT command under the given TableName with AttrName as key. Note that this involves creating a BST based on the key for TableName. Key AttrName must be one of the selected attributes.


```
RENAME TableName ‘(‘ AttrNameList ‘)’ ‘;’
```

Renames all attributes of TableName to the given AttrNameList in the order given. Note that the number of attributes in TableName must
equal the number of attributes in AttrNameList.


```
INSERT TableName
VALUES ‘(‘ V1, V2, .. , Vn ‘)’ ‘;’
```

Checks Domain and Key Constraints for the new tuple. If all is okay, the new tuple is inserted in TableName.
	

```
UPDATE TableName
SET AttrName = Constant [,AttrName = Constant]*
[WHERE Condition] ‘;’
```

Updates tuples from TableName that satisfy the WHERE condition to the new SET values.


```
DELETE TableName [WHERE Condition] ‘;’
```

Deletes tuples from TableName that satisfy the WHERE condition. If WHERE clause is ommitted, then all tuples are deleted and the relation schema for table name is removed from the database.


```
INPUT FileName1 [OUTPUT FileName2];
```

Reads and carries out the commands from FileName1. If FileName2 is specified, the result is written to it.

```
EXIT;
```

Terminates program execution. Note that the schemas and data will be saved before exit and ready for use the next time the program is executed.
