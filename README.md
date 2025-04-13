# MiniDBMS
A mini database management system program. Created for a class project. Runs SQL-esque commands.


## Installing and Running

Currently, the only way to run the MiniDBMS is from source. Luckily, it is entirely written in rust.
Just clone the repository and use cargo to run it.

```console
git clone https://github.com/lulange/MiniDBMS.git
cd MiniDBMS
cargo run
```

## Command Specifications

All commands may be written without case-sensitivity.

Here is a quick key for some commonly used terms:
- '[]' => Brackets denote that the value inside is variable and based on some key like this one
- '*' => A star after a term is used to mean that the term is optional
- Identifier => Ascii-alphanumeric plus underscores with the exception that it cannot all be numeric
- Domain => One of 'Text', 'Integer', or 'Float' case insensitive of course


### Create

```cmd
CREATE DATABASE [Identifier];
```

or

```cmd
CREATE TABLE [Identifier] ([Identifier] [Domain] [Primary Key]*, ...*);
```