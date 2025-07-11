use std::env;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let file_path = "./test.db";

    let command = &args[1];

    let mut file_options = OpenOptions::new();

    if command == "SET" {
        file_options.append(true).create(true);
    } else if command == "GET" {
        file_options.read(true);
    } else {
        panic!("Unknown command");
    }

    let file = file_options.open(file_path)?;
    let key = &args[2];

    if command == "SET" {
        let value = &args[3];
        let mut writer = BufWriter::new(file);
        writeln!(writer, "{},{}", key, value)?;
    } else if command == "GET" {
        let buf_reader = BufReader::new(file);
        let mut return_value = String::from("");
        for line in buf_reader.lines() {
            let real_line = line?;
            let (line_key, val) = real_line
                .split_once(',')
                .expect("Failed to split line [{}].\nCheck for db corruption");
            if line_key == key {
                return_value = String::from(val);
            }
        }
        if return_value.is_empty() {
            println!("Could not find value for key {}", key);
        } else {
            println!("Found value: {}", return_value);
        }
    }

    Ok(())
}
