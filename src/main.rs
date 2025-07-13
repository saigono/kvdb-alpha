use std::env;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, stdin};

fn get_data(file_path: &String, key: &String) -> Result<String, std::io::Error> {
    let file = OpenOptions::new().read(true).open(file_path)?;
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

    Ok(return_value)
}

fn set_data(file_path: &String, key: &String, value: &String) -> Result<(), std::io::Error> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{},{}", key, value)?;
    Ok(())
}

fn handle_command(command_args: &Vec<String>) {
    let file_path = String::from("./test.db");
    let command = &command_args[0];
    let key = &command_args[1];
    if command == "SET" {
        let value = &command_args[2];
        let return_value = set_data(&file_path, key, value);
        match return_value {
            Ok(_) => {
                println!("Written key: [{}] value: [{}]", key, value);
            }
            Err(e) => {
                println!("Could not write key-value pair. Error: [{}]", e);
            }
        }
    } else if command == "GET" {
        let return_value = get_data(&file_path, key);
        match return_value {
            Ok(value) => {
                if value.is_empty() {
                    println!("Value not found");
                } else {
                    println!("Found value: {}", value);
                }
            }
            Err(e) => {
                println!("Could not find value for key [{}]. Error: [{}]", key, e);
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let is_interactive = args[0] == "--interactive";
    if !is_interactive {
        handle_command(&args);
        return Ok(());
    }
    let stdin = stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(real_line) => {
                print!("> ");
                let command_args: Vec<String> =
                    real_line.splitn(3, ' ').map(|x| String::from(x)).collect();
                handle_command(&command_args);
            }
            Err(e) => {
                println!("Failed to work with DB, [{}]", e);
                return Err(e);
            }
        }
    }
    Ok(())
}
