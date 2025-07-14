use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, stdin};
use std::io::{SeekFrom, prelude::*};

struct Environment {
    file_path: String,
    index: HashMap<String, u64>,
}

impl Environment {
    pub fn new(file_path: &String) -> Self {
        return Environment {
            file_path: file_path.clone(),
            index: build_index(file_path).unwrap(),
        };
    }
}

fn build_index(file_path: &String) -> Result<HashMap<String, u64>, std::io::Error> {
    let mut result = HashMap::new();
    let file = OpenOptions::new().read(true).open(file_path)?;
    let buf_reader = BufReader::new(file);

    let mut current_position: u64 = 0;
    for line in buf_reader.lines() {
        let real_line = line?;
        let (line_key, _) = real_line.split_once(',').expect(
            format!(
                "Failed to split line [{}].\nCheck for db corruption",
                real_line
            )
            .as_str(),
        );
        result.insert(line_key.to_string(), current_position);
        current_position += real_line.len() as u64 + 1; // accounting newline here
    }
    return Ok(result);
}

fn get_data(env: &Environment, key: &String) -> Result<String, std::io::Error> {
    let file = OpenOptions::new().read(true).open(&env.file_path)?;
    let mut buf_reader = BufReader::new(file);
    let mut return_value = String::new();
    match env.index.get(key) {
        Some(offset) => {
            let _ = buf_reader.seek(SeekFrom::Start(*offset as u64));
            let mut real_line = String::new();
            let _ = buf_reader.read_line(&mut real_line)?;
            let (line_key, val) = real_line.split_once(',').expect(
                format!(
                    "Failed to split line [{}].\nCheck for db corruption",
                    real_line
                )
                .as_str(),
            );
            if line_key == key {
                return_value = String::from(val);
                return_value.pop(); // remove endline
            } else {
                panic!("index corrupted");
            }
        }
        None => (),
    };
    Ok(return_value)
}

fn set_data(env: &mut Environment, key: &String, value: &String) -> Result<(), std::io::Error> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&env.file_path)?;
    let mut writer = BufWriter::new(file);
    let line = format!("{},{}", key, value);
    writeln!(writer, "{}", line)?;
    env.index.insert(
        key.clone(),
        writer.stream_position()? - line.len() as u64 - 1,
    );
    Ok(())
}

fn handle_command(env: &mut Environment, command_args: &Vec<String>) {
    let command = &command_args[0];
    let key = &command_args[1];
    if command == "SET" {
        let value = &command_args[2];
        let return_value = set_data(env, key, value);
        match return_value {
            Ok(_) => {
                println!("Written key: [{}] value: [{}]", key, value);
            }
            Err(e) => {
                println!("Could not write key-value pair. Error: [{}]", e);
            }
        }
    } else if command == "GET" {
        let return_value = get_data(env, key);
        match return_value {
            Ok(value) => {
                if value.is_empty() {
                    println!("Value not found");
                } else {
                    println!("Found value: [{}]", value);
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
    let mut env = Environment::new(&String::from("./test.db"));
    if !is_interactive {
        handle_command(&mut env, &args);
        return Ok(());
    }
    let stdin = stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(real_line) => {
                print!("> ");
                let command_args: Vec<String> =
                    real_line.splitn(3, ' ').map(|x| String::from(x)).collect();
                handle_command(&mut env, &command_args);
            }
            Err(e) => {
                println!("Failed to work with DB, [{}]", e);
                return Err(e);
            }
        }
    }
    Ok(())
}
