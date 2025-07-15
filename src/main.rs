use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions, metadata, read_dir, rename};
use std::io::{BufReader, BufWriter, Seek, stdin};
use std::io::{SeekFrom, prelude::*};
use std::path::Path;

const SEGMENT_THRESHOLD: u64 = 256;
const CURRENT_SEGMENT_SUFFIX: &str = "current";

#[derive(Debug)]
struct Segment {
    file_path: String,
    index: HashMap<String, u64>,
    size: u64,
}

impl Segment {
    pub fn new(file_path: String) -> Self {
        let path = Path::new(&file_path);
        if !path.exists() {
            File::create(&path).unwrap();
        }
        let metadata = metadata(&file_path).unwrap();
        return Segment {
            file_path: file_path.clone(),
            index: build_index(&file_path).unwrap(),
            size: metadata.len(),
        };
    }
}

struct Environment {
    data_path: String,
    file_prefix: String,
    segments: Vec<Segment>,
    write_segment: Segment,
}

impl Environment {
    pub fn new(data_path: &String, prefix: &String) -> Self {
        let paths = read_dir(data_path).unwrap();

        let mut segments: Vec<Segment> = paths
            .filter_map(|path| path.ok())
            .filter(|p| p.file_name().into_string().unwrap().starts_with(prefix))
            .map(|p| Segment::new(p.path().display().to_string()))
            .collect();

        let index = segments
            .iter()
            .position(|s| s.file_path.ends_with(CURRENT_SEGMENT_SUFFIX));

        if index.is_some() {
            segments.remove(index.unwrap());
        }

        return Environment {
            data_path: data_path.clone(),
            file_prefix: prefix.clone(),
            segments: segments,
            write_segment: Environment::new_write_segment(&data_path, &prefix),
        };
    }

    pub fn next_file_name(&self) -> String {
        let file_number: u64 = match self.segments.last() {
            Some(segment) => {
                u64::from_str_radix(segment.file_path.split('.').last().unwrap(), 10).unwrap()
            }
            None => 0,
        };
        let path_to_file =
            Path::new(&self.data_path).join(format!("{}.{:05}", self.file_prefix, file_number + 1));
        return path_to_file.display().to_string();
    }

    fn new_write_segment(data_path: &String, file_prefix: &String) -> Segment {
        Segment::new(
            Path::new(data_path)
                .join(format!("{}.{}", file_prefix, CURRENT_SEGMENT_SUFFIX))
                .display()
                .to_string(),
        )
    }

    pub fn retire_write_segment(&mut self) {
        // we have only one write thread, so this is fine
        let next_file_name = self.next_file_name();
        rename(&self.write_segment.file_path, &next_file_name).unwrap();
        self.segments.push(Segment::new(next_file_name));
        self.write_segment = Environment::new_write_segment(&self.data_path, &self.file_prefix);
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
        current_position += real_line.len() as u64 + 1; // accounting for newline here
    }
    return Ok(result);
}

fn get_data(env: &Environment, key: &String) -> Result<String, std::io::Error> {
    match get_data_from_segment(&env.write_segment, key) {
        Ok(value) => {
            if !value.is_empty() {
                return Ok(value);
            }
        }
        Err(e) => {
            return Err(e);
        }
    }
    for segment in env.segments.iter().rev() {
        match get_data_from_segment(segment, key) {
            Ok(value) => {
                if !value.is_empty() {
                    return Ok(value);
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(String::new())
}

fn get_data_from_segment(segment: &Segment, key: &String) -> Result<String, std::io::Error> {
    let file = OpenOptions::new().read(true).open(&segment.file_path)?;
    let mut buf_reader = BufReader::new(file);
    let mut return_value = String::new();
    match segment.index.get(key) {
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
    if env.write_segment.size > SEGMENT_THRESHOLD {
        env.retire_write_segment();
    }
    set_data_to_segment(&mut env.write_segment, key, value)
}

fn set_data_to_segment(
    segment: &mut Segment,
    key: &String,
    value: &String,
) -> Result<(), std::io::Error> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&segment.file_path)?;
    let mut writer = BufWriter::new(file);
    let line = format!("{},{}", key, value);
    writeln!(writer, "{}", line)?;
    segment.index.insert(
        key.clone(),
        writer.stream_position()? - line.len() as u64 - 1,
    );
    segment.size += line.len() as u64 + 1;
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
    // TODO: create directory if not exists
    let mut env = Environment::new(&String::from("./data/"), &String::from("db"));
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
