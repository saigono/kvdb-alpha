use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions, metadata, read_dir, remove_file, rename};
use std::io::{BufReader, BufWriter, Seek, stdin};
use std::io::{SeekFrom, prelude::*};
use std::path::Path;

const SEGMENT_THRESHOLD: u64 = 256;
const CURRENT_SEGMENT_SUFFIX: &str = "current";
const DELETE_TERMINATOR: &str = "";

#[derive(Debug)]
struct Segment {
    file_path: String,
    index: HashMap<String, u64>,
    size: u64,
}

#[derive(Debug)]
enum SegmentError {
    Io(std::io::Error),
    KeyDeleted,
}

impl From<std::io::Error> for SegmentError {
    fn from(err: std::io::Error) -> SegmentError {
        SegmentError::Io(err)
    }
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

    pub fn get_data(&self, key: &String) -> Result<String, SegmentError> {
        let file = OpenOptions::new().read(true).open(&self.file_path)?;
        let mut buf_reader = BufReader::new(file);
        let mut return_value = String::new();
        let mut found = false;
        match self.index.get(key) {
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
                    found = true;
                } else {
                    panic!("index corrupted");
                }
            }
            None => (),
        };
        if found && return_value == DELETE_TERMINATOR {
            return Err(SegmentError::KeyDeleted);
        }
        Ok(return_value)
    }

    pub fn save_data(&mut self, key: &String, value: &String) -> Result<(), std::io::Error> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)?;
        let mut writer = BufWriter::new(file);
        let line = format!("{},{}", key, value);
        writeln!(writer, "{}", line)?;
        self.index.insert(
            key.clone(),
            writer.stream_position()? - line.len() as u64 - 1,
        );
        self.size += line.len() as u64 + 1;
        Ok(())
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
            // TODO: do not build segment for CURRENT here
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
        let file_number = self
            .segments
            .iter()
            .map(|s| u64::from_str_radix(s.file_path.split('.').last().unwrap(), 10).unwrap())
            .max()
            .unwrap();
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

    pub fn compact_segments(&mut self) -> Result<(), std::io::Error> {
        // This function is blocking an env, need to rewrite
        let mut total_data: HashMap<String, String> = HashMap::new();
        for segment in self.segments.iter() {
            let file = OpenOptions::new().read(true).open(&segment.file_path)?;
            let buf_reader = BufReader::new(file);
            for line in buf_reader.lines() {
                let real_line = line?;
                let (line_key, val) = real_line.split_once(',').unwrap();
                if val == DELETE_TERMINATOR {
                    total_data.remove(&line_key.to_string());
                } else {
                    total_data.insert(line_key.to_string(), val.to_string());
                }
            }
        }
        let mut new_segments: Vec<Segment> = Vec::new();
        let mut current_segment = Segment::new(self.next_file_name());
        for (key, val) in total_data {
            if current_segment.size > SEGMENT_THRESHOLD {
                new_segments.push(current_segment);
                current_segment = Segment::new(self.next_file_name());
            }
            current_segment.save_data(&key, &val)?;
        }
        new_segments.push(current_segment);
        let filenames: Vec<String> = self.segments.iter().map(|s| s.file_path.clone()).collect();
        for file_path in filenames {
            remove_file(file_path)?;
        }
        self.segments = new_segments;
        Ok(())
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

fn get_data(env: &Environment, key: &String) -> Result<String, SegmentError> {
    match env.write_segment.get_data(key) {
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
        match segment.get_data(key) {
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

fn set_data(env: &mut Environment, key: &String, value: &String) -> Result<(), std::io::Error> {
    if env.write_segment.size > SEGMENT_THRESHOLD {
        env.retire_write_segment();
    }
    env.write_segment.save_data(key, value)
}

fn handle_command(env: &mut Environment, command_args: &Vec<String>) {
    let command = &command_args[0];
    if command == "SET" {
        let key = &command_args[1];

        let value = &command_args[2];
        if value.is_empty() {
            println!("Empty value, ignoring");
            return;
        }
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
        let key = &command_args[1];

        let return_value = get_data(env, key);
        match return_value {
            Ok(value) => {
                if value.is_empty() {
                    println!("Value not found");
                } else {
                    println!("Found value: [{}]", value);
                }
            }
            Err(e) => match e {
                SegmentError::Io(e) => {
                println!("Could not find value for key [{}]. Error: [{:?}]", key, e);
                },
                SegmentError::KeyDeleted => {
                    println!("Value not found (actually deleted)");
                }
            }
        }
    } else if command == "COMPACT" {
        match env.compact_segments() {
            Ok(_) => {
                println!("Segments compacted");
            }
            Err(e) => {
                println!("Failed to compact segments: [{}]", e);
            }
        }
    } else if command == "DELETE" {
        let key = &command_args[1];
        let return_value = set_data(env, key, &DELETE_TERMINATOR.to_string());
        match return_value {
            Ok(_) => {
                println!("Deleted key: [{}]", key);
            }
            Err(e) => {
                println!("Could not write key-value pair. Error: [{}]", e);
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
