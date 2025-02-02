use config::Config;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::warn;

// spawns a thread which listens for interrupt signal. Sets flag to true upon signal interruption
pub async fn sig_int_handler(flag: Arc<Mutex<bool>>) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        warn!("Received interrupt signal. Finishing up...");
        let mut flag = flag.try_lock().unwrap();
        *flag = true;
    });
}

// spawns a thread that creates a TcpListener server and that waits for a command to be sent to it,
pub async fn sig_cmd_handler(command: Arc<Mutex<String>>, response: Arc<Mutex<String>>) {
    tokio::spawn(async move {
        let addr = "localhost:8080";
        let listener = TcpListener::bind(addr).await.unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();

            let mut cmd = std::str::from_utf8(&buf[..n]).unwrap();
            cmd = cmd.trim_end_matches(|c: char| !c.is_alphanumeric());
            let formatted_cmd = format!("{} ", cmd);
            cmd = formatted_cmd.as_str();

            loop {
                let command = command.try_lock();
                match command {
                    Ok(mut command) => {
                        *command = cmd.to_string();

                        drop(command);
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }

            let response_content;

            // wait for the response to be set
            loop {
                let response = response.try_lock();
                match response {
                    Ok(mut response) => {
                        if response.len() > 0 {
                            println!("response from scheduler: {}", response);
                            response_content = response.clone();
                            *response = "".to_string();
                            break;
                        }
                        drop(response);
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }

            socket
                .write_all(format!("{}\n", response_content).as_bytes())
                .await
                .unwrap();
        }
    });
}

// checks if a flag is set to true and, if so, exits the program
pub fn check_exit(flag: Arc<Mutex<bool>>) {
    match flag.try_lock() {
        Ok(x) => {
            if *x {
                std::process::exit(0)
            }
        }
        _ => {}
    }
}

// checks returns value of flag
pub fn check_flag(flag: Arc<Mutex<bool>>) -> bool {
    match flag.try_lock() {
        Ok(x) => {
            if *x {
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

// check returns value of cmd, or None if it is not available
pub fn check_cmd(command: Arc<Mutex<String>>) -> Option<String> {
    match command.try_lock() {
        Ok(x) => {
            if x.len() > 0 {
                Some(x.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

pub async fn get_candids_from_stream(
    con: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    options: &StreamReadOptions,
) -> Vec<i64> {
    let result: Option<StreamReadReply> = con
        .xread_options(&[stream.to_owned()], &[">"], options)
        .await
        .unwrap();
    let mut candids: Vec<i64> = Vec::new();
    if let Some(reply) = result {
        for stream_key in reply.keys {
            let xread_ids = stream_key.ids;
            for stream_id in xread_ids {
                let candid = stream_id.map.get("candid").unwrap();
                // candid is a Value type, so we need to convert it to i64
                match candid {
                    redis::Value::BulkString(x) => {
                        // then x is a Vec<u8> type, so we need to convert it an i64
                        let x = String::from_utf8(x.to_vec())
                            .unwrap()
                            .parse::<i64>()
                            .unwrap();
                        // append to candids
                        candids.push(x);
                    }
                    _ => {
                        warn!("Candid unknown type: {:?}", candid);
                    }
                }
            }
        }
    }
    candids
}

pub fn get_check_command_interval(conf: Config, stream_name: &str) -> i64 {
    let table = conf
        .get_table("workers")
        .expect("worker table not found in config");
    let stream_table = table
        .get(stream_name)
        .expect(format!("stream name {} not found in config", stream_name).as_str())
        .to_owned()
        .into_table()
        .unwrap();
    let check_command_interval = stream_table
        .get("command_interval")
        .expect("command_interval not found in config")
        .to_owned()
        .into_int()
        .unwrap();
    return check_command_interval;
}

#[derive(Clone, Debug)]
pub enum WorkerType {
    Alert,
    Filter,
    ML,
}

impl Copy for WorkerType {}

impl fmt::Display for WorkerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str;
        match self {
            WorkerType::Alert => {
                enum_str = "Alert";
            }
            WorkerType::Filter => enum_str = "Filter",
            WorkerType::ML => enum_str = "ML",
        }
        write!(f, "{}", enum_str)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerCmd {
    TERM,
}

impl fmt::Display for WorkerCmd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str;
        match self {
            WorkerCmd::TERM => {
                enum_str = "TERM";
            }
        }
        write!(f, "{}", enum_str)
    }
}
