use crate::{
    alert_worker, fake_ml_worker, filter_worker,
    kafka::consume_alerts,
    worker_util::{WorkerCmd, WorkerType},
};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex},
    thread,
};
use tracing::{info, warn};

// Thread pool
// allows spawning, killing, and managing of various worker threads through
// the use of a messages
pub struct ThreadPool {
    pub worker_type: WorkerType,
    pub stream_name: String,
    pub config_path: String,
    pub workers: HashMap<String, Worker>,
    pub senders: HashMap<String, Option<mpsc::Sender<WorkerCmd>>>,
    pub topic: Option<String>,
}

/// Threadpool
///
/// The threadpool manages an array of workers of one type
impl ThreadPool {
    /// Create a new threadpool
    ///
    /// worker_type: a `WorkerType` enum to designate which type of workers this threadpool contains
    /// size: number of workers initially inside of threadpool
    /// stream_name: source stream. e.g. 'ZTF'
    /// config_path: path to config file
    pub fn new(
        worker_type: WorkerType,
        size: usize,
        stream_name: String,
        config_path: String,
        topic: Option<String>,
    ) -> ThreadPool {
        let mut workers = HashMap::new();
        let mut senders = HashMap::new();

        for _ in 0..size {
            let id = uuid::Uuid::new_v4().to_string();
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            workers.insert(
                id.clone(),
                Worker::new(
                    worker_type,
                    id.clone(),
                    Arc::clone(&receiver),
                    stream_name.clone(),
                    config_path.clone(),
                    topic.clone(),
                ),
            );
            senders.insert(id.clone(), Some(sender));
        }

        ThreadPool {
            worker_type,
            stream_name,
            config_path,
            workers,
            senders,
            topic,
        }
    }

    /// remove a worker from the thread pool by id
    ///
    /// id: string identifier for the worker to be removed
    pub fn remove_worker(&mut self, id: String) {
        if let Some(sender) = &self.senders[&id] {
            match sender.send(WorkerCmd::TERM) {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to send terminate message to worker {}: {}", &id, e);
                }
            }
            self.senders.remove(&id);
        }
    }

    // add a new worker to the thread pool
    pub fn add_worker(&mut self) {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        self.workers.insert(
            id.clone(),
            Worker::new(
                self.worker_type,
                id.clone(),
                Arc::clone(&receiver),
                self.stream_name.clone(),
                self.config_path.clone(),
                self.topic.clone(),
            ),
        );
        self.senders.insert(id.clone(), Some(sender));
        info!("Added worker with id: {}", &id);
    }
}

// shut down all workers from the thread pool and drop the threadpool
impl Drop for ThreadPool {
    fn drop(&mut self) {
        // get the ids of all workers
        let ids: Vec<String> = self.senders.keys().cloned().collect();

        for id in ids {
            self.remove_worker(id);
        }

        for (id, worker) in &mut self.workers {
            warn!("Shutting down worker {}: {}", &self.worker_type, &id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

#[tokio::main]
pub async fn kafka_consumer_worker(topic: &str, receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>) {
    consume_alerts(&topic, None, false, 1000, Some(receiver))
        .await
        .unwrap();
}

/// Worker Struct
/// The `worker` struct represents a threaded worker which might serve as
/// one of several possible roles in the processing pipeline. A `worker` is
/// controlled completely by a threadpool and has a listening channel through
/// which it listens for commands from it.
pub struct Worker {
    pub id: String,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// Create a new pipeline worker
    ///
    /// worker_type: an instance of enum `WorkerType`
    /// id: unique string identifier
    /// receiver: receiver by which the owning threadpool communicates with the worker
    /// stream_name: name of the stream worker from. e.g. 'ZTF' or 'WINTER'
    /// config_path: path to the config file we are working with
    fn new(
        worker_type: WorkerType,
        id: String,
        receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>,
        stream_name: String,
        config_path: String,
        topic: Option<String>,
    ) -> Worker {
        let id_copy = id.clone();
        let thread = match worker_type {
            WorkerType::Alert => thread::spawn(|| {
                alert_worker::alert_worker(id, receiver, stream_name, config_path);
            }),
            WorkerType::Filter => thread::spawn(|| {
                let _ = filter_worker::filter_worker(id, receiver, stream_name, config_path);
            }),
            WorkerType::ML => thread::spawn(|| {
                fake_ml_worker::fake_ml_worker(id, receiver, stream_name, config_path);
            }),
            WorkerType::Consumer => {
                // if the topic isn't provided, we can't consume alerts so we throw an error
                match topic {
                    Some(topic) => thread::spawn(move || {
                        kafka_consumer_worker(&topic, receiver);
                    }),
                    None => panic!("No topic provided for consumer worker"),
                }
            }
        };

        Worker {
            id: id_copy,
            thread: Some(thread),
        }
    }
}
