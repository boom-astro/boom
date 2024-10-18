use std::{
    collections::HashMap, sync::{mpsc, Arc, Mutex}, thread
};

pub struct ThreadPool {
    pub workers: HashMap<String, Worker>,
    pub senders: HashMap<String, Option<mpsc::Sender<Message>>>,
}

pub type Message = String;



impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let mut workers = HashMap::new();
        let mut senders = HashMap::new();

        for _ in 0..size {
            let id = uuid::Uuid::new_v4().to_string();
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            workers.insert(id.clone(), Worker::new(id.clone(), Arc::clone(&receiver)));
            senders.insert(id.clone(), Some(sender));
        }

        ThreadPool {
            workers,
            senders,
        }
    }

    pub fn remove_worker(&mut self, id: String) {
            if let Some(sender) = &self.senders[&id] {
                sender.send("terminate".to_string()).unwrap();
                self.senders.remove(&id);

                // if let Some(worker) = self.workers.get_mut(&id) {
                //     if let Some(thread) = worker.thread.take() {
                //     thread.join().unwrap();
                // }

            }
        }

    pub fn add_worker(&mut self) {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        self.workers.insert(id.clone(), Worker::new(id.clone(), Arc::clone(&receiver)));
        self.senders.insert(id.clone(), Some(sender));
        println!("Added worker with id: {}", &id);
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        // get the ids of all workers
        let ids: Vec<String> = self.senders.keys().cloned().collect();
        
        for id in ids {
            self.remove_worker(id);
        }

        println!("Shutting down all workers.");

        for (id, worker) in &mut self.workers {
            println!("Shutting down worker {}", &id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

pub struct Worker {
    pub id: String,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let id_copy = id.clone();
        let thread = thread::spawn(move || loop {
            // check if we received a message from the channel, telling us to stop
            if let Ok(message) = receiver.lock().unwrap().try_recv() {
                println!("Worker {} received a message: {}", &id_copy, &message);
                break;
            }
            println!("Worker {} is running.", &id_copy);
            thread::sleep(std::time::Duration::from_secs(1));
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}