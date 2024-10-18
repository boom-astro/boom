use std::thread;

use rust_multithread::ThreadPool;

fn main() {
    let mut pool = ThreadPool::new(4);

    let mut count = 0;
    loop {
        println!("heart beat");
        // sleep for 1 second
        thread::sleep(std::time::Duration::from_secs(1));
        count += 1;
        if count == 5 {
            break;
        }
    }

    // get the id of the first worker in the pool
    let id = pool.senders.keys().next().unwrap().clone();

    // shutdown worker with id
    pool.remove_worker(id);

    // add another worker
    pool.add_worker();

    // sleep for 5 seconds
    thread::sleep(std::time::Duration::from_secs(5));
}