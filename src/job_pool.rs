use colored::*;
use std::sync::{mpsc, Arc, Mutex, PoisonError};
use std::thread;

// Type alias for any thread sending closure.
// Is not related to backup job 'Job' within job.rs.
type Job = Box<dyn FnOnce() + Send + 'static>;

// The types of messages our worker can execute
// and recieve.
enum Message {
    New(Job),
    Terminate,
}

// Stores our sender and worker threads.
pub struct JobPool {
    tx: mpsc::Sender<Message>,
    workers: Vec<Worker>,
}

impl JobPool {
    pub fn new(thread_amt: usize) -> Self {
        // Create tx, rx channels
        let (tx, rx) = mpsc::channel();
        // Allows multiple workers ownership of rx channel
        let rx = Arc::new(Mutex::new(rx));
        // Create a new vec with the size provided
        let mut workers = Vec::with_capacity(thread_amt);
        // Create and add new threads to pool
        for i in 0..thread_amt {
            workers.push(Worker::new(i, Arc::clone(&rx)));
        }
        // Return new job pool
        JobPool { workers, tx }
    }

    pub fn execute<J: FnOnce() + Send + 'static>(&self, job: J) {
        // Send the job defined in the caller's closure
        // to the worker queue, where they will take and
        // perform the job.
        self.tx
            .send(Message::New(Box::new(job)))
            .unwrap_or_else(|e| {
                eprintln!("{} failed to send job to rx channel: {}", "[ERR]".red(), e);
            });
    }
}

impl Drop for JobPool {
    fn drop(&mut self) {
        // Loop through workers and send terminate message
        for _ in &mut self.workers {
            self.tx.send(Message::Terminate).unwrap();
        }
        // Loop through workers and terminate their thread
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, rx: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread_builder = thread::Builder::new()
            .name(format!("kip {}", id))
            .stack_size(32 * 1024);
        let thread = thread_builder
            .spawn(move || loop {
                // Block in a new thread and wait for a job
                // to become available.
                let message = match rx.lock().unwrap_or_else(PoisonError::into_inner).recv() {
                    Ok(m) => m,
                    Err(e) => panic!("{} failed to recv from tx: {}", "[ERR]".red(), e),
                };
                match message {
                    Message::New(job) => {
                        // Recieved a job, now run it. This refers
                        // to the closure defined by the caller.
                        job();
                    }
                    Message::Terminate => {
                        // Break from loop of checking for work,
                        // essentially killing the worker & thread.
                        break;
                    }
                }
            })
            .expect("[ERR] failed to spawn new worker.");

        // Return this bad boy.
        Worker {
            id,
            thread: Some(thread),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute() {
        let pool = JobPool::new(4);
        pool.execute(|| {
            println!("it works!");
        });
    }

    #[test]
    fn test_new_worker() {
        let (_, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let w = Worker::new(1337, Arc::clone(&rx));
        drop(w);
    }

    #[test]
    fn test_new_jobpool() {
        JobPool::new(2);
    }
}
