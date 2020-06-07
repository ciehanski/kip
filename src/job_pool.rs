use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Job = Box<dyn FnBox + Send + 'static>;

pub struct JobPool {
    workers: Vec<Worker>,
    tx: mpsc::Sender<Job>,
}

impl JobPool {
    pub fn new(size: usize) -> Self {
        // Ensure size is greater than 0
        assert!(size > 0);
        // Create tx, rx channels
        let (tx, rx) = mpsc::channel();
        // Allows multiple workers ownership of rx channel
        let rx = Arc::new(Mutex::new(rx));
        // Create a new vec with the size provided
        let mut workers = Vec::with_capacity(size);
        // Create and add new threads to pool
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&rx)));
        }
        // Return new job pool
        JobPool { workers, tx }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.tx.send(job).unwrap();
    }
}

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, rx: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let job = rx.lock().unwrap().recv().unwrap();

            job.call_box();
        });

        Worker { id, thread }
    }
}
