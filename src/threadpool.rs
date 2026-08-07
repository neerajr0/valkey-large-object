//! Fixed-size thread pool for blocking NVMe operations.
//! Eliminates ~1ms per-request thread spawn overhead.
//! Workers block on io_uring/channel operations — sized to match expected concurrency.

use crossbeam_channel::{Sender, Receiver, bounded};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A fixed-size thread pool for blocking I/O work.
pub struct ThreadPool {
    sender: Sender<Job>,
    shutdown: Arc<AtomicBool>,
    _workers: Vec<thread::JoinHandle<()>>,
}

/// Global thread pool instance.
static mut POOL: Option<ThreadPool> = None;

pub fn init_pool(size: usize) {
    unsafe {
        POOL = Some(ThreadPool::new(size));
    }
}

pub fn pool() -> &'static ThreadPool {
    unsafe { POOL.as_ref().expect("ThreadPool not initialized") }
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver): (Sender<Job>, Receiver<Job>) = bounded(size * 4);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(size);

        for i in 0..size {
            let rx = receiver.clone();
            let shut = shutdown.clone();

            let handle = thread::Builder::new()
                .name(format!("bigobj-worker-{}", i))
                .spawn(move || {
                    while !shut.load(Ordering::Relaxed) {
                        match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(job) => job(),
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                            Err(_) => break,
                        }
                    }
                })
                .expect("Failed to spawn worker thread");

            workers.push(handle);
        }

        Self { sender, shutdown, _workers: workers }
    }

    /// Submit a job to the pool. Non-blocking if queue has space.
    pub fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).ok();
    }
}
