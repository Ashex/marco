//! The miss pool: OS threads where a request whose first pass missed runs
//! its blocking reads, so no tokio worker waits on a disk. A request waits
//! for a permit asynchronously, so its worker keeps serving hits; one that
//! waits too long is refused as busy.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

type Job = Box<dyn FnOnce() + Send>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    /// No thread was free in time.
    Busy,
    /// The job panicked.
    Panicked,
}

/// Counts a job as running until it's done, panic or not.
struct Busy(Arc<AtomicI64>);

impl Busy {
    fn start(n: Arc<AtomicI64>) -> Busy {
        n.fetch_add(1, Ordering::Relaxed);
        crate::metrics::POOL_BUSY.inc();
        Busy(n)
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        crate::metrics::POOL_BUSY.dec();
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct MissPool {
    permits: Arc<Semaphore>,
    tx: Mutex<mpsc::Sender<Job>>,
    threads: usize,
    busy: Arc<AtomicI64>,
}

impl MissPool {
    /// `threads` threads, each running one request at a time.
    pub fn new(threads: usize) -> MissPool {
        let threads = threads.max(1);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("marco-miss-{i}"))
                .spawn(move || loop {
                    let job = rx.lock().unwrap().recv();
                    match job {
                        // a job's panic fails its request, not the thread
                        Ok(job) => drop(std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))),
                        Err(_) => return,
                    }
                })
                .expect("spawn a miss pool thread");
        }
        MissPool { permits: Arc::new(Semaphore::new(threads)), tx: Mutex::new(tx), threads, busy: Default::default() }
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Requests on the pool's threads now.
    pub fn busy(&self) -> i64 {
        self.busy.load(Ordering::Relaxed)
    }

    /// Runs `f` on a pool thread once one is free, waiting at most `wait`
    /// for it. The permit goes with the job, so a request whose caller
    /// gave up still holds its thread until it's done.
    pub async fn run<T: Send + 'static>(
        &self,
        wait: Duration,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, PoolError> {
        let t = Instant::now();
        let permit = match tokio::time::timeout(wait, self.permits.clone().acquire_owned()).await {
            Ok(Ok(p)) => p,
            _ => {
                crate::metrics::BUSY.inc();
                return Err(PoolError::Busy);
            }
        };
        crate::metrics::POOL_WAIT.observe(t.elapsed().as_secs_f64());
        let (reply, rx) = tokio::sync::oneshot::channel();
        let busy = self.busy.clone();
        let job: Job = Box::new(move || {
            let _busy = Busy::start(busy);
            let _permit = permit;
            let _ = reply.send(f());
        });
        self.tx.lock().unwrap().send(job).expect("the miss pool's threads run until exit");
        rx.await.map_err(|_| PoolError::Panicked)
    }
}
