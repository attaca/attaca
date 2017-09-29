use std::io;
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use attaca::batch::Batch;
use attaca::marshal::ObjectHash;
use attaca::trace::{Trace, BatchTrace, MarshalTrace, SplitTrace, WriteDestination, WriteTrace};


pub struct MarshalProgressTrace {
    total_known: usize,
    pb: ProgressBar,
}


impl MarshalProgressTrace {
    pub fn new(pb: ProgressBar) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} {msg}",
        ));

        pb.enable_steady_tick(500);

        Self { total_known: 0, pb }
    }
}


impl MarshalTrace for MarshalProgressTrace {
    fn on_reserve(&mut self, n: usize) {
        self.total_known += n;

        self.pb.set_length(self.total_known as u64);
    }


    fn on_hashed(&mut self, object_hash: &ObjectHash) {
        self.pb.inc(1);
        self.pb.set_message(
            &format!("{}...", &object_hash.to_string()[0..8]),
        );
    }
}


impl Drop for MarshalProgressTrace {
    fn drop(&mut self) {
        self.pb.finish();
    }
}


pub struct SplitProgressTrace {
    pb: ProgressBar,
}


impl SplitProgressTrace {
    pub fn new(pb: ProgressBar) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes}",
        ));

        Self { pb }
    }
}


impl SplitTrace for SplitProgressTrace {
    fn on_chunk(&mut self, _offset: u64, chunk: &[u8]) {
        self.pb.inc(chunk.len() as u64);
    }
}


impl Drop for SplitProgressTrace {
    fn drop(&mut self) {
        self.pb.finish();
    }
}


pub struct WriteProgressTrace {
    in_progress: usize,
    total_unique: u64,
    last_written: Option<ObjectHash>,
    pb: ProgressBar,
}


impl WriteProgressTrace {
    pub fn new(total_unique: u64, pb: ProgressBar, destination: WriteDestination) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.yellow/blue} {pos}/{len} {msg} {prefix}",
        ));

        pb.enable_steady_tick(500);

        match destination {
            WriteDestination::Local => pb.set_prefix("local"),
            WriteDestination::Remote(name) => pb.set_prefix(&format!("remote {}", name)),
        }

        let mut new = Self {
            in_progress: 0,
            total_unique,
            last_written: None,
            pb,
        };
        new.update_msg();

        new
    }


    fn update_msg(&mut self) {
        match self.last_written.as_ref() {
            Some(last_written) => {
                self.pb.set_message(&format!(
                    "({}) {}...",
                    self.in_progress,
                    &last_written.to_string()[0..8],
                ))
            }
            None => self.pb.set_message(&format!("({})", self.in_progress)),
        }
    }
}


impl WriteTrace for WriteProgressTrace {
    fn on_begin(&mut self, _object_hash: &ObjectHash) {
        self.in_progress += 1;
        self.update_msg();
    }


    fn on_complete(&mut self, object_hash: &ObjectHash, fresh: bool) {
        self.in_progress -= 1;
        if fresh {
            self.pb.inc(1);
            self.last_written = Some(*object_hash);
        } else {
            self.total_unique -= 1;
            self.pb.set_length(self.total_unique);
        }
        self.update_msg();
    }
}


impl Drop for WriteProgressTrace {
    fn drop(&mut self) {
        self.pb.finish();
    }
}


pub struct BatchProgressTrace {
    multi: Weak<MultiProgress>,
}


impl BatchProgressTrace {
    pub fn new(multi: Weak<MultiProgress>) -> Self {
        Self { multi }
    }
}


impl BatchTrace for BatchProgressTrace {
    type MarshalTrace = MarshalProgressTrace;
    type SplitTrace = SplitProgressTrace;

    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace {
        let multi = Weak::upgrade(&self.multi).unwrap();
        let pb = multi.add(ProgressBar::new(chunks as u64));

        MarshalProgressTrace::new(pb)
    }

    fn on_split(&mut self, bytes: u64) -> Self::SplitTrace {
        let progress_bar = Weak::upgrade(&self.multi).unwrap().add(
            ProgressBar::new(bytes),
        );

        SplitProgressTrace::new(progress_bar)
    }
}


pub struct ProgressTrace {
    multi: Arc<MultiProgress>,
    pb: ProgressBar,
    join_handle: Option<JoinHandle<io::Result<()>>>,
}


impl ProgressTrace {
    pub fn new() -> Self {
        let multi = Arc::new(MultiProgress::new());
        let pb = multi.add(ProgressBar::hidden());

        let multi0 = multi.clone();
        let join_handle = thread::spawn(move || multi0.join());

        Self {
            multi,
            pb,
            join_handle: Some(join_handle),
        }
    }
}


impl Drop for ProgressTrace {
    fn drop(&mut self) {
        self.pb.finish_and_clear();

        self.join_handle.take().map(
            |jh| { jh.join().unwrap().unwrap(); },
        );
    }
}


impl Trace for ProgressTrace {
    type BatchTrace = BatchProgressTrace;
    type WriteTrace = WriteProgressTrace;

    fn on_batch(&mut self) -> Self::BatchTrace {
        BatchProgressTrace::new(Arc::downgrade(&self.multi))
    }

    fn on_write(
        &mut self,
        batch: &Batch<Self::BatchTrace>,
        destination: WriteDestination,
    ) -> Self::WriteTrace {
        let batch_len = batch.len() as u64;
        let progress_bar = self.multi.add(ProgressBar::new(batch_len));

        WriteProgressTrace::new(batch_len, progress_bar, destination)
    }
}
