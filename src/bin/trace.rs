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
            "[{elapsed_precise}] {bar:40.green/blue} marshaled {pos}/{len} chunks, last marshalled {msg}",
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


    fn on_register(&mut self, object_hash: &ObjectHash) {
        self.pb.inc(1);
        self.pb.set_message(&object_hash.to_string());
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
            "[{elapsed_precise}] {bar:40.cyan/blue} split {bytes}/{total_bytes}",
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
    pb: ProgressBar,
}


impl WriteProgressTrace {
    pub fn new(pb: ProgressBar, destination: WriteDestination) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.yellow/blue} wrote {pos}/{len} objects to {prefix}, writing {msg}",
        ));

        pb.enable_steady_tick(500);

        match destination {
            WriteDestination::Local => pb.set_prefix("local"),
            WriteDestination::Remote(&Some(ref name), _) => {
                pb.set_prefix(&format!("remote {}", name))
            }
            WriteDestination::Remote(&None, _) => pb.set_prefix("remote"),
        }

        Self { pb }
    }
}


impl WriteTrace for WriteProgressTrace {
    fn on_write(&mut self, object_hash: &ObjectHash) {
        self.pb.inc(1);
        self.pb.set_message(&object_hash.to_string());
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
        let progress_bar = Weak::upgrade(&self.multi).unwrap().add(ProgressBar::new(
            chunks as u64,
        ));

        MarshalProgressTrace::new(progress_bar)
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

    fn on_write(&mut self, batch: &Batch<Self::BatchTrace>, destination: WriteDestination) -> Self::WriteTrace {
        let progress_bar = self.multi.add(ProgressBar::new(batch.len() as u64));

        WriteProgressTrace::new(progress_bar, destination)
    }
}
