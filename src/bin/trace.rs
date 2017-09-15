use std::io;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use attaca::marshal::{Object, ObjectHash};
use attaca::split::Chunk;
use attaca::trace::{Trace, MarshalTrace, SplitTrace, WriteMarshalledTrace};


pub struct MarshalProgressTrace {
    total_known: usize,
    pb: ProgressBar,
}


impl MarshalProgressTrace {
    pub fn new(pb: ProgressBar) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.green/blue} {pos:>12}/{len:>12} {msg}",
        ));

        Self { total_known: 0, pb }
    }
}


impl MarshalTrace for MarshalProgressTrace {
    fn on_reserve(&mut self, n: usize) {
        self.total_known += n;

        self.pb.set_length(self.total_known as u64);
    }


    fn on_register(&mut self, _object: &Object, object_hash: &ObjectHash, cache_hit: bool) {
        self.pb.inc(1);
        self.pb.set_message(&format!(
            "({}) {}",
            if cache_hit { "stale" } else { "fresh" },
            object_hash
        ));
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
            "[{elapsed_precise}] {bar:40.cyan/blue} {bytes:>12}/{total_bytes:>12} {msg}",
        ));

        Self { pb }
    }
}


impl SplitTrace for SplitProgressTrace {
    fn on_chunk(&mut self, offset: u64, _chunk: &Chunk) {
        self.pb.set_position(offset);
    }
}


impl Drop for SplitProgressTrace {
    fn drop(&mut self) {
        self.pb.finish();
    }
}


pub struct WriteMarshalledProgressTrace {
    pb: ProgressBar,
}


impl WriteMarshalledProgressTrace {
    pub fn new(pb: ProgressBar) -> Self {
        pb.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.yellow/blue} {pos:>12}/{len:>12} {msg}",
        ));

        Self { pb }
    }
}


impl WriteMarshalledTrace for WriteMarshalledProgressTrace {
    fn on_write(&mut self, object_hash: &ObjectHash) {
        self.pb.inc(1);
        self.pb.set_message(&object_hash.to_string());
    }
}


impl Drop for WriteMarshalledProgressTrace {
    fn drop(&mut self) {
        self.pb.finish();
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
    type MarshalTrace = MarshalProgressTrace;
    type SplitTrace = SplitProgressTrace;
    type WriteMarshalledTrace = WriteMarshalledProgressTrace;

    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace {
        let progress_bar = self.multi.add(ProgressBar::new(chunks as u64));

        MarshalProgressTrace::new(progress_bar)
    }

    fn on_split(&mut self, bytes: u64) -> Self::SplitTrace {
        let progress_bar = self.multi.add(ProgressBar::new(bytes));

        SplitProgressTrace::new(progress_bar)
    }

    fn on_write_marshalled(&mut self, objects: usize) -> Self::WriteMarshalledTrace {
        let progress_bar = self.multi.add(ProgressBar::new(objects as u64));

        WriteMarshalledProgressTrace::new(progress_bar)
    }
}
