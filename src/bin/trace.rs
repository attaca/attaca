use std::io;
use std::sync::{Arc, Weak, Mutex};
use std::thread::{self, JoinHandle};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use attaca::marshal::ObjectHash;
use attaca::trace::Trace;


pub struct ProgressInner {
    to_split: u64,
    split: u64,
    object_count: u64,
    in_flight: u64,
    written: u64,
    fresh: u64,

    remote: Option<String>,

    split_progress: ProgressBar,
    write_progress: ProgressBar,

    join_handle: Option<JoinHandle<io::Result<()>>>,
}


impl ProgressInner {
    fn update_split_progress(&mut self) {
        self.split_progress.set_length(self.to_split);
        self.split_progress.set_position(self.split);
    }

    fn update_write_progress(&mut self) {
        self.write_progress.set_length(self.object_count);
        self.write_progress.set_position(self.written);
        self.write_progress.set_message(
            &format!("({})", self.in_flight),
        );
    }
}


impl Drop for ProgressInner {
    fn drop(&mut self) {
        self.split_progress.finish();
        self.write_progress.finish();

        self.join_handle.take().map(
            |jh| { jh.join().unwrap().unwrap(); },
        );
    }
}


#[derive(Clone)]
pub struct Progress {
    inner: Arc<Mutex<ProgressInner>>,
}


impl Progress {
    pub fn new(remote: Option<String>) -> Self {
        let multi = MultiProgress::new();

        let split_progress = multi.add(ProgressBar::new(0));
        split_progress.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes}",
        ));


        let write_progress = multi.add(ProgressBar::new(0));
        write_progress.set_style(ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} {msg}",
        ));
        write_progress.enable_steady_tick(500);

        let join_handle = Some(thread::spawn(move || multi.join()));

        Self {
            inner: Arc::new(Mutex::new(ProgressInner {
                to_split: 0,
                split: 0,
                object_count: 0,
                in_flight: 0,
                written: 0,
                fresh: 0,

                remote,

                split_progress,
                write_progress,

                join_handle,
            })),
        }
    }
}


impl Trace for Progress {
    fn on_split_begin(&self, size: u64) {
        let mut inner = self.inner.lock().unwrap();

        inner.to_split += size;
        inner.update_split_progress();
    }

    fn on_split_chunk(&self, _offset: u64, chunk: &[u8]) {
        let mut inner = self.inner.lock().unwrap();

        inner.split += chunk.len() as u64;
        inner.update_split_progress();
    }

    fn on_marshal_process(&self, _object_hash: &ObjectHash) {
        let mut inner = self.inner.lock().unwrap();

        inner.object_count += 1;
        inner.update_write_progress();
    }

    fn on_write_object_start(&self, _object_hash: &ObjectHash) {
        let mut inner = self.inner.lock().unwrap();

        inner.in_flight += 1;
        inner.update_write_progress();
    }

    fn on_write_object_finish(&self, _object_hash: &ObjectHash, fresh: bool) {
        let mut inner = self.inner.lock().unwrap();

        inner.in_flight -= 1;
        if fresh {
            inner.fresh += 1;
        }
        inner.written += 1;
        inner.update_write_progress();
    }
}
