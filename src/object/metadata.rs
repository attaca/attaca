use store::Handle;

#[derive(Debug, Clone)]
pub struct Metadata<H: Handle> {
    raw: String,
    handles: Vec<H>,
}

impl<H: Handle> Metadata<H> {
    pub fn new(raw: String, handles: Vec<H>) -> Self {
        Self { raw, handles }
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn as_handles(&self) -> &[H] {
        &self.handles
    }
}
