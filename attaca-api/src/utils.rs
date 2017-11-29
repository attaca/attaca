use std::fmt;


pub struct Indenter<'a, 'b: 'a> {
    fmt: &'a mut fmt::Formatter<'b>,
    on_newline: bool,
}

impl<'a, 'b: 'a> Indenter<'a, 'b> {
    pub fn new(fmt: &'a mut fmt::Formatter<'b>) -> Indenter<'a, 'b> {
        Indenter {
            fmt,
            on_newline: false,
        }
    }
}

impl<'a, 'b: 'a> fmt::Write for Indenter<'a, 'b> {
    fn write_str(&mut self, mut s: &str) -> fmt::Result {
        while !s.is_empty() {
            if self.on_newline {
                self.fmt.write_str("    ")?;
            }

            let split = match s.find('\n') {
                Some(pos) => {
                    self.on_newline = true;
                    pos + 1
                }
                None => {
                    self.on_newline = false;
                    s.len()
                }
            };
            self.fmt.write_str(&s[..split])?;
            s = &s[split..];
        }

        Ok(())
    }
}
