//! Rows of an export file: zstd-compressed ClickHouse RowBinary. Strings
//! are a LEB128 length, then bytes; the numbers are little-endian.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{bail, Context, Result};

pub struct Rows {
    r: BufReader<zstd::stream::read::Decoder<'static, BufReader<File>>>,
    pub bytes: u64,
}

impl Rows {
    pub fn open(path: &Path) -> Result<Rows> {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let d = zstd::stream::read::Decoder::new(f)?;
        Ok(Rows { r: BufReader::with_capacity(1 << 20, d), bytes: 0 })
    }

    fn varint(&mut self) -> Result<Option<u64>> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let buf = self.r.fill_buf()?;
            if buf.is_empty() {
                if shift == 0 {
                    return Ok(None);
                }
                bail!("export ends inside a length");
            }
            let b = buf[0];
            self.r.consume(1);
            self.bytes += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(Some(v));
            }
            shift += 7;
            if shift > 63 {
                bail!("bad length in export");
            }
        }
    }

    /// Reads the next row's `cols.len()` String columns; false at the end.
    pub fn next(&mut self, cols: &mut [Vec<u8>]) -> Result<bool> {
        for (i, c) in cols.iter_mut().enumerate() {
            let Some(n) = self.varint()? else {
                if i == 0 {
                    return Ok(false);
                }
                bail!("export ends inside a row");
            };
            c.resize(n as usize, 0);
            self.r.read_exact(c)?;
            self.bytes += n;
        }
        Ok(true)
    }

    pub fn u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.r.read_exact(&mut b)?;
        self.bytes += 1;
        Ok(b[0])
    }

    /// A little-endian Int64 column (time_us).
    pub fn i64(&mut self) -> Result<i64> {
        let mut b = [0u8; 8];
        self.r.read_exact(&mut b)?;
        self.bytes += 8;
        Ok(i64::from_le_bytes(b))
    }
}
