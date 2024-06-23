use crc32fast::Hasher;
use lzma_rust::LZMA2Reader;
use std::{
    cell::RefCell, io::{Error, Read, Result}, rc::Rc
};

struct RcCountReader<R: Read> {
    inner: Rc<RefCell<R>>,
    count: Rc<RefCell<usize>>,
}

enum XzReader<R: Read> {
    RawReader(RcCountReader<R>),
    LzmaReader(LZMA2Reader<RcCountReader<R>>),
}

pub struct XzDecoder<R: Read> {
    compressed_reader: XzReader<R>,
    flags: [u8; 2],
    block_begin: usize,
    block_written: usize,
    records: Vec<(usize, usize)>,
}

impl<R: Read> XzDecoder<R> {
    pub fn new(inner: R) -> Self {
        XzDecoder {
            compressed_reader: XzReader::RawReader(RcCountReader::new(inner)),
            flags: [0, 0],
            block_begin: 0,
            block_written: 0,
            records: vec![],
        }
    }
}

impl<R: Read> RcCountReader<R> {
    fn new(inner: R) -> Self {
        RcCountReader {
            inner: Rc::new(RefCell::new(inner)),
            count: Rc::new(RefCell::new(0)),
        }
    }

    fn count(&self) -> usize {
        *self.count.borrow()
    }

    fn reset_count(&self) {
        *self.count.borrow_mut() = 0;
    }
}

impl<R: Read> Read for RcCountReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = self.inner.borrow_mut().read(buf)?;
        *self.count.borrow_mut() += count;
        Ok(count)
    }
}

impl<R: Read> Clone for RcCountReader<R> {
    fn clone(&self) -> Self {
        RcCountReader {
            inner: Rc::clone(&self.inner),
            count: Rc::clone(&self.count),
        }
    }
}

impl<R: Read> XzReader<R> {
    fn into_inner(self) -> Option<R> {
        let reader = match self {
            XzReader::RawReader(reader) => reader,
            XzReader::LzmaReader(reader) => reader.into_inner(),
        };
        Rc::into_inner(reader.inner).map(|r| r.into_inner())
    }
}

fn error<T>(s: &'static str) -> Result<T> {
    Err(Error::new(std::io::ErrorKind::InvalidData, s))
}

fn get_multibyte<R: Read>(input: &mut R, hasher: &mut Hasher) -> Result<u64> {
    let mut result = 0;
    for i in 0..9 {
        let mut b = [0u8; 1];
        input.read_exact(&mut b)?;
        hasher.update(&b);
        let b = b[0];
        result ^= ((b & 0x7F) as u64) << (i * 7);
        if (b & 0x80) == 0 {
            return Ok(result);
        }
    }
    error("Invalid multi-byte encoding")
}

impl<R: Read> Read for XzDecoder<R> {
fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let XzReader::LzmaReader(reader) = &mut self.compressed_reader {
            match reader.read(buf) {
                Ok(0) => {
                    let unpadded_size = reader.get_ref().count() - self.block_begin;
                    self.records.push((unpadded_size, self.block_written));
                    // ignore check here since zip itself will check it
                    let check_size = match self.flags[1] & 0x0F {
                        0 => 0,
                        1 => 4,
                        _ => unreachable!(),
                    };

                    let mut b = vec![0u8; ((4 - (unpadded_size & 0x3)) & 0x3) + check_size];
                    reader.read_exact(b.as_mut_slice())?;
                    if !b.as_slice()[..check_size].iter().all(|&b| b == 0) {
                        return error("Invalid XZ block padding");
                    }

                    self.compressed_reader = XzReader::RawReader(reader.get_ref().clone());
                }
                Ok(n) => {
                    self.block_written += n;
                    return Ok(n);
                } 
                Err(e) => return Err(e),
            }
        }
        let reader = match &mut self.compressed_reader {
            XzReader::RawReader(reader) => reader,
            _ => unreachable!(),
        };


        if reader.count() == 0 {
            let mut b = [0u8; 12];
            match reader.read(&mut b) {
                Ok(0) => return Ok(0),
                Err(e) => return Err(e),
                _ => (),
            }
            if b[..6] != b"\xFD7zXZ\0"[..] {
                return error("Invalid XZ header");
            }
            self.flags = [b[6], b[7]];
            if self.flags[0] != 0 || self.flags[1] & 0xF0 != 0 {
                return error("Invalid XZ stream flags");
            }
            match self.flags[1] & 0x0F {
                0 | 1 => (),
                _ => return error("Unsupported XZ stream flags"),
            }
            let mut digest = Hasher::new();
            digest.update(&self.flags);
            if digest.finalize().to_le_bytes() != b[8..] {
                return error("Invalid XZ stream flags CRC32");
            }
        }

        self.block_begin = reader.count();
        let mut b = [0u8; 1];
        reader.read_exact(&mut b)?;

        let mut digest = Hasher::new();
        digest.update(&b);
        if b[0] == 0 {
            // index
            let num_records = get_multibyte(reader, &mut digest)?;
            if num_records != self.records.len() as u64 {
                return error("Invalid XZ index record count");
            }
            for (unpadded_size, total) in &self.records {
                if get_multibyte(reader, &mut digest)? != *unpadded_size as u64 {
                    return error("Invalid XZ unpadded size");
                }
                if get_multibyte(reader, &mut digest)? != *total as u64 {
                    return error("Invalid XZ uncompressed size");
                }
            }
            let mut size = reader.count() - self.block_begin;
            let mut b = vec![0u8; (4 - (size & 0x3)) & 0x3];
            reader.read_exact(b.as_mut_slice())?;
            if !b.iter().all(|&b| b == 0) {
                return error("Invalid XZ index padding");
            }
            digest.update(b.as_slice());
            size += b.len();
            let mut b = [0u8; 16];
            reader.read_exact(&mut b)?;
            if digest.finalize().to_le_bytes() != b[..4] {
                return error("Invalid XZ index CRC32");
            }
            let mut digest = Hasher::new();
            digest.update(&b[8..14]);
            if digest.finalize().to_le_bytes() != b[4..8] {
                return error("Invalid XZ footer CRC32");
            }
            if b[8..12] != ((size >> 2) as u32).to_le_bytes() {
                return error("Invalid XZ footer size");
            }
            if self.flags != b[12..14] {
                return error("Invalid XZ footer flags");
            }
            if &b[14..16] != b"YZ" {
                return error("Invalid XZ footer magic");
            }
            let mut b = vec![0u8; (4 - (reader.count() & 0x3)) & 0x3];
            reader.read_exact(b.as_mut_slice())?;
            if !b.iter().all(|&b| b == 0) {
                return error("Invalid XZ footer padding");
            }
            reader.reset_count();
            return self.read(buf);
        }

        // block
        let header_end = ((b[0] as usize) << 2) - 1 + reader.count();
        let mut b = [0u8; 1];
        reader.read_exact(&mut b)?;
        digest.update(&b);
        let flags = b[0];
        let num_filters = (flags & 0x03) + 1;

        if flags & 0x3C != 0 {
            return error("Invalid XZ block flags");
        }
        if flags & 0x40 != 0 {
            get_multibyte(reader, &mut digest)?;
        }
        if flags & 0x80 != 0 {
            get_multibyte(reader, &mut digest)?;
        }
        for _ in 0..num_filters {
            let filter_id = get_multibyte(reader, &mut digest)?;
            if filter_id != 0x21 {
                return error("Unsupported XZ filter ID");
            }
            let properties_size = get_multibyte(reader, &mut digest)?;
            if properties_size != 1 {
                return error("Unsupported XZ filter properties size");
            }
            reader.read_exact(&mut b)?;
            if b[0] & 0xC0 != 0 {
                return error("Unsupported XZ filter properties");
            }
            digest.update(&b);
        }
        let mut b = vec![0u8; header_end - reader.count()];
        reader.read_exact(b.as_mut_slice())?;
        if !b.iter().all(|&b| b == 0) {
            return error("Invalid XZ block header padding");
        }
        digest.update(b.as_slice());

        let mut b = [0u8; 4];
        reader.read_exact(&mut b)?;
        if digest.finalize().to_le_bytes() != b {
            return error("Invalid XZ block header CRC32");
        }
        self.block_written = 0;
        let mut reader = LZMA2Reader::new(reader.clone(), 8_388_608u32, None);
        let written = reader.read(buf)?;
        self.block_written += written;
        self.compressed_reader = XzReader::LzmaReader(reader);
        Ok(written)

    }
}

impl<R: Read> XzDecoder<R> {
    pub fn into_inner(self) -> R {
        self.compressed_reader.into_inner().unwrap()
    }
}
