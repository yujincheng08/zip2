#![cfg(feature = "xz")]

use std::io::{self, Read};
use zip::ZipArchive;

#[test]
fn decompress_xz() -> io::Result<()> {
    let mut v = Vec::new();
    v.extend_from_slice(include_bytes!("data/xz.zip"));
    let mut archive = ZipArchive::new(io::Cursor::new(v)).expect("couldn't open test zip file");

    let mut file = archive.by_name("hello.txt")?;
    assert_eq!("hello.txt", file.name());

    let mut content = Vec::new();
    let mut b = [0u8; 1];
    loop {
        let n = file.read(&mut b)?;
        if n == 0 {
            break;
        }
        content.push(b[0]);
    }
    assert_eq!("Hello world\n", String::from_utf8(content).unwrap());
    Ok(())
}
