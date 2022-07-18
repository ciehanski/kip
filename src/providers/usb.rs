//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use serde::{Deserialize, Serialize};
// use std::fs::File;
// use std::io::copy;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipUsb {
    pub root_path: PathBuf,
    pub capacity: u64,
}

impl KipUsb {
    pub fn new<P: AsRef<Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.as_ref().to_path_buf(),
            capacity: 0,
        }
    }
}

// fn upload(source: &str, usb: KipUsb) -> Result<()> {
//     // Open file to read
//     // let mut reader = File::open(Path::new(source))?;
//     // // Open file to write
//     // let _ = copy(&mut reader, &mut writer)?;
//     Ok(())
// }
