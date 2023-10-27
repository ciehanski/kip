//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

#![warn(clippy::all)]

pub mod chunk;
pub mod cli;
pub mod compress;
pub mod conf;
pub mod crypto;
pub mod job;
pub mod providers;
pub mod run;
pub mod smtp;

/// A simple macro to remove some boilerplate
/// on error or exiting kip.
#[macro_export]
macro_rules! terminate {
    ($xcode:expr, $($arg:tt)*) => {{
        let res = std::fmt::format(format_args!($($arg)*));
        tracing::error!("{res}");
        eprintln!("{res}");
        std::process::exit($xcode);
    }};
    // Allows trailing comma
    ($xcode:expr, $($arg:tt)*,) => {{
        let res = std::fmt::format(format_args!($($arg)*));
        tracing::error!("{res}");
        eprintln!("{res}");
        std::process::exit($xcode);
    }};
}
