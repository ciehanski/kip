//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use criterion::{criterion_group, criterion_main, Criterion};
use kip::{chunk, crypto, job};
use std::fs::read;
use std::path::PathBuf;

fn criterion_benchmark(c: &mut Criterion) {
    let file = read("test/vandy.jpg").unwrap();
    let encrypted = crypto::encrypt(&file, "hunter2").unwrap();
    c.bench_function("encrypt_small_file", |b| {
        b.iter(|| crypto::encrypt(b"g", "hunter2"))
    });
    c.bench_function("encrypt_med_file", |b| {
        b.iter(|| crypto::encrypt(&file, "hunter2"))
    });
    c.bench_function("decrypt_med_file", |b| {
        b.iter(|| crypto::decrypt(&encrypted, "hunter2"))
    });
    c.bench_function("chunk_med_file", |b| {
        b.iter(|| chunk::chunk_file(&encrypted))
    });
    c.bench_function("get_files_amt", |b| {
        let mut j = job::Job::new("test1", "hunter2", "", "", "testing1", "us-east-1");
        j.files
            .push(job::KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files
            .push(job::KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files
            .push(job::KipFile::new(PathBuf::from("test/vandy.jpg")));
        b.iter(|| j.get_files_amt())
    });
    // c.bench_function("extract_salt_nonce", |b| {
    //     b.iter(|| crypto::extract_salt_nonce(&encrypted))
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
