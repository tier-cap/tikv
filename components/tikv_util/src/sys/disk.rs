// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use fail::fail_point;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub static DISK_FULL: AtomicBool = AtomicBool::new(false);
pub static DISK_RESERVED: AtomicU64 = AtomicU64::new(0);

pub fn set_disk_full() {
    DISK_FULL.store(true, Ordering::Release);
}
pub fn clear_disk_full() {
    DISK_FULL.store(false, Ordering::Release);
}
#[cfg(feature = "failpoints")]
fn disk_full_precheck() -> bool {
    println!("failpoints cfged");
    fail_point!("disk_full_t", |_| {
        println!("failpoints.disk_full_t cfged");
        return true;
    });
    return false;
}
#[cfg(not(feature = "failpoints"))]
fn disk_full_precheck() -> bool {
    println!("failpoints not cfg");
    return false;
}

pub fn is_disk_full() -> bool {
    return disk_full_precheck() || DISK_FULL.load(Ordering::Acquire);
}
pub fn set_disk_reserved(v: u64) {
    let mut value = v;
    if v == 0 {
        value = 5 * 1024 * 1024 * 1024;
    }
    DISK_RESERVED.store(value, Ordering::Release);
}
pub fn get_disk_reserved() -> u64 {
    DISK_RESERVED.load(Ordering::Acquire)
}
