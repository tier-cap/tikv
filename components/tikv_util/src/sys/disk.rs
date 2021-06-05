// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub static WRITE_PERMISSION: AtomicBool = AtomicBool::new(true);
pub static DISK_RESERVED: AtomicU64 = AtomicU64::new(0);

pub fn set_write_permission() {
    WRITE_PERMISSION.store(true, Ordering::Release);
}
pub fn clear_write_permission() {
    WRITE_PERMISSION.store(false, Ordering::Release);
}
