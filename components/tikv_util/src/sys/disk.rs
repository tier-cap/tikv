use std::sync::atomic::AtomicBool;

pub static WRITE_PERMISSION: AtomicBool = AtomicBool::new(true);
