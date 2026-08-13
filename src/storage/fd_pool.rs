//! File Descriptor Pool — pre-opened fds for NVMe object files.
//!
//! Open once per object (on write), reuse on every read, close on delete.
//! Saves open()/close() syscalls on the hot read path.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::RwLock;

use crate::data_type::ObjectId;

pub struct FdPool {
    fds: RwLock<HashMap<u64, RawFd>>,
}

impl FdPool {
    pub fn new() -> Self {
        Self {
            fds: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, oid: ObjectId) -> Option<RawFd> {
        self.fds.read().unwrap().get(&oid.0).copied()
    }

    pub fn insert(&self, oid: ObjectId, fd: RawFd) {
        self.fds.write().unwrap().insert(oid.0, fd);
    }

    pub fn remove(&self, oid: ObjectId) {
        if let Some(fd) = self.fds.write().unwrap().remove(&oid.0) {
            // SAFETY: fd is a valid file descriptor opened by us via libc::open.
            // We own it exclusively (removed from map) and close exactly once.
            unsafe { libc::close(fd) };
        }
    }
}

impl Drop for FdPool {
    fn drop(&mut self) {
        for (_, fd) in self.fds.write().unwrap().drain() {
            // SAFETY: All fds were opened by us via libc::open and are valid.
            // drain() ensures each fd is closed exactly once during shutdown.
            unsafe { libc::close(fd) };
        }
    }
}
