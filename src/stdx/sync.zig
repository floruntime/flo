//! stdx.sync — Replacements for sync primitives removed from std in 0.16.
//!
//! `@import("stdx").Mutex` was replaced by `std.Io.Mutex` which requires an `Io`
//! parameter. For boundary code that just needs a critical section, this
//! pthread-backed mutex is a drop-in for the old API (`init`, `lock`, `unlock`,
//! `tryLock`).

const std = @import("std");

pub const Mutex = struct {
    inner: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,

    pub fn lock(m: *Mutex) void {
        _ = std.c.pthread_mutex_lock(&m.inner);
    }

    pub fn unlock(m: *Mutex) void {
        _ = std.c.pthread_mutex_unlock(&m.inner);
    }

    pub fn tryLock(m: *Mutex) bool {
        return std.c.pthread_mutex_trylock(&m.inner) == .SUCCESS;
    }
};
