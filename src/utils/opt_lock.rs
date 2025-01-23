#![allow(unused)]

use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub enum OptLockError {
    VersionMismatch,
    Locked,
    Obsoleted,
}

use OptLockError::*;

type OptResult<T> = Result<T, OptLockError>;

/// # NOTICE
///
/// The most suitable way to implement this optimistic lock is to implement it in an intrusive way.
///
/// **This implementation should not be used**.
#[derive(Debug)]
pub struct OptLock<T> {
    inner: UnsafeCell<T>,

    // version: 62 bit | lock: 1 bit | obsolete: 1 bit |
    version: AtomicU64,
}

// OptLock is thread safe.
// If the `T` is `Send`, then means `OptLock<T>` is `Send`.
// If the `T` is `Send + Sync`, then means `OptLock<T>` is `Sync`.
unsafe impl<T: Send> Send for OptLock<T> {}
unsafe impl<T: Send + Sync> Sync for OptLock<T> {}

impl<T> OptLock<T> {
    pub fn new(data: T) -> Self {
        if !cfg!(test) {
            panic!("This implementation should not be used");
        }

        Self {
            inner: UnsafeCell::new(data),
            version: AtomicU64::new(0),
        }
    }

    pub fn read(&self) -> OptResult<OptReadGuard<'_, T>> {
        let version = self.check_version()?;
        Ok(OptReadGuard {
            lock: self,
            locked_version: version,
        })
    }

    pub fn write(&self) -> OptResult<OptWriteGuard<'_, T>> {
        let version = self.check_version()?;

        match self.version.compare_exchange(
            version,
            Self::mark_lock(version),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(OptWriteGuard { lock: self }),
            Err(_) => Err(VersionMismatch),
        }
    }

    fn mark_lock(version: u64) -> u64 {
        version + 0b10
    }

    pub fn mark_obsolte(self) {
        self.version.fetch_or(0b01, Ordering::Release);
    }

    fn is_obsolted(version: u64) -> bool {
        version & 0b01 != 0
    }

    fn is_locked(version: u64) -> bool {
        version & 0b10 != 0
    }

    fn check_version(&self) -> OptResult<u64> {
        let version = self.version.load(Ordering::Acquire);
        if Self::is_obsolted(version) {
            return Err(Obsoleted);
        }
        if Self::is_locked(version) {
            return Err(Locked);
        }
        Ok(version)
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    pub unsafe fn inner(&self) -> &T {
        &*(self.inner.get() as *const _)
    }
}

impl<T> From<T> for OptLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

#[derive(Debug)]
pub struct OptReadGuard<'a, T: 'a> {
    lock: &'a OptLock<T>,
    locked_version: u64,
}

impl<T> OptReadGuard<'_, T> {
    pub fn check_version(&self) -> OptResult<()> {
        if self.locked_version == self.lock.check_version()? {
            Ok(())
        } else {
            Err(VersionMismatch)
        }
    }

    pub fn unlock(self) -> Result<(), OptLockError> {
        self.check_version()
    }
}

impl<T> Deref for OptReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.lock.inner.get().as_ref().unwrap() }
    }
}

#[derive(Debug)]
pub struct OptWriteGuard<'a, T: 'a> {
    lock: &'a OptLock<T>,
}

impl<T> OptWriteGuard<'_, T> {
    pub fn mark_obsolte(&mut self) {
        self.lock.version.fetch_or(0b01, Ordering::Release);
    }
}

impl<T> Deref for OptWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.lock.inner.get().as_ref().unwrap() }
    }
}

impl<T> DerefMut for OptWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.lock.inner.get().as_mut().unwrap() }
    }
}

impl<T> Drop for OptWriteGuard<'_, T> {
    // When the write_guard is droped, the lock will be released,
    // and the version will be increased.
    fn drop(&mut self) {
        self.lock.version.fetch_add(0b10, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use crate::utils::opt_lock::OptLock;

    #[test]
    fn multi_threads() {
        const ONE_LOOP: usize = 100000;
        const THREADS: usize = 10;
        const RESULT: usize = ONE_LOOP * THREADS;

        let raw_lock = Arc::new(OptLock::from(0));

        let threads = (0..THREADS)
            .map(|_| {
                let lock = raw_lock.clone();

                thread::spawn(move || {
                    for _ in 0..ONE_LOOP {
                        'retry: loop {
                            match lock.write() {
                                Ok(mut write_guard) => {
                                    *write_guard += 1;
                                    break 'retry;
                                }
                                Err(_) => continue 'retry,
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for th in threads.into_iter() {
            th.join().unwrap();
        }

        let read_guard = raw_lock.read().unwrap();
        assert_eq!(*read_guard, RESULT);
        read_guard.check_version().unwrap();
    }

    #[test]
    fn wait_release_write_lock() {
        let lock = OptLock::from(0);
        let w = lock.write().unwrap();
        let _r = lock.read().unwrap_err();
        drop(w);

        let r = lock.read().unwrap();
        assert_eq!(*r, 0);
    }
}
