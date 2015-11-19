use std::sync::{RwLock, Mutex, MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::fmt;
use std::default::Default;

/// A lock with 3 states
/// Allows one writer and many readers to run concurrently,
/// a special exclusive state that blocks both readers and writers is also available. 
pub struct TristateLock<T>
{
    w_lock: Mutex<()>,
    r_lock: RwLock<()>,
    data: UnsafeCell<T>,
}

/// A guard to which the protected data can be read
///
/// When the guard falls out of scope it will decrement the read count,
/// potentially releasing the lock.
pub struct TristateLockReadGuard<'a, T:'a>
{
    r_guard: RwLockReadGuard<'a, ()>,
    data: &'a T,
}

/// A guard to which the protected data can be written
///
/// When the guard falls out of scope it will release the lock.
pub struct TristateLockWriteGuard<'a, T:'a>
{
    w_guard: MutexGuard<'a, ()>,
    data: &'a mut T,
}

/// A guard to which the protected data is being access exclusivelly
///
/// When the guard falls out of scope it will release the lock.
pub struct TristateLockExclusiveGuard<'a, T:'a>
{
    w_guard: MutexGuard<'a, ()>,
    r_guard: RwLockWriteGuard<'a, ()>,
    data: &'a mut T,
}

unsafe impl<T> Sync for TristateLock<T> {}
unsafe impl<T:'static+Send> Send for TristateLock<T> {}

impl<T> TristateLock<T>
{
    #[inline]
    pub fn new(user_data: T) -> TristateLock<T>
    {
        TristateLock
        {
            w_lock: Mutex::new(()),
            r_lock: RwLock::new(()),
            data: UnsafeCell::new(user_data),
        }
    }

    #[inline]
    pub fn read<'a>(&'a self) -> TristateLockReadGuard<'a, T>
    {
        TristateLockReadGuard {
            r_guard: self.r_lock.read().unwrap(),
            data: unsafe { & *self.data.get() },
        }
    }

    #[inline]
    pub fn try_read(&self) -> Option<TristateLockReadGuard<T>>
    {
        match self.r_lock.try_read() {
            Ok(r_guard) => Some(TristateLockReadGuard {
                r_guard: r_guard,
                data: unsafe { & *self.data.get() },
            }),
            _ => None
        }
    }

    #[inline]
    pub fn write<'a>(&'a self) -> TristateLockWriteGuard<'a, T>
    {
        TristateLockWriteGuard {
            w_guard: self.w_lock.lock().unwrap(),
            data: unsafe { &mut *self.data.get() },
        }
    }

    #[inline]
    pub fn try_write(&self) -> Option<TristateLockWriteGuard<T>>
    {
        match self.w_lock.try_lock() {
            Ok(w_guard) => Some(TristateLockWriteGuard {
                w_guard: w_guard,
                data: unsafe { &mut *self.data.get() },
            }),
            _ => None
        }
    }

    #[inline]
    pub fn lock<'a>(&'a self) -> TristateLockExclusiveGuard<'a, T>
    {
        TristateLockExclusiveGuard {
            w_guard: self.w_lock.lock().unwrap(),
            r_guard: self.r_lock.write().unwrap(),
            data: unsafe { &mut *self.data.get() },
        }
    }

    #[inline]
    pub fn try_lock<'a>(&'a self) -> Option<TristateLockExclusiveGuard<'a, T>>
    {
        match (self.w_lock.try_lock(), self.r_lock.try_write()) {
            (Ok(w_guard), Ok(r_guard)) => Some(TristateLockExclusiveGuard {
                w_guard: w_guard,
                r_guard: r_guard,
                data: unsafe { &mut *self.data.get() },
            }),
            _ => None
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for TristateLock<T>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        match self.try_read()
        {
            Some(guard) => write!(f, "TristateLock {{ data: {:?} }}", &*guard),
            None => write!(f, "TristateLock {{ <locked> }}"),
        }
    }
}

impl<T: Default> Default for TristateLock<T> {
    fn default() -> TristateLock<T> {
        TristateLock::new(Default::default())
    }
}

impl<'a, T> Deref for TristateLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T { self.data }
}

impl<'a, T> Deref for TristateLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T { self.data }
}

impl<'a, T> DerefMut for TristateLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T { self.data }
}

impl<'a, T> Deref for TristateLockExclusiveGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T { self.data }
}

impl<'a, T> DerefMut for TristateLockExclusiveGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T { self.data }
}
