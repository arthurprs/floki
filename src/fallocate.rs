use std::fs::File;
use std::io;

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "dragonfly"))]
pub fn fallocate(f: &File, offset: u64, len: u64) -> io::Result<()> {
	unsafe {
		use std::os::unix::io::AsRawFd;
		use libc::{posix_fallocate, off_t};

		match posix_fallocate(f.as_raw_fd(), offset as off_t, len as off_t) {
			0 => Ok(()),
			err_code => Err(io::Error::from_raw_os_error(err_code))
		}
	}
}

#[cfg(target_os = "macos")]
pub fn fallocate(f: &File, offset: u64, len: u64) -> io::Result<()> {
	unsafe {
		use std::os::unix::io::AsRawFd;
		use libc::{ftruncate, fcntl, fstore_t, off_t,
			F_ALLOCATECONTIG, F_PEOFPOSMODE, F_PREALLOCATE, F_ALLOCATEALL};

		let fd = f.as_raw_fd();
	    let mut store = fstore_t{
	    	fst_flags: F_ALLOCATECONTIG,
	    	fst_posmode: F_PEOFPOSMODE,
	    	fst_offset: offset as off_t,
	    	fst_length: len as off_t,
	    	fst_bytesalloc: 0
    	};
	    if fcntl(fd, F_PREALLOCATE, &store) == -1 {
	        // try and allocate space with fragments
	        store.fst_flags = F_ALLOCATEALL;
	        if fcntl(fd, F_PREALLOCATE, &store) == -1 {
		        return Err(io::Error::last_os_error())
		    }
	    }
	    match ftruncate(fd, (offset + len) as off_t) {
			0 => Ok(()),
			err_code => Err(io::Error::last_os_error())
		}
	}
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "dragonfly", target_os = "macos")))]
pub fn fallocate(f: &File, offset: u64, len: u64) -> io::Result<()> {
	f.set_len(offset + len)
}
