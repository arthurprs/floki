use std::cmp::{PartialOrd, PartialEq, Ordering};
use std::fmt;

#[derive(Eq, Ord)]
pub struct Rev<T: Ord>(pub T);

impl<T: Ord> PartialEq<Rev<T>> for Rev<T> {
    #[inline(always)]
	fn eq(&self, other: &Rev<T>) -> bool {
		other.0.eq(&self.0)
	}
}

impl<T: Ord> PartialOrd<Rev<T>> for Rev<T> {
    #[inline(always)]
	fn partial_cmp(&self, other: &Rev<T>) -> Option<Ordering> {
		other.0.partial_cmp(&self.0)
	}
}

impl<T: Ord + fmt::Debug> fmt::Debug for Rev<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		self.0.fmt(f)
	}
}

impl<T: Ord + fmt::Display> fmt::Display for Rev<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		self.0.fmt(f)
	}
}
