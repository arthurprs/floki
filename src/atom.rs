use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Atom(Arc<String>);

impl Borrow<str> for Atom {
    #[inline(always)]
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Deref for Atom {
    type Target = str;
    #[inline(always)]
    fn deref(&self) -> &str {
        &self.0
    }
}

impl<T: AsRef<str>> From<T> for Atom {
    #[inline(always)]
	fn from(from: T) -> Atom {
		Atom(Arc::new(from.as_ref().into()))
	}
}
