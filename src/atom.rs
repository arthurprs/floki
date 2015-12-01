use std::borrow::Borrow;
use std::ops::Deref;
use std::hash;
use tendril;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Atom(tendril::Tendril<tendril::fmt::UTF8, tendril::Atomic>);

impl hash::Hash for Atom {
    #[inline]
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        let atom_str: &str = &self.0;
        atom_str.hash(state)
    }
}

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
		Atom(from.as_ref().into())
	}
}

/*
#[cfg(test)]
mod tests {
    use string_cache::Atom;
    use test;
    use std::sync::Arc;
    use tendril;
    use std::collections::HashMap;

    type TheTendril = tendril::Tendril<tendril::fmt::UTF8, tendril::Atomic>;

    #[bench]
    fn bench_atom_into(b: &mut test::Bencher) {
        let items = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        let atoms: Vec<Atom> = items.iter().map(|s| s.into()).collect();
        b.iter(|| {
            for s in &items {
                let atom: Atom = s.into();
                test::black_box(atom);
            }
        });
        test::black_box(atoms);
    }

    #[bench]
    fn bench_arc_string_into(b: &mut test::Bencher) {
        let items = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        b.iter(|| {
            for s in &items {
                test::black_box(Arc::new(s.clone()));
            }
        })
    }

    #[bench]
    fn bench_string_into(b: &mut test::Bencher) {
        let items = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        b.iter(|| {
            for s in &items {
                test::black_box(s.clone());
            }
        })
    }

    #[bench]
    fn bench_sync_tendril_into(b: &mut test::Bencher) {
        let items = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        b.iter(|| {
            for s in &items {
                let tendril_str = TheTendril::from_slice(s.as_ref());
                test::black_box(s);
            }
        })
    }

    #[bench]
    fn bench_atom_hashmap(b: &mut test::Bencher) {
        let items_v = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        let items_hm: HashMap<Atom, _> = items_v.iter().map(
            |s| (s.into(), s.clone())).collect();
        b.iter(|| {
            for s in &items_v {
                let atom: Atom = s.into();
                test::black_box(items_hm.get(&atom));
            }
        });
    }

    #[bench]
    fn bench_tendril_hashmap(b: &mut test::Bencher) {
        let items_v = vec![
            "my_queue".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        let items_hm: HashMap<TheTendril, _> = items_v.iter().map(
            |s| (TheTendril::from_slice(s.as_ref()), s.clone())).collect();
        b.iter(|| {
            for s in &items_v {
                let atom: TheTendril = TheTendril::from_slice(s.as_ref());
                test::black_box(items_hm.get(&atom));
            }
        });
    }

    #[bench]
    fn bench_string_hashmap(b: &mut test::Bencher) {
        let items_v = vec![
            "my_queue_1".to_owned(),
            "my_own_queue_100000".to_owned(),
            "my_longer_queue_name_100001".to_owned()
        ];
        let items_hm: HashMap<_, _> = items_v.iter().map(
            |s| (s.clone(), s.clone())).collect();
        b.iter(|| {
            for s in &items_v {
                let string: String = s.clone();
                test::black_box(items_hm.get(&string));
                test::black_box(string);
            }
        });
    }
}
*/