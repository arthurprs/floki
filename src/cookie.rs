use mio::Token;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Cookie(u64);

impl Cookie {
    #[inline(always)]
    pub fn new(token: Token, nonce: u64) -> Cookie {
        // TODO: panic on overflow?
        Cookie((token.0 << 48) as u64 | (nonce & 0xFFFFFFFFFFFF))
    }

    #[inline(always)]
    pub fn token(self) -> Token {
        Token((self.0 >> 48) as usize)
    }

    #[inline(always)]
    pub fn nonce(self) -> u64 {
        self.0 & 0xFFFFFFFFFFFF
    }
}

mod tests {
    use super::Cookie;
    use mio::Token;

    #[test]
    fn round_trip() {
        let a = Cookie::new(Token(1), 1);
        assert_eq!(a.token(), Token(1));
        assert_eq!(a.nonce(), 1);
        let a = Cookie::new(Token((1 << 16) - 1), (1 << 48) - 1);
        assert_eq!(a.token(), Token((1 << 16) - 1));
        assert_eq!(a.nonce(), (1 << 48) - 1);
    }

    #[test]
    fn overflow() {
        let a = Cookie::new(Token((1 << 16) + 1), (1 << 48) + 1);
        assert_eq!(a.token(), Token(1));
        assert_eq!(a.nonce(), 1);
    }
}
