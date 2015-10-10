use mio::Token;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Cookie(u64);

impl Cookie {
    #[inline(always)]
    pub fn new(token: Token, nonce: u64) -> Cookie {
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
