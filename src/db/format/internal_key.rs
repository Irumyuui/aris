use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::db::format::unpack_value_type_and_seq;

use super::{pack_value_type_and_seq, SeqNumber, ValueType};

pub const INTERNAL_KEY_TAIL_SIZE: usize = 8;

pub struct ParsedInternalKey {
    user_key: Bytes,
    seq: SeqNumber,
    value_type: ValueType,
}

impl ParsedInternalKey {
    pub fn new(user_key: Bytes, seq: SeqNumber, value_type: ValueType) -> Self {
        Self {
            user_key,
            seq,
            value_type,
        }
    }

    pub fn encord(&self) -> InternalKey {
        InternalKey::new(self.user_key.as_ref(), self.seq, self.value_type)
    }
}

/// The internal key is used to store the key and value type.
///
/// The format like this:
///
/// | user key | seq, vtype |
#[derive(Clone)]
pub struct InternalKey {
    bytes: Bytes,
}

impl InternalKey {
    pub fn extern_buf(buf: &mut impl BufMut, key: &[u8], seq: SeqNumber, value_type: ValueType) {
        buf.put(key);
        buf.put_u64(pack_value_type_and_seq(seq, value_type));
    }

    pub fn new(key: &[u8], seq: SeqNumber, value_type: ValueType) -> Self {
        let mut buf = BytesMut::with_capacity(key.len() + INTERNAL_KEY_TAIL_SIZE);
        Self::extern_buf(&mut buf, key, seq, value_type);

        Self {
            bytes: buf.freeze(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub fn to_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn user_key(&self) -> &[u8] {
        &self.bytes[..self.bytes.len() - INTERNAL_KEY_TAIL_SIZE]
    }

    pub fn parse(&self) -> ParsedInternalKey {
        let user_key = self
            .bytes
            .slice(..self.bytes.len() - INTERNAL_KEY_TAIL_SIZE);

        let (seq, ty) = unpack_value_type_and_seq(
            (&self.bytes[self.bytes.len() - INTERNAL_KEY_TAIL_SIZE..]).get_u64(),
        );

        ParsedInternalKey::new(user_key, seq, ty)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use crate::db::format::{internal_key::InternalKey, ValueType};

    #[test]
    fn internal_key_format() {
        let key = b"hello";
        let seq = 100;
        let value_type = ValueType::Value;

        let internal_key = InternalKey::new(key, seq, value_type);

        let bytes = internal_key.to_bytes();
        assert!(bytes.len() == key.len() + 8);
        assert!(&bytes[..key.len()] == key);
        assert_eq!((&bytes[key.len()..]).get_u64(), (100 << 8) | 1);
    }

    #[test]
    fn parse_internal_key() {
        let key = b"hello";
        let seq = 100;
        let value_type = ValueType::Value;

        let internal_key = InternalKey::new(key, seq, value_type);

        let parsed = internal_key.parse();
        assert_eq!(&parsed.user_key, &key[..]);
        assert_eq!(parsed.seq, seq);
        assert_eq!(parsed.value_type as u8, value_type as u8);
    }

    #[test]
    fn extend_buf() {
        let key = b"hello";
        let seq = 100;
        let value_type = ValueType::Value;

        let mut buf = Vec::new();
        InternalKey::extern_buf(&mut buf, key, seq, value_type);

        assert_eq!(buf.len(), key.len() + 8);
        assert_eq!(&buf[..key.len()], key);
        assert_eq!((&buf[key.len()..]).get_u64(), (100 << 8) | 1);
    }
}
