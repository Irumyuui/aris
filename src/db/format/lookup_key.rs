use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    db::format::{internal_key::INTERNAL_KEY_TAIL_SIZE, pack_value_type_and_seq},
    utils::varint::VarUInt,
};

use super::{internal_key::InternalKey, SeqNumber, ValueType};

/// A key used to look up a value in a table or an index.
///
/// The format like this:
///
/// | var internal key len | internal key |
#[derive(Clone)]
pub struct LookUpKey {
    bytes: Bytes,
    user_key_offset: usize,
}

impl LookUpKey {
    pub fn extend_buf(
        buf: &mut impl BufMut,
        user_key: &[u8],
        seq: SeqNumber,
        value_type: ValueType,
    ) -> usize {
        let offset = VarUInt::extend_buf(buf, user_key.len() as u64);
        InternalKey::extern_buf(buf, user_key, seq, value_type);
        offset
    }

    pub fn new(user_key: &[u8], seq: SeqNumber, value_type: ValueType) -> Self {
        let mut buf = BytesMut::with_capacity(10 + user_key.len() + INTERNAL_KEY_TAIL_SIZE);
        let offset = Self::extend_buf(&mut buf, user_key, seq, value_type);

        Self {
            bytes: buf.freeze(),
            user_key_offset: offset,
        }
    }

    pub fn internal_key_slice(&self) -> &[u8] {
        &self.bytes[self.user_key_offset..]
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::format::{internal_key::InternalKey, lookup_key::LookUpKey, ValueType},
        utils::varint::VarUInt,
    };

    #[test]
    fn new_lookup_key() {
        let user_key = b"hello";
        let seq = 100;
        let value_type = ValueType::Value;

        let lookup_key = LookUpKey::new(user_key, seq, value_type);
        let internal_key = InternalKey::new(user_key, seq, value_type);

        assert_eq!(lookup_key.internal_key_slice(), internal_key.as_slice());
        let varlen = VarUInt::from(user_key.len() as u64);
        assert_eq!(&lookup_key.bytes[..varlen.len()], varlen.as_slice());
    }
}
