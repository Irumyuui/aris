use std::fmt::{Debug, Display};

use bytes::Bytes;

const MAX_VAR_UINT_SIZE: usize = 10;

#[derive(Debug, thiserror::Error)]
pub(crate) enum VarUIntError {
    #[error("Invalid Slice: {0}")]
    InvalidSlice(String),

    #[error("{0}")]
    VarUIntTooLong(String),
}

#[derive(Clone)]
pub(crate) struct VarUInt {
    bytes: Bytes,
}

impl VarUInt {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn try_to_u64(&self) -> Result<u64, VarUIntError> {
        let mut result: u64 = 0;
        let mut shift = 0;
        for byte in &self.bytes {
            let byte = ((byte & 0b0111_1111) as u64).checked_shl(shift).ok_or(
                VarUIntError::VarUIntTooLong("VarUInt too long for u64".into()),
            )?;
            result = result
                .checked_add(byte)
                .ok_or(VarUIntError::VarUIntTooLong(
                    "VarUInt too long for u64".into(),
                ))?;

            shift += 7;
        }
        Ok(result)
    }

    pub(crate) fn try_to_u32(&self) -> Result<u32, VarUIntError> {
        let mut result: u32 = 0;
        let mut shift = 0;
        for byte in &self.bytes {
            let byte = ((byte & 0b0111_1111) as u32).checked_shl(shift).ok_or(
                VarUIntError::VarUIntTooLong("VarUInt too long for u64".into()),
            )?;
            result = result
                .checked_add(byte)
                .ok_or(VarUIntError::VarUIntTooLong(
                    "VarUInt too long for u32".into(),
                ))?;

            shift += 7;
        }
        Ok(result)
    }
}

impl From<u64> for VarUInt {
    fn from(mut value: u64) -> Self {
        if value < (1 << 7) {
            return VarUInt {
                bytes: Bytes::copy_from_slice(&[value as u8]),
            };
        }

        let mut bytes = [0_u8; MAX_VAR_UINT_SIZE];
        let mut i = 0;
        while value > 0 {
            bytes[i] = (value & (0b0111_1111)) as u8;
            value >>= 7;
            i += 1;
        }

        for j in 0..i - 1 {
            bytes[j] |= 0b1000_0000;
        }

        Self {
            bytes: Bytes::copy_from_slice(&bytes[..i]),
        }
    }
}

impl From<u32> for VarUInt {
    fn from(value: u32) -> Self {
        (value as u64).into()
    }
}

impl TryFrom<&[u8]> for VarUInt {
    type Error = VarUIntError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let zero_pos = value
            .iter()
            .position(|b| (b & 0b1000_0000) == 0)
            .ok_or(VarUIntError::InvalidSlice("Zero pos not found".into()))?;

        if zero_pos > MAX_VAR_UINT_SIZE {
            return Err(VarUIntError::InvalidSlice(format!(
                "Slice too long, zero_pos: {zero_pos}"
            )));
        }

        let slice = &value[..=zero_pos];
        Ok(VarUInt {
            bytes: Bytes::copy_from_slice(slice),
        })
    }
}

impl Display for VarUInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.try_to_u64().unwrap();
        write!(f, "{}", value)
    }
}

impl Debug for VarUInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VarUInt")
            .field("bytes", &self.bytes)
            .field("value", &self.try_to_u64().unwrap())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::VarUInt;

    #[test]
    fn trans_and_read_u32() {
        let cases = vec![
            (0b0_1111111_u32, Bytes::copy_from_slice(&[0b0111_1111_u8])),
            (0b1_1111111_u32, Bytes::copy_from_slice(&[0xFF, 0x01])),
            (
                0b0010100_0101010_u32,
                Bytes::copy_from_slice(&[0b10101010, 0b00010100]),
            ),
            (
                0b0010000_1010101_0101010_u32,
                Bytes::copy_from_slice(&[0b10101010, 0b11010101, 0b00010000]),
            ),
            (
                0b11111111_11111111_11111111_11111111_u32,
                Bytes::copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            ),
        ];

        for (value, expected) in cases {
            let varint = super::VarUInt::from(value);
            assert_eq!(varint.as_slice(), expected.as_ref());
            assert_eq!(varint.try_to_u32().unwrap(), value);
        }
    }

    #[test]
    #[should_panic]
    fn read_u32_error() {
        let cases = 0xFFFFFFFFFF_u64;
        let varuint = VarUInt::from(cases);
        varuint.try_to_u32().unwrap();
    }

    #[test]
    fn trans_and_read_u64() {
        let cases = [
            (0b0_1111111_u64, Bytes::copy_from_slice(&[0b0111_1111_u8])),
            (0b1_1111111_u64, Bytes::copy_from_slice(&[0xFF, 0x01])),
            (
                0b0010100_0101010_u64,
                Bytes::copy_from_slice(&[0b10101010, 0b00010100]),
            ),
            (
                0b0010000_1010101_0101010_u64,
                Bytes::copy_from_slice(&[0b10101010, 0b11010101, 0b00010000]),
            ),
            (
                0b11111111_11111111_11111111_11111111_u64,
                Bytes::copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            ),
            (
                u64::MAX,
                Bytes::copy_from_slice(&[
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
                ]),
            ),
        ];

        for (value, expected) in cases {
            let varint = VarUInt::from(value);
            assert_eq!(varint.as_slice(), expected.as_ref());
            assert_eq!(varint.try_to_u64().unwrap(), value);
        }
    }

    #[test]
    fn try_from_slice() {
        let slice = &[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0xFF,
        ][..];
        let excepted = u64::MAX;

        let varint = VarUInt::try_from(slice).unwrap();
        assert_eq!(varint.try_to_u64().unwrap(), excepted);
    }
}
