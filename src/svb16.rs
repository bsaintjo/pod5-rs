//! This implements the compression algorithm used in POD5 format
//!
//! POD5 uses a variant of the streamvbyte algorithm. Since signal values are only 16-bit (i16) values,
//! it only needs to consider if values fit into 1 data byte or 2 data bytes. This means that it only needs
//! to use 1-bit to encode the size, so every control byte encodes up to 8 values, instead of 4..
//!
//! In POD5, data is decoded by data -> zstd decoding ->

use bitvec::{
    prelude::Msb0,
    slice::{Iter},
    view::BitView,
};
use delta_encoding::DeltaDecoderExt;
use delta_encoding::DeltaEncoderExt;
use itertools::Itertools;
use zigzag::ZigZag;

struct DecodeIter<'a> {
    count: usize,
    samples: usize,
    bits: Iter<'a, u8, Msb0>,
    idx: usize,
    data: &'a [u8],
}

impl<'a> DecodeIter<'a> {
    fn new(ctrl_bytes: &'a [u8], data: &'a [u8], samples: usize) -> Self {
        Self {
            bits: ctrl_bytes.view_bits().iter(),
            idx: 0,
            data,
            count: 0,
            samples,
        }
    }

    fn from_compressed(data: &'a [u8], samples: usize) -> Self {
        let (ctrl, data) = split_data(data, samples);
        DecodeIter::new(ctrl, data, samples)
    }
}

impl<'a> Iterator for DecodeIter<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.samples {
            return None;
        }
        let code = self.bits.next()?;
        let value = if *code {
            let tmp = u16::from_le_bytes(self.data[self.idx..self.idx + 2].try_into().unwrap());
            self.idx += 2;
            tmp
        } else {
            let tmp = self.data[self.idx] as u16;
            self.idx += 1;
            tmp
        };
        self.count += 1;
        Some(value)
    }
}

pub fn decode(compressed: &[u8], count: usize) -> Vec<i16> {
    DecodeIter::from_compressed(compressed, count)
        .map(ZigZag::decode)
        .original()
        .collect()
}

struct Encoder<I> {
    ctrl_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
    iter: I,
}

impl<I: Iterator<Item=u16>> Encoder<I> {
    fn new(iter: I) -> Self {
        Self {
            ctrl_bytes: Vec::new(),
            data_bytes: Vec::new(),
            iter
        }
    }

    fn encode(mut self) -> Vec<u8> {
        for chunk in &self.iter.chunks(8) {
            let mut ctrl_byte = 0u8;
            let bits = ctrl_byte.view_bits_mut::<Msb0>();
            for (x, mut code) in chunk.zip(bits.iter_mut()) {
                if x > (u8::MAX as u16) {
                    *code = true;
                    self.data_bytes.extend_from_slice(&x.to_le_bytes());
                } else {
                    self.data_bytes.push(x as u8);
                }
            }
            self.ctrl_bytes.push(ctrl_byte)
        }
        let mut compressed = self.ctrl_bytes;
        compressed.append(&mut self.data_bytes);
        compressed
    }
}

pub fn encode(uncompressed: &[i16]) -> Vec<u8> {
    let iter = uncompressed.iter().copied().deltas().map(ZigZag::encode);
    Encoder::new(iter).encode()
}

fn split_data(compressed: &[u8], count: usize) -> (&[u8], &[u8]) {
    let mid = num_ctrl_bytes(count);
    compressed.split_at(mid)
}

fn num_ctrl_bytes(count: usize) -> usize {
    // (count as f64 / 8.).ceil() as usize
    (count >> 3) + (((count & 7) + 7) >> 3)
}

#[cfg(test)]
mod test {
    

    use super::*;

    #[test]
    fn test_num_ctrl_bytes() {
        assert_eq!(num_ctrl_bytes(5), 1);
        assert_eq!(num_ctrl_bytes(8), 1);
        assert_eq!(num_ctrl_bytes(9), 2);
        assert_eq!(num_ctrl_bytes(17), 3);
    }

    #[test]
    fn test_bitvec() {
        let samples = 5;
        let answer = [10u16, 1234, 20, 2345, 30];
        let xs = [0b01010101u8, 10, 0xd2, 0x04, 20, 0x29, 0x09, 30];
        let (ctrl, data) = split_data(&xs, samples);
        let decoded = DecodeIter::new(ctrl, data, samples).collect::<Vec<_>>();
        assert_eq!(decoded, answer);
    }

    #[test]
    fn test_roundtrip() {
        let nums = [10i16, 1234, 20, 2345, 30];
        assert_eq!(decode(&encode(&nums), nums.len()), nums);
    }
}
