//! This implements the compression algorithm used in POD5 format.
//!
//! POD5 uses a variant of the streamvbyte algorithm. Since signal values are
//! only 16-bit (i16) values, it only needs to consider if values fit into 1
//! data byte or 2 data bytes. This means that it only needs to use 1-bit to
//! encode the size, so every control byte encodes up to 8 values, instead of
//! 4..

use std::io;

use bitvec::{prelude::Lsb0, slice::Iter, view::BitView};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use itertools::Itertools;
use zigzag::ZigZag;

// TODO could remove idx, and just mutate the data field in place
struct DecodeIter<'a> {
    count: usize,
    samples: usize,
    bits: Iter<'a, u8, Lsb0>,
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

impl Iterator for DecodeIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.samples {
            return None;
        }
        let code = self.bits.next()?;
        let value = if *code {
            // Bit is set to 1, so two-bytes need to be parsed
            let tmp = u16::from_le_bytes(self.data[self.idx..self.idx + 2].try_into().unwrap());
            self.idx += 2;
            tmp
        } else {
            // Bit is set to 0, so only one byte is needed
            let tmp = self.data[self.idx] as u16;
            self.idx += 1;
            tmp
        };
        self.count += 1;
        Some(value)
    }
}

/// zstd -> streamvbyte -> zig-zag -> delta
/// Can panic if the compressed array doesn't follow the SVB16 specification.
///
/// When running on compressed signal data from a signal column in a POD5 file,
/// use `decode` on the individual rows. If you try to combine the compressed
/// signal across multiple rows that correspond to a signal read this function
/// will panic.
pub fn decode(compressed: &[u8], count: usize) -> io::Result<Vec<i16>> {
    let compressed = zstd::decode_all(compressed)?;
    Ok(DecodeIter::from_compressed(&compressed, count)
        .map(ZigZag::decode)
        .original()
        .collect())
}

// TODO We can know exactly how many ctrl bytes are needed and max number of
// data bytes needed Use Vec::with_capacaity to avoid multiplie allocations
struct Encoder<I> {
    ctrl_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
    iter: I,
}

impl<I: Iterator<Item = u16>> Encoder<I> {
    fn new(iter: I) -> Self {
        Self {
            ctrl_bytes: Vec::new(),
            data_bytes: Vec::new(),
            iter,
        }
    }

    // Iterate over 16-bit values, splitting the bigger values into two bytes
    // and smaller ones in one byte.
    fn encode(mut self) -> Vec<u8> {
        for chunk in &self.iter.chunks(8) {
            let mut ctrl_byte = 0u8;
            let bits = ctrl_byte.view_bits_mut::<Lsb0>();
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

/// Performs VBZ compression
/// delta -> zig-zag -> streamvbyte -> zstd
///
/// Warning
/// The output nearly, but not exactly matches the implementation in ONT's
/// pod5-file-format. The encoded output here will be slightly different in
/// size. I suspect this is due to pod5-file-format allocating all the buffers
/// ahead of time, which are slightly larger than absolutely needed. This
/// implementation builds the data and control byte buffers iteratively and so
/// contain the minimum number of bytes needed. I believe, but haven't
/// confirmed, the output from this should still be compatible with
/// pod5-file-format's decompressor, since the extra padding bytes won't change
/// the resulting decoded values. Future work, may attempt to achieve this
/// 1-to-1 compatibility.
pub fn encode(uncompressed: &[i16]) -> io::Result<Vec<u8>> {
    let iter = uncompressed.iter().copied().deltas().map(ZigZag::encode);
    let svb = Encoder::new(iter).encode();
    // svb.extend_from_slice(&[0; 27]);
    // let max_encoded = max_encoded_length(uncompressed.len());
    // let max_zstd_compressed = zstd::zstd_safe::compress_bound(max_encoded);
    // let mut dst = vec![0; max_zstd_compressed];
    // zstd::encode_all(Cursor::new(svb), 1)
    zstd::bulk::compress(&svb, 1)
    // let compressed_size = zstd::zstd_safe::compress(&mut dst, &svb,
    // 1).unwrap(); dst.resize(compressed_size, 0);
    // Ok(dst)
}

fn split_data(compressed: &[u8], count: usize) -> (&[u8], &[u8]) {
    let mid = num_ctrl_bytes(count);
    compressed.split_at(mid)
}

/// Get number of control bytes used in this variant of streamvbyte
///
/// Essential ceil(count / 8) but we copy the bit operator version from
/// nanopore/pod5-file-format
fn num_ctrl_bytes(count: usize) -> usize {
    // (count as f64 / 8.).ceil() as usize
    (count >> 3) + (((count & 7) + 7) >> 3)
}

fn max_encoded_length(count: usize) -> usize {
    num_ctrl_bytes(count) + (2 * count)
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{Cursor, Read},
    };

    use proptest::{arbitrary::any, prelude::proptest, prop_assert_eq};

    use super::*;

    #[test]
    fn test_num_ctrl_bytes() {
        assert_eq!(num_ctrl_bytes(5), 1);
        assert_eq!(num_ctrl_bytes(8), 1);
        assert_eq!(num_ctrl_bytes(9), 2);
        assert_eq!(num_ctrl_bytes(17), 3);
    }

    #[test]
    fn test_decoder() {
        let samples = 5;
        let answer = [10u16, 1234, 20, 2345, 30];

        // answer in u8 format
        let xs = [0b10101010u8, 10, 0xd2, 0x04, 20, 0x29, 0x09, 30];
        let (ctrl, data) = split_data(&xs, samples);
        let decoded = DecodeIter::new(ctrl, data, samples).collect::<Vec<_>>();
        assert_eq!(decoded, answer);
    }

    #[test]
    fn test_roundtrip() {
        let nums = [10i16, 1234, 20, 2345, 30];
        assert_eq!(decode(&encode(&nums).unwrap(), nums.len()).unwrap(), nums);
    }

    proptest! {
        #[test]
        fn proptest_round_trip(ref vec in any::<Vec<i16>>()) {
            let len = vec.len();
            let vec2 = decode(&encode(vec).unwrap(), len).unwrap();
            prop_assert_eq!(vec, &vec2);
        }
    }

    #[test]
    fn test_zstd() {
        let x = max_encoded_length(102400);
        println!("max encoded: {x}");
        println!("{:?}", zstd::zstd_safe::compress_bound(x));
    }

    #[test]
    fn test_components() {
        let mut compressed_file = File::open("extra/102400-compressed.signal").unwrap();
        let mut compressed = Vec::new();
        compressed_file.read_to_end(&mut compressed).unwrap();

        let mut decompressed_file = File::open("extra/102400-decompressed.signal").unwrap();
        let mut decompressed = String::new();
        decompressed_file.read_to_string(&mut decompressed).unwrap();
        let decompressed = decompressed
            .split(',')
            .map(|x| x.parse::<i16>().unwrap())
            .collect::<Vec<_>>();

        // Delta stage
        let delta_encoded = decompressed.iter().cloned().deltas().collect::<Vec<_>>();

        let d = zstd::decode_all(Cursor::new(&compressed)).unwrap();
        let d: Vec<i16> = DecodeIter::from_compressed(&d, 102400)
            .map(ZigZag::decode)
            .collect::<Vec<_>>();
        assert_eq!(delta_encoded, d);

        // Zig-zag stage
        let z_enc = decompressed
            .iter()
            .cloned()
            .deltas()
            .map(ZigZag::encode)
            .collect::<Vec<_>>();
        let z = zstd::decode_all(Cursor::new(&compressed)).unwrap();
        let z: Vec<u16> = DecodeIter::from_compressed(&z, 102400).collect::<Vec<_>>();
        assert_eq!(z_enc, z);
    }

    #[test]
    fn test_compatibility() {
        let mut compressed_file = File::open("extra/102400-compressed.signal").unwrap();
        let mut compressed = Vec::new();
        compressed_file.read_to_end(&mut compressed).unwrap();

        let mut decompressed_file = File::open("extra/102400-decompressed.signal").unwrap();
        let mut decompressed = String::new();
        decompressed_file.read_to_string(&mut decompressed).unwrap();
        let decompressed = decompressed
            .split(',')
            .map(|x| x.parse::<i16>().unwrap())
            .collect::<Vec<_>>();

        let d = decode(&compressed, 102400).unwrap();
        // let e = encode(&decompressed).unwrap();
        assert_eq!(decompressed, d);
        // assert_eq!(&e[..10], &compressed[..10]);
        // assert_eq!(e.len(), compressed.len());
        // assert_eq!(&e[..10], &compressed[..10]);
    }
}
