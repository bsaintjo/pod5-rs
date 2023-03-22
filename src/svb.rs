use delta_encoding::DeltaDecoderExt;
use stream_vbyte::{
    decode::{cursor::DecodeCursor, DecodeSingleSink},
    decode_quad_scalar,
    scalar::Scalar,
};
use zigzag::ZigZag;

fn zigzag_decode(val: u16) -> u16 {
    (val >> 1) ^ (0_u16.wrapping_sub(val & 1))
}

fn key_length(count: u32) -> u32 {
    // (count >> 3) + (((count & 7) + 7) >> 3)
    // (count >> 3) + (((count & 7) + 7) >> 3)
    // ((count as f32) / 8.).ceil() as u32
    (count + 3) / 4
}

fn decode_data(data_ptr_ptr: &mut &[u8], code: u8) -> u16 {
    let mut data_ptr = *data_ptr_ptr;
    let value = if code == 0 {
        let v = data_ptr[0] as u16;
        data_ptr = &data_ptr[1..];
        v
    } else {
        let v = u16::from_le_bytes(data_ptr[..2].try_into().unwrap());
        data_ptr = &data_ptr[2..];
        v
    };
    *data_ptr_ptr = data_ptr;
    value
}

fn decode_scalar(input: &[u8], count: u32) -> eyre::Result<Vec<i16>> {
    let mut out = Vec::new();
    let data_idx = key_length(count);
    // let (keys, mut data) = input.split_at(key_length(count) as usize);
    let keys = input;
    let mut data = &input[data_idx as usize..];
    let mut shift = 0;
    let mut key_byte_idx = 1;
    let mut key_byte = keys[0];
    let _u_prev = 0;
    for _ in 0..count {
        if shift == 8 {
            shift = 0;
            key_byte = keys[key_byte_idx];
            key_byte_idx += 1;
        }
        let code = (key_byte >> shift) & 0x01;
        let decoded_value = {
            let value = decode_data(&mut data, code);
            // value = zigzag_decode(value);
            // value = value.wrapping_add(u_prev);
            // u_prev = value;
            value as i16
        };
        out.push(decoded_value);
        shift += 1;
    }
    Ok(out)
}

struct Sink(Vec<u16>);

impl Sink {
    fn new(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
}

impl DecodeSingleSink for Sink {
    fn on_number(&mut self, num: u32, _nums_decoded: usize) {
        self.0.push(num as u16)
    }
}

decode_quad_scalar!(Sink);

fn decode_svb(data: &[u8], count: u32) -> Vec<u16> {
    let data_idx = key_length(count);
    println!("data_idx: {data_idx}");
    let diff_count = data.len() - data_idx as usize;
    println!("diff count: {diff_count}");
    let _control_bytes = &data[..data_idx as usize];
    let _encoded_nums = &data[data_idx as usize..];
    let mut cursor = DecodeCursor::new(data, count as usize - 1);
    let sink = Sink::new(count as usize);
    let mut output = vec![0; count as usize - 1];
    // let (num_decoded, bytes_read) = scalar::Scalar::decode_quads(control_bytes, encoded_nums, data_idx as usize, 0, &mut sink);
    // let num_decoded = cursor.decode_sink(&mut sink, count as usize - 2);
    let num_decoded = cursor.decode_slice::<Scalar>(&mut output);
    println!("num decoded {num_decoded}");
    println!("count: {count}");
    sink.0
}

fn decode_vbz(_data: &[u8], _count: u32) -> Vec<i16> {
    todo!()
}

pub(crate) fn decode(source: &[u8], count: u32) -> eyre::Result<Vec<i16>> {
    // let Some(size) = zstd_safe::get_frame_content_size(source).unwrap() else { return Err(eyre::eyre!("Frame doesn't contain content size")) };
    // println!("frame content size: {size}");
    // let mut zstd_decoded = vec![0; size as usize];
    let zstd_decoded = zstd::decode_all(source)?;
    println!("zstd len: {}", zstd_decoded.len());
    // zstd_safe::decompress(&mut zstd_decoded, source).unwrap();
    let svb_decoded = decode_svb(&zstd_decoded, count);
    let _delta_zigzag_decoded: Vec<i16> = svb_decoded
        .into_iter()
        .map(ZigZag::decode)
        .original()
        .collect();
    decode_scalar(&zstd_decoded, count)
    // let mut svb_decoded = vec![0; count as usize];
    // let _svb_decoded_count =
    //     stream_vbyte::decode::decode::<Scalar>(&zstd_decoded, zstd_decoded.len(), &mut svb_decoded);
    // let delta_zz_decoded = svb_decoded
    //     .into_iter()
    //     .map(|x| i16::decode(x as u16))
    //     .original()
    //     .collect::<Vec<_>>();
    // Ok(delta_zz_decoded)
}

#[cfg(test)]
mod test {
    use stream_vbyte::{encode::encode, scalar};

    use super::*;

    #[test]
    fn test_svb_vs() {
        let nums = (5i16..15).collect::<Vec<_>>();
        let nums = nums.iter().map(|x| *x as u32).collect::<Vec<_>>();
        let mut encoded_crate = Vec::new();
        encoded_crate.resize(nums.len() * 5, 0x0);
        let encoded_len = encode::<scalar::Scalar>(&nums, &mut encoded_crate);
        println!("encoded_len {encoded_len}");

        let decoded_vbz = decode_scalar(&encoded_crate, nums.len() as u32).unwrap();
        let mut decoded_crate = Vec::new();
        decoded_crate.resize(nums.len(), 0x0);
        stream_vbyte::decode::decode::<scalar::Scalar>(
            &encoded_crate,
            nums.len(),
            &mut decoded_crate,
        );
        let decoded_crate = decoded_crate
            .into_iter()
            .map(|x| x as i16)
            .collect::<Vec<_>>();
        assert_eq!(decoded_crate, decoded_vbz);
    }
}
