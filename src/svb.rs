use delta_encoding::DeltaDecoderExt;
use stream_vbyte::{decode::{DecodeSingleSink, cursor::DecodeCursor, Decoder}, decode_quad_scalar, scalar};
use zigzag::ZigZag;

fn zigzag_decode(val: u16) -> u16 {
    (val >> 1) ^ (0_u16.wrapping_sub(val & 1))
}

fn key_length(count: u32) -> u32 {
    (count >> 3) + (((count & 7) + 7) >> 3)
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
    let mut key_byte = keys[0];
    let mut key_byte_idx = 1;
    let mut u_prev = 0;
    for _ in 0 .. count {
        shift += 1;
        if shift == 8 {
            shift = 0;
            key_byte = keys[key_byte_idx];
            key_byte_idx += 1;
        }
        let code = (key_byte >> shift) & 0x01;
        let decoded_value = {
            let mut value = decode_data(&mut data, code);
            value = zigzag_decode(value);
            value = value.wrapping_add(u_prev);
            u_prev = value;
            value as i16
        };
        out.push(decoded_value);
    }
    Ok(out)
}

struct Sink(Vec<u16>);

impl DecodeSingleSink for Sink {
    fn on_number(&mut self, num: u32, _nums_decoded: usize) {
        self.0.push(num as u16)
    }
}

decode_quad_scalar!(Sink);

fn decode_svb(data: &[u8], count: u32) -> Vec<u16>{
    let data_idx = key_length(count);
    let control_bytes = data;
    let encoded_nums = &data[data_idx as usize..];
    let mut sink = Sink(Vec::new());
    let _ = scalar::Scalar::decode_quads(control_bytes, encoded_nums, data_idx as usize, 0, &mut sink);
    sink.0
}

fn decode_vbz(data: &[u8], count: u32) -> Vec<i16> {
    todo!()
}

pub(crate) fn decode(source: &[u8], count: u32) -> eyre::Result<Vec<i16>>
{
    // let Some(size) = zstd_safe::get_frame_content_size(source).unwrap() else { return Err(eyre::eyre!("Frame doesn't contain content size")) };
    // println!("frame content size: {size}");
    // let mut zstd_decoded = vec![0; size as usize];
    let zstd_decoded = zstd::decode_all(source)?;
    // zstd_safe::decompress(&mut zstd_decoded, source).unwrap();
    let svb_decoded = decode_svb(&zstd_decoded, count);
    let delta_zigzag_decoded: Vec<i16> = svb_decoded.into_iter().map(ZigZag::decode).original().collect();
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
