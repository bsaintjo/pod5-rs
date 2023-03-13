use std::io::Read;

use delta_encoding::DeltaDecoderExt;
use stream_vbyte::scalar::Scalar;
use zigzag::ZigZag;
use zstd::decode_all;

fn decode<R>(source: R) -> eyre::Result<Vec<i16>>
where
    R: Read,
{
    let zstd_decoded = decode_all(source)?;
    let mut svb_decoded = Vec::new();
    let _svb_decoded_count =
        stream_vbyte::decode::decode::<Scalar>(&zstd_decoded, zstd_decoded.len(), &mut svb_decoded);
    let delta_zz_decoded = svb_decoded
        .into_iter()
        .map(|x| i16::decode(x as u16))
        .original()
        .collect::<Vec<_>>();
    Ok(delta_zz_decoded)
}
