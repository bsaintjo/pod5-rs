fn decode_data(data: &[u8], code: u8) {
    todo!()
}

fn decode_scalar(out: &[i16], keys: &[u8], data: &[u8], count: u32, prev: i16) {
    if count == 0 {
        todo!()
    }
    let mut shift = 0u8;
    let mut key_byte = todo!();
    let u_prev = prev as u16;
    for c in 0..count {
        shift += 1;
        if shift == 8 {
            shift = 0;
            key_byte = todo!();
        }
        todo!()
    }
    todo!()
}
