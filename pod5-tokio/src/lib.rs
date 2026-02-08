struct Reader;

#[cfg(test)]
mod test {
    use pod5_format::ParsedFooter;
    use tokio::{fs::File, io::AsyncReadExt};

    use super::*;

    #[tokio::test]
    async fn test_pod5_tokio() {
        let path = "../extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path).await.unwrap();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();

        let footer = ParsedFooter::read_footer(&buf).unwrap();
        let mut read_table_buf = Vec::new();
        let read_table = footer.read_table().unwrap();
        read_table.read_to_buf(&buf, &mut read_table_buf).unwrap();
        
    }
}