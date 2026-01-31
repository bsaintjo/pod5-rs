struct Reader;

#[cfg(test)]
mod test {
    use tokio::fs::File;

    use super::*;

    #[tokio::test]
    async fn test_pod5_tokio() {
        let path = "../extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path).await.unwrap();
        
    }
}