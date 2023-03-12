use std::path::Path;


struct Reader;

impl Reader {
    fn from_path<P>(path: P) -> Self where P: AsRef<Path> {
        todo!()
    }

    fn reads(&self) -> ReadIter {
        todo!()
    }
}

struct ReadIter;

impl Iterator for ReadIter {
    type Item=Pod5Read;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
    
}

struct Pod5Read;

impl Pod5Read {
    fn read_id(&self) -> &str {
        todo!()
    }

    fn sample_count(&self) -> usize {
        todo!()
    }

    fn run_info(&self) -> Option<RunInfo> {
        todo!()
    }
}

struct RunInfo;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reader() {
        let reader = Reader::from_path("extra/multi_fast5_zip_v0.pod5");
        for read in reader.reads() {
            todo!()
        }
    }
}