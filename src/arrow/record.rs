#[derive(Debug)]
pub(crate) struct SignalData {
    pub(crate) raw_signal: Vec<i16>,
}

impl SignalData {
    pub(crate) fn from_raw_signal<S: Into<Vec<i16>>>(raw_signal: S) -> Self {
        Self {
            raw_signal: raw_signal.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadId {
    pub(crate) bytes: Vec<u8>,
}

impl ReadId {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }

    pub(crate) fn from_uuid(uuid: &str) -> Self {
        Self::from_slice(&uuid::Uuid::parse_str(uuid).unwrap().into_bytes())
    }

    pub(crate) fn raw(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn uuid(&self) -> String {
        uuid::Uuid::from_slice(&self.bytes).unwrap().to_string()
    }
}

#[derive(Debug)]
pub(crate) struct Record {
    pub(crate) read_id: ReadId,
    pub(crate) samples: u32,
    pub(crate) signal: SignalData,
}

impl Record {
    pub(crate) fn new(read_id: ReadId, samples: u32, signal: SignalData) -> Self {
        Self {
            read_id,
            samples,
            signal,
        }
    }
}
