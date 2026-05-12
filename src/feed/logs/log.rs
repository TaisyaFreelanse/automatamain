use solana_rpc_client_types::config::RpcTransactionLogsFilter;

pub trait HasLogsFilter {
    const PROGRAM: &'static str;
    fn logs_filter() -> RpcTransactionLogsFilter;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Borsh error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Base64 Decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("Base64 Decode Slice: {0}")]
    Base64DecodeSlice(#[from] base64::DecodeSliceError),

    #[error("Base64 Encode Slice: {0}")]
    Base64EncodeSlice(#[from] base64::EncodeSliceError),

    #[error("Invalid discriminator")]
    InvalidDiscriminator,

    #[error("Invalid log event")]
    InvalidLogEvent,
}
