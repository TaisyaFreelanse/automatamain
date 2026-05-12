#[derive(Debug)]
pub enum Error {
    InstructionDataIsTooSmall,
    InvalidDiscriminator,
    InvalidInstructionSize,
    InvalidInstructionData,
}
