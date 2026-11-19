//! KV cache placement events: the wire codec shared by the SGLang publisher
//! and its consumers, and the publisher itself.

#[cfg(feature = "publisher")]
pub mod publisher;
#[cfg(feature = "python")]
mod python;
pub mod wire;
