//! KV cache placement events: the wire codec shared by the SGLang publisher
//! and its consumers, and the publisher itself.

#[cfg(feature = "relay")]
pub mod history;
#[cfg(feature = "publisher")]
pub mod publisher;
#[cfg(feature = "python")]
mod python;
#[cfg(feature = "relay")]
pub mod subscriber;
pub mod wire;
