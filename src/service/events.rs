//! Docker service event types.
//!
//! Re-exports the existing `DockerEvent` from `types.rs` which already covers
//! all event variants needed by the service layer. Additional service-specific
//! events (exec output, connected/disconnected) are added here.

// Re-export the existing DockerEvent and all display types
pub use crate::types::{
    ContainerInfo, ContainerStats, DockerEvent, ImageInfo, NetworkInfo, VolumeInfo,
};

/// Events specific to exec session streaming (raw bytes, not DockerEvent).
/// These are sent on a separate channel from the main event stream.
#[derive(Debug)]
pub enum ExecEvent {
    /// Raw output bytes from the exec session.
    Output(Vec<u8>),
    /// Exec session started.
    Started,
    /// Exec session ended.
    Ended,
}
