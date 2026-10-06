//! Docker service command types.
//!
//! Commands are sent from the TUI plugin to the background service task
//! via an unbounded mpsc channel.

use tokio::sync::oneshot;

use crate::config::DockerConfig;

/// Top-level command enum for the Docker service.
#[derive(Debug)]
pub enum DockerCommand {
    // --- Connection lifecycle ---
    /// Create Docker client and verify connection.
    Connect {
        config: DockerConfig,
        reply: oneshot::Sender<Result<String, String>>,
    },

    /// Disconnect and shut down the background task.
    Disconnect,

    // --- List operations ---
    /// List containers (all=true includes stopped).
    ListContainers { all: bool },

    /// List images.
    ListImages,

    /// List networks.
    ListNetworks,

    /// List volumes.
    ListVolumes,

    // --- Container operations ---
    /// Perform a container lifecycle action (start, stop, restart, remove).
    ContainerAction { id: String, action: String },

    /// Load container inspect data.
    InspectContainer { id: String },

    // --- Streaming operations ---
    /// Start streaming container logs. Cancels any existing log stream.
    StartLogs { container_id: String, tail: usize },

    /// Stop the current log stream.
    StopLogs,

    /// Start streaming container stats. Cancels any existing stats stream.
    StartStats { container_id: String },

    /// Stop the current stats stream.
    StopStats,

    /// Start an exec session in a container.
    StartExec {
        container_id: String,
        cols: u16,
        rows: u16,
    },

    /// Send input data to the active exec session.
    ExecInput { data: Vec<u8> },

    /// Resize the active exec session terminal.
    ResizeExec { cols: u16, rows: u16 },

    /// Stop the active exec session.
    StopExec,

    // --- Image operations ---
    /// Pull an image.
    PullImage { image: String },

    /// Remove an image.
    RemoveImage { id: String },
}
