//! Shared types for the Docker plugin UI

/// Container information extracted from Docker API
pub struct ContainerInfo {
    /// Short container ID
    pub id: String,
    /// Container name (without leading /)
    pub name: String,
    /// Image name
    pub image: String,
    /// Container state (running, exited, etc.)
    pub state: String,
    /// Human-readable status string
    pub status: String,
    /// Port mappings as display string
    pub ports: String,
    /// Creation timestamp
    pub created: String,
}

/// Image information
pub struct ImageInfo {
    /// Short image ID
    pub id: String,
    /// Repository tags (e.g., ["nginx:latest"])
    pub tags: Vec<String>,
    /// Image size in bytes
    pub size: u64,
    /// Creation timestamp
    pub created: String,
}

/// Network information
pub struct NetworkInfo {
    /// Network ID
    pub id: String,
    /// Network name
    pub name: String,
    /// Driver (bridge, overlay, etc.)
    pub driver: String,
    /// Scope (local, global, swarm)
    pub scope: String,
}

/// Volume information
pub struct VolumeInfo {
    /// Volume name
    pub name: String,
    /// Driver
    pub driver: String,
    /// Mount point on host
    pub mountpoint: String,
}

/// Real-time container resource stats
pub struct ContainerStats {
    pub cpu_percent: f64,
    pub mem_usage: u64,
    pub mem_limit: u64,
    pub mem_percent: f64,
    pub net_rx: u64,
    pub net_tx: u64,
}

/// Events sent from background Docker tasks to the UI
pub enum DockerEvent {
    /// Container inspect data loaded
    InspectLoaded(Box<bollard::models::ContainerInspectResponse>),
    /// Container list loaded
    ContainersLoaded(Vec<ContainerInfo>),
    /// Image list loaded
    ImagesLoaded(Vec<ImageInfo>),
    /// Network list loaded
    NetworksLoaded(Vec<NetworkInfo>),
    /// Volume list loaded
    VolumesLoaded(Vec<VolumeInfo>),
    /// Container stats update
    StatsUpdate(ContainerStats),
    /// Log line received
    LogLine { is_stderr: bool, text: String },
    /// Image pull progress update
    PullProgress { status: String, progress: String },
    /// Image pull completed
    PullComplete,
    /// Operation completed with message
    OperationComplete(String),
    /// An error occurred
    Error(String),
}
