//! Docker plugin configuration structures

use serde::{Deserialize, Serialize};

/// Docker connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerConfig {
    /// How to connect to Docker
    pub connection: DockerConnection,
    /// Timeout for Docker API operations in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

/// Docker connection method
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DockerConnection {
    /// Local Unix socket (/var/run/docker.sock)
    Local,
    /// Custom Unix socket path
    Socket { path: String },
    /// TCP connection (no TLS)
    Http { url: String },
    /// TLS-secured TCP connection
    Tls {
        url: String,
        ca_cert: String,
        cert: String,
        key: String,
    },
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            connection: DockerConnection::Local,
            timeout: default_timeout(),
        }
    }
}

fn default_timeout() -> u64 {
    30
}
