//! Docker management plugin for VoidB

mod agent_session;
mod capabilities;
mod cli_plugin;
pub mod config;
mod docker_ops;
pub mod service;
mod tui;
mod types;

pub use agent_session::DockerAgentSessionFactory;
pub use capabilities::{docker_capabilities, invoke_docker_capability};
pub use cli_plugin::create_docker_cli_plugin;
pub use config::DockerConfig;
pub use tui::{
    DOCKER_AGENT_CONTEXT_STORE_DIR_ENV, LEGACY_DOCKER_ASSIST_STORE_DIR_ENV,
    docker_agent_context_store_root,
};

/// Test a Docker connection from a ConnectionConfig
pub async fn test_connection(
    conn: &voidb_core::connection::ConnectionConfig,
) -> anyhow::Result<String> {
    let config: DockerConfig = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|v| serde_json::from_value(v.clone()).map_err(Into::into))?;

    let docker = docker_ops::create_client(&config)?;
    docker_ops::ping(&docker).await
}
