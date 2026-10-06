//! Standalone entrypoint for voidb-plugin-docker.
//!
//! Provides stdio-jsonrpc server for VoidB external process plugin integration.

#![allow(clippy::result_large_err)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use voidb_core::{CapabilityError, CapabilityErrorCategory, RedactionStatus};
use voidb_plugin_docker::{DockerConfig, invoke_docker_capability};
use voidb_process_plugin_sdk::{CapabilityRouter, serve_stdio};

#[derive(Parser, Debug)]
#[command(name = "voidb-plugin-docker")]
#[command(about = "Docker management process plugin for VoidB", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run as a stdio-jsonrpc server (default when launched by VoidB)
    Serve,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Serve) | None => run_server(),
    }
}

fn run_server() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let mut router = CapabilityRouter::new("docker");

    for cap in voidb_plugin_docker::docker_capabilities() {
        let rt = runtime.handle().clone();

        router = router.capability(cap.id, move |invocation, _grants| {
            let config: DockerConfig = match &invocation.connection {
                voidb_core::InvocationConnectionTarget::FromProfile { options, .. } => {
                    serde_json::from_value(options.clone()).unwrap_or_default()
                }
                voidb_core::InvocationConnectionTarget::Stateless => DockerConfig::default(),
                _ => {
                    return Err(CapabilityError {
                        category: CapabilityErrorCategory::Plugin,
                        code: "docker.unsupported_connection_target".into(),
                        message: "Connection target must be FromProfile or Stateless.".into(),
                        details: serde_json::json!({}),
                        target: None,
                        retryable: false,
                        redaction: RedactionStatus::NotRequired,
                    });
                }
            };

            rt.block_on(async {
                invoke_docker_capability(&config, invocation).await
            })
        });
    }

    serve_stdio(router)?;
    Ok(())
}
