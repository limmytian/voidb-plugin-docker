//! Docker CLI plugin.
//!
//! Docker operations stay behind the plugin service layer and capability surface.

use std::fs;

use async_trait::async_trait;
use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches, Command};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::{ConnectionProfileRef, TuiLaunchRequest, VoidbError, build_tui_launch_plan};

use crate::config::DockerConfig;
use crate::docker_ops;
use crate::tui::{
    DockerTuiLaunch, DockerTuiSource, build_docker_tui_evidence, run_docker_tui,
    write_docker_tui_preflight,
};

pub struct DockerCliPlugin;

pub fn create_docker_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(DockerCliPlugin)
}

#[async_trait]
impl CliPlugin for DockerCliPlugin {
    fn plugin_id(&self) -> &str {
        "docker"
    }

    fn name(&self) -> &str {
        "Docker"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .value_name("CONNECTION")
            .help("Connection name (defaults to local Docker daemon)");

        let profile_arg = Arg::new("profile")
            .long("profile")
            .value_name("PROFILE")
            .conflicts_with("connection")
            .help("Profile name, id:<id>, or name:<name>");

        vec![
            Command::new("test")
                .about("Test Docker daemon connectivity and show version")
                .arg(profile_arg.clone())
                .arg(conn_arg.clone()),
            Command::new("ps")
                .about("List Docker containers")
                .arg(profile_arg.clone())
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("all")
                        .short('a')
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Show all containers (default shows just running)"),
                ),
            Command::new("images")
                .about("List Docker images")
                .arg(profile_arg.clone())
                .arg(conn_arg.clone()),
            Command::new("volumes")
                .about("List Docker volumes")
                .arg(profile_arg.clone())
                .arg(conn_arg.clone()),
            Command::new("networks")
                .about("List Docker networks")
                .arg(profile_arg.clone())
                .arg(conn_arg.clone()),
            Command::new("tui")
                .about("Launch the standalone Docker operations TUI")
                .arg(
                    Arg::new("profile")
                        .long("profile")
                        .value_name("PROFILE")
                        .conflicts_with("connection")
                        .help("Profile name, id:<id>, or name:<name>"),
                )
                .arg(
                    Arg::new("connection")
                        .short('c')
                        .long("connection")
                        .value_name("CONNECTION")
                        .conflicts_with("profile")
                        .help("Legacy connection name"),
                )
                .arg(
                    Arg::new("fixture").long("fixture").value_name("PATH").help(
                        "Load deterministic Docker TUI fixture JSON instead of opening a target",
                    ),
                )
                .arg(
                    Arg::new("purpose")
                        .long("purpose")
                        .value_name("PURPOSE")
                        .default_value("operations")
                        .help("Launch purpose, for example operations or triage"),
                )
                .arg(
                    Arg::new("readonly")
                        .long("readonly")
                        .action(ArgAction::SetTrue)
                        .help("Stage operation plans but block mutating service commands"),
                )
                .arg(
                    Arg::new("no-restore")
                        .long("no-restore")
                        .action(ArgAction::SetTrue)
                        .help("Start without restoring plugin-owned UI state"),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_parser(["json"])
                        .help("Emit secret-free preflight JSON and exit"),
                )
                .arg(
                    Arg::new("evidence")
                        .long("evidence")
                        .value_name("PATH")
                        .help("Write fixture-backed standalone Docker TUI evidence JSON and exit"),
                ),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "test" => self.handle_test(matches, ctx).await,
            "ps" => self.handle_ps(matches, ctx).await,
            "images" => self.handle_images(matches, ctx).await,
            "volumes" => self.handle_volumes(matches, ctx).await,
            "networks" => self.handle_networks(matches, ctx).await,
            "tui" => self.handle_tui(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!(
                "Unknown docker command: {}",
                command
            ))),
        }
    }
}

impl DockerCliPlugin {
    fn resolve_config(matches: &ArgMatches, ctx: &CliContext) -> Result<DockerConfig, VoidbError> {
        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (_profile, connection) =
                ctx.resolve_profile_connection(profile_ref, Some("docker"))?;
            connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone()).map_err(|e| {
                        VoidbError::Connection(format!("Invalid Docker config: {}", e))
                    })
                })
        } else if let Some(conn_name) = matches.get_one::<String>("connection") {
            Self::parse_config(conn_name, ctx)
        } else {
            Ok(DockerConfig::default())
        }
    }

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::resolve_config(matches, ctx)?;
        let docker = docker_ops::create_client(&config)
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        let version = docker_ops::ping(&docker)
            .await
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        println!("Connected: {version}");
        Ok(())
    }

    async fn handle_ps(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::resolve_config(matches, ctx)?;
        let docker = docker_ops::create_client(&config)
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        let all = matches.get_flag("all");
        let containers = docker_ops::list_containers(&docker, all)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;
        println!("{:<14} {:<24} {:<20} {:<12} {:<20}", "CONTAINER ID", "IMAGE", "COMMAND/NAME", "STATE", "STATUS");
        println!("{}", "-".repeat(94));
        for c in containers {
            println!("{:<14} {:<24} {:<20} {:<12} {:<20}", c.id, c.image, c.name, c.state, c.status);
        }
        Ok(())
    }

    async fn handle_images(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::resolve_config(matches, ctx)?;
        let docker = docker_ops::create_client(&config)
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        let images = docker_ops::list_images(&docker)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;
        println!("{:<14} {:<40} {:<12} {:<20}", "IMAGE ID", "TAG", "SIZE", "CREATED");
        println!("{}", "-".repeat(88));
        for img in images {
            let tag = img.tags.first().cloned().unwrap_or_else(|| "<none>".to_string());
            let size_mb = format!("{:.1} MB", (img.size as f64) / 1_048_576.0);
            println!("{:<14} {:<40} {:<12} {:<20}", img.id, tag, size_mb, img.created);
        }
        Ok(())
    }

    async fn handle_volumes(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::resolve_config(matches, ctx)?;
        let docker = docker_ops::create_client(&config)
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        let volumes = docker_ops::list_volumes(&docker)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;
        println!("{:<12} {:<32} {:<40}", "DRIVER", "VOLUME NAME", "MOUNTPOINT");
        println!("{}", "-".repeat(86));
        for v in volumes {
            println!("{:<12} {:<32} {:<40}", v.driver, v.name, v.mountpoint);
        }
        Ok(())
    }

    async fn handle_networks(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::resolve_config(matches, ctx)?;
        let docker = docker_ops::create_client(&config)
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        let networks = docker_ops::list_networks(&docker)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;
        println!("{:<14} {:<24} {:<12} {:<12}", "NETWORK ID", "NAME", "DRIVER", "SCOPE");
        println!("{}", "-".repeat(64));
        for n in networks {
            println!("{:<14} {:<24} {:<12} {:<12}", n.id, n.name, n.driver, n.scope);
        }
        Ok(())
    }

    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<DockerConfig, VoidbError> {
        let config = ctx
            .find_connection(conn_name)
            .ok_or_else(|| VoidbError::Plugin(format!("Connection '{}' not found", conn_name)))?;

        if config.effective_plugin_id() != "docker" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a Docker connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid Docker config: {}", e)))
            })
    }

    fn parse_tui_launch(
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<DockerTuiLaunch, VoidbError> {
        let fixture_path = matches.get_one::<String>("fixture").cloned();
        let purpose = matches
            .get_one::<String>("purpose")
            .cloned()
            .unwrap_or_else(|| "operations".to_string());
        let readonly = matches.get_flag("readonly");
        let restore = !matches.get_flag("no-restore");

        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (profile, connection) =
                ctx.resolve_profile_connection(profile_ref, Some("docker"))?;
            let config = connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone()).map_err(|e| {
                        VoidbError::Connection(format!("Invalid Docker config: {}", e))
                    })
                })?;
            let profile_arg = profile_ref_arg(profile_ref, &profile.id);
            let request = TuiLaunchRequest::new("docker", profile_arg, purpose.clone())
                .readonly(readonly)
                .restore(restore)
                .raw_input(false);
            let launch_plan = build_tui_launch_plan(&profile, request, "voidb-cli", Utc::now())?;

            return Ok(DockerTuiLaunch {
                profile_label: profile.name,
                config: Some(config),
                source: DockerTuiSource::Profile,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: Some(launch_plan),
            });
        }

        if let Some(conn_name) = matches.get_one::<String>("connection") {
            return Ok(DockerTuiLaunch {
                profile_label: conn_name.clone(),
                config: Some(Self::parse_config(conn_name, ctx)?),
                source: DockerTuiSource::Connection,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        if fixture_path.is_some() {
            return Ok(DockerTuiLaunch {
                profile_label: "fixture".to_string(),
                config: None,
                source: DockerTuiSource::Fixture,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        Err(VoidbError::Plugin(
            "docker tui requires --profile, --connection, or --fixture".to_string(),
        ))
    }

    async fn handle_tui(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let launch = Self::parse_tui_launch(matches, ctx)?;
        if let Some(path) = matches.get_one::<String>("evidence") {
            let evidence = build_docker_tui_evidence(&launch)
                .map_err(|e| VoidbError::Plugin(format!("Docker TUI evidence failed: {}", e)))?;
            let rendered = serde_json::to_string_pretty(&evidence).map_err(|e| {
                VoidbError::Plugin(format!("Docker TUI evidence serialization failed: {}", e))
            })?;
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    VoidbError::Plugin(format!(
                        "Failed to create Docker TUI evidence directory '{}': {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::write(path, rendered).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Failed to write Docker TUI evidence '{}': {}",
                    path, e
                ))
            })?;
            println!("Wrote Docker TUI evidence to {path}");
            return Ok(());
        }

        if matches
            .get_one::<String>("format")
            .is_some_and(|format| format == "json")
        {
            write_docker_tui_preflight(&launch)
                .map_err(|e| VoidbError::Plugin(format!("Docker TUI preflight failed: {}", e)))?;
            return Ok(());
        }

        run_docker_tui(launch)
            .await
            .map_err(|e| VoidbError::Plugin(format!("Docker TUI failed: {}", e)))
    }
}

fn profile_ref_arg(input: &str, profile_id: &str) -> ConnectionProfileRef {
    if let Some(id) = input.strip_prefix("id:") {
        ConnectionProfileRef::Id(id.to_string())
    } else if let Some(name) = input
        .strip_prefix("name:")
        .or_else(|| input.strip_prefix("alias:"))
    {
        ConnectionProfileRef::Name(name.to_string())
    } else if input == profile_id {
        ConnectionProfileRef::Id(input.to_string())
    } else {
        ConnectionProfileRef::Name(input.to_string())
    }
}
