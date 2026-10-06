//! Docker API operations wrapping the bollard crate

use anyhow::{Context, Result};
use bollard::Docker;
use bollard::container::{
    ListContainersOptions, LogsOptions, RemoveContainerOptions, StatsOptions,
};
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::image::{CreateImageOptions, ListImagesOptions, RemoveImageOptions};
use bollard::models::ContainerInspectResponse;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::error;

use crate::config::{DockerConfig, DockerConnection};
use crate::types::*;

/// Bounded non-follow log output for agent-facing capability calls.
pub struct DockerLogOutput {
    pub text: String,
    pub source_bytes: usize,
    pub truncated: bool,
}

/// Convert a Docker/IO error message into a user-friendly hint.
/// Detects permission-denied errors on Unix sockets and suggests adding
/// the user to the `docker` group.
pub fn friendly_docker_error(err: &anyhow::Error) -> String {
    let msg = err.to_string();
    let lower = msg.to_lowercase();

    if lower.contains("permission denied") || lower.contains("access denied") {
        return format!(
            "{}\n\nHint: Docker socket permission denied.\n\
             On Linux, add your user to the docker group:\n\
             \x20 sudo usermod -aG docker $USER\n\
             Then log out and back in (or run: newgrp docker)",
            msg
        );
    }

    if lower.contains("no such file")
        && (lower.contains("docker.sock") || lower.contains("docker.pipe"))
    {
        return format!(
            "{}\n\nHint: Docker daemon is not running or the socket path is wrong.\n\
             Start Docker: sudo systemctl start docker",
            msg
        );
    }

    if lower.contains("connection refused") {
        return format!(
            "{}\n\nHint: Cannot reach Docker daemon.\n\
             Check that Docker is running and the host/port is correct.",
            msg
        );
    }

    msg
}

/// Resolve the best local Docker socket path for the current platform.
///
/// Priority:
/// 1. `DOCKER_HOST` env var (bollard will pick this up inside
///    `connect_with_socket_defaults`, but we check it first to keep
///    the probe logic simple).
/// 2. `~/.docker/run/docker.sock` — macOS Docker Desktop.
/// 3. `/var/run/docker.sock` — Linux default.
///
/// Returns a `Docker` client using the first path that exists, or falls back
/// to `connect_with_socket_defaults()` which handles the `DOCKER_HOST` env var
/// and platform-specific defaults automatically.
fn connect_local() -> Result<Docker> {
    // If DOCKER_HOST is set, defer entirely to bollard's own resolution.
    if std::env::var_os("DOCKER_HOST").is_some() {
        return Docker::connect_with_socket_defaults()
            .map_err(|e| anyhow::anyhow!("{}", friendly_docker_error(&anyhow::anyhow!(e))));
    }

    // Probe macOS Docker Desktop socket path first.
    if let Ok(home) = std::env::var("HOME") {
        let macos_sock = std::path::Path::new(&home).join(".docker/run/docker.sock");
        if macos_sock.exists() {
            let path = macos_sock.to_string_lossy();
            return Docker::connect_with_socket(&path, 30, bollard::API_DEFAULT_VERSION)
                .map_err(|e| anyhow::anyhow!("{}", friendly_docker_error(&anyhow::anyhow!(e))));
        }
    }

    // Fall back to bollard defaults (/var/run/docker.sock on Linux,
    // npipe on Windows, etc.).
    Docker::connect_with_socket_defaults()
        .map_err(|e| anyhow::anyhow!("{}", friendly_docker_error(&anyhow::anyhow!(e))))
}

/// Create a Docker client from config
pub fn create_client(config: &DockerConfig) -> Result<Docker> {
    let docker = match &config.connection {
        DockerConnection::Local => connect_local()?,
        DockerConnection::Socket { path } => {
            Docker::connect_with_socket(path, config.timeout, bollard::API_DEFAULT_VERSION)
                .map_err(|e| anyhow::anyhow!("{}", friendly_docker_error(&anyhow::anyhow!(e))))?
        }
        DockerConnection::Http { url } => {
            Docker::connect_with_http(url, config.timeout, bollard::API_DEFAULT_VERSION)
                .context(format!("Failed to connect via HTTP: {}", url))?
        }
        DockerConnection::Tls {
            url,
            ca_cert: _,
            cert: _,
            key: _,
        } => {
            // bollard 0.18 doesn't have connect_with_ssl; use HTTP for now
            // TLS support requires bollard 0.20+ or custom transport
            Docker::connect_with_http(url, config.timeout, bollard::API_DEFAULT_VERSION).context(
                format!(
                    "Failed to connect via TLS: {} (TLS not yet supported, using HTTP)",
                    url
                ),
            )?
        }
    };
    Ok(docker)
}

/// Test Docker connection by pinging the daemon
pub async fn ping(docker: &Docker) -> Result<String> {
    let version = docker
        .version()
        .await
        .context("Failed to ping Docker daemon")?;
    let ver = version.version.unwrap_or_else(|| "unknown".to_string());
    let api = version.api_version.unwrap_or_else(|| "unknown".to_string());
    Ok(format!("Docker {} (API {})", ver, api))
}

/// List containers
pub async fn list_containers(docker: &Docker, all: bool) -> Result<Vec<ContainerInfo>> {
    let options = Some(ListContainersOptions::<String> {
        all,
        ..Default::default()
    });
    let containers = docker
        .list_containers(options)
        .await
        .context("Failed to list containers")?;

    Ok(containers
        .into_iter()
        .map(|c| {
            let id =
                c.id.as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(12)
                    .collect::<String>();
            let name = c
                .names
                .as_ref()
                .and_then(|n| n.first())
                .map(|n| n.trim_start_matches('/').to_string())
                .unwrap_or_default();
            let image = c.image.unwrap_or_default();
            let state = c.state.unwrap_or_default();
            let status = c.status.unwrap_or_default();
            let ports = c
                .ports
                .as_ref()
                .map(|ports| {
                    ports
                        .iter()
                        .map(|p| {
                            let private = p.private_port;
                            if let (Some(pub_port), Some(typ)) = (p.public_port, &p.typ) {
                                format!("{}:{}/{}", pub_port, private, typ)
                            } else {
                                format!("{}", private)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let created = c.created.map(format_timestamp).unwrap_or_default();

            ContainerInfo {
                id,
                name,
                image,
                state,
                status,
                ports,
                created,
            }
        })
        .collect())
}

/// Start a container
pub async fn start_container(docker: &Docker, id: &str) -> Result<()> {
    docker
        .start_container::<String>(id, None)
        .await
        .context(format!("Failed to start container {}", id))
}

/// Stop a container
pub async fn stop_container(docker: &Docker, id: &str) -> Result<()> {
    docker
        .stop_container(id, None)
        .await
        .context(format!("Failed to stop container {}", id))
}

/// Restart a container
pub async fn restart_container(docker: &Docker, id: &str) -> Result<()> {
    docker
        .restart_container(id, None)
        .await
        .context(format!("Failed to restart container {}", id))
}

/// Remove a container (force)
pub async fn remove_container(docker: &Docker, id: &str) -> Result<()> {
    docker
        .remove_container(
            id,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .context(format!("Failed to remove container {}", id))
}

/// Inspect a container (full details)
pub async fn inspect_container(docker: &Docker, id: &str) -> Result<ContainerInspectResponse> {
    docker
        .inspect_container(id, None)
        .await
        .context(format!("Failed to inspect container {}", id))
}

/// Stream container logs, sending lines to a channel
pub async fn stream_logs(
    docker: Docker,
    container_id: String,
    tail: usize,
    tx: mpsc::UnboundedSender<DockerEvent>,
) {
    let options = Some(LogsOptions::<String> {
        follow: true,
        stdout: true,
        stderr: true,
        timestamps: true,
        tail: tail.to_string(),
        ..Default::default()
    });

    let mut stream = docker.logs(&container_id, options);
    while let Some(result) = stream.next().await {
        match result {
            Ok(output) => {
                let (is_stderr, text) = match output {
                    bollard::container::LogOutput::StdErr { message } => {
                        (true, String::from_utf8_lossy(&message).to_string())
                    }
                    bollard::container::LogOutput::StdOut { message } => {
                        (false, String::from_utf8_lossy(&message).to_string())
                    }
                    bollard::container::LogOutput::Console { message } => {
                        (false, String::from_utf8_lossy(&message).to_string())
                    }
                    bollard::container::LogOutput::StdIn { message: _ } => continue,
                };
                if tx.send(DockerEvent::LogLine { is_stderr, text }).is_err() {
                    break;
                }
            }
            Err(e) => {
                let _ = tx.send(DockerEvent::Error(format!("Log stream error: {}", e)));
                break;
            }
        }
    }
}

/// Read bounded container logs without following the stream.
pub async fn read_logs(
    docker: &Docker,
    container_id: &str,
    tail: usize,
    max_bytes: usize,
) -> Result<DockerLogOutput> {
    let options = Some(LogsOptions::<String> {
        follow: false,
        stdout: true,
        stderr: true,
        timestamps: true,
        tail: tail.to_string(),
        ..Default::default()
    });

    let mut stream = docker.logs(container_id, options);
    let mut text = String::new();
    let mut source_bytes = 0usize;
    let mut truncated = false;

    while let Some(result) = stream.next().await {
        let output = result.context("Failed to read Docker logs")?;
        let chunk = match output {
            bollard::container::LogOutput::StdErr { message }
            | bollard::container::LogOutput::StdOut { message }
            | bollard::container::LogOutput::Console { message } => {
                String::from_utf8_lossy(&message).to_string()
            }
            bollard::container::LogOutput::StdIn { message: _ } => continue,
        };
        source_bytes = source_bytes.saturating_add(chunk.len());
        if truncated {
            continue;
        }
        if push_bounded_text(&mut text, &chunk, max_bytes) {
            truncated = true;
        }
    }

    Ok(DockerLogOutput {
        text,
        source_bytes,
        truncated,
    })
}

fn push_bounded_text(buffer: &mut String, chunk: &str, max_bytes: usize) -> bool {
    let remaining = max_bytes.saturating_sub(buffer.len());
    if chunk.len() <= remaining {
        buffer.push_str(chunk);
        return false;
    }

    for ch in chunk.chars() {
        let len = ch.len_utf8();
        if buffer.len().saturating_add(len) > max_bytes {
            return true;
        }
        buffer.push(ch);
    }
    false
}

/// Stream container stats, sending updates to a channel
pub async fn stream_stats(
    docker: Docker,
    container_id: String,
    tx: mpsc::UnboundedSender<DockerEvent>,
) {
    let options = Some(StatsOptions {
        stream: true,
        one_shot: false,
    });

    let mut stream = docker.stats(&container_id, options);
    while let Some(result) = stream.next().await {
        match result {
            Ok(stats) => {
                let cpu_percent = calculate_cpu_percent(&stats);
                let mem_usage = stats.memory_stats.usage.unwrap_or(0);
                let mem_limit = stats.memory_stats.limit.unwrap_or(1);
                let mem_percent = if mem_limit > 0 {
                    (mem_usage as f64 / mem_limit as f64) * 100.0
                } else {
                    0.0
                };

                let (net_rx, net_tx) = stats
                    .networks
                    .as_ref()
                    .map(|nets| {
                        nets.values().fold((0u64, 0u64), |(rx, tx), n| {
                            (rx + n.rx_bytes, tx + n.tx_bytes)
                        })
                    })
                    .unwrap_or((0, 0));

                let event = DockerEvent::StatsUpdate(ContainerStats {
                    cpu_percent,
                    mem_usage,
                    mem_limit,
                    mem_percent,
                    net_rx,
                    net_tx,
                });
                if tx.send(event).is_err() {
                    break;
                }
            }
            Err(e) => {
                error!("Stats stream error: {}", e);
                break;
            }
        }
    }
}

/// List images
pub async fn list_images(docker: &Docker) -> Result<Vec<ImageInfo>> {
    let images = docker
        .list_images(Some(ListImagesOptions::<String> {
            all: false,
            ..Default::default()
        }))
        .await
        .context("Failed to list images")?;

    Ok(images
        .into_iter()
        .map(|img| {
            let id = img.id.chars().skip(7).take(12).collect::<String>();
            let tags = img.repo_tags;
            let size = img.size.max(0) as u64;
            let created = format_timestamp(img.created);

            ImageInfo {
                id,
                tags,
                size,
                created,
            }
        })
        .collect())
}

/// Pull an image, streaming progress events to a channel
pub async fn pull_image(
    docker: Docker,
    image: String,
    tx: tokio::sync::mpsc::UnboundedSender<DockerEvent>,
) {
    // Split "repo:tag" into from_image and tag
    let (from_image, tag) = if let Some(pos) = image.rfind(':') {
        (&image[..pos], &image[pos + 1..])
    } else {
        (image.as_str(), "latest")
    };

    let options = Some(CreateImageOptions {
        from_image,
        tag,
        ..Default::default()
    });

    let mut stream = docker.create_image(options, None, None);
    while let Some(result) = stream.next().await {
        match result {
            Ok(info) => {
                let status = info.status.unwrap_or_default();
                let progress = info.progress.unwrap_or_default();
                if tx
                    .send(DockerEvent::PullProgress { status, progress })
                    .is_err()
                {
                    break;
                }
            }
            Err(e) => {
                let _ = tx.send(DockerEvent::Error(format!("Pull failed: {}", e)));
                return;
            }
        }
    }
    let _ = tx.send(DockerEvent::PullComplete);
}

/// Remove an image
pub async fn remove_image(docker: &Docker, image: &str) -> Result<()> {
    docker
        .remove_image(
            image,
            Some(RemoveImageOptions {
                force: false,
                noprune: false,
            }),
            None,
        )
        .await
        .context(format!("Failed to remove image {}", image))?;
    Ok(())
}

/// List networks
pub async fn list_networks(docker: &Docker) -> Result<Vec<NetworkInfo>> {
    let networks = docker
        .list_networks::<String>(None)
        .await
        .context("Failed to list networks")?;

    Ok(networks
        .into_iter()
        .map(|n| NetworkInfo {
            id: n.id.as_deref().unwrap_or("").chars().take(12).collect(),
            name: n.name.unwrap_or_default(),
            driver: n.driver.unwrap_or_default(),
            scope: n.scope.unwrap_or_default(),
        })
        .collect())
}

/// Create and start an interactive exec session (shell) in a container.
/// Returns a channel sender for input and spawns output streaming to the event channel.
pub async fn start_exec_session(
    docker: &Docker,
    container_id: &str,
    cols: u16,
    rows: u16,
    output_tx: mpsc::UnboundedSender<Vec<u8>>,
) -> Result<(mpsc::UnboundedSender<Vec<u8>>, String)> {
    let exec = docker
        .create_exec(
            container_id,
            CreateExecOptions {
                attach_stdin: Some(true),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                tty: Some(true),
                cmd: Some(vec!["/bin/sh", "-l"]),
                ..Default::default()
            },
        )
        .await
        .context("Failed to create exec")?;

    let exec_id = exec.id.clone();

    // Resize to match terminal
    let _ = docker
        .resize_exec(
            &exec_id,
            ResizeExecOptions {
                width: cols,
                height: rows,
            },
        )
        .await;

    let result = docker
        .start_exec(
            &exec_id,
            Some(StartExecOptions {
                detach: false,
                tty: true,
                ..Default::default()
            }),
        )
        .await
        .context("Failed to start exec")?;

    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    match result {
        StartExecResults::Attached {
            mut output,
            mut input,
        } => {
            // Spawn output reader
            tokio::spawn(async move {
                while let Some(result) = output.next().await {
                    match result {
                        Ok(log_output) => {
                            let bytes = match log_output {
                                bollard::container::LogOutput::StdOut { message } => {
                                    message.to_vec()
                                }
                                bollard::container::LogOutput::StdErr { message } => {
                                    message.to_vec()
                                }
                                bollard::container::LogOutput::Console { message } => {
                                    message.to_vec()
                                }
                                bollard::container::LogOutput::StdIn { .. } => continue,
                            };
                            if output_tx.send(bytes).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Exec output error: {}", e);
                            break;
                        }
                    }
                }
            });

            // Spawn input writer
            tokio::spawn(async move {
                while let Some(data) = input_rx.recv().await {
                    if input.write_all(&data).await.is_err() {
                        break;
                    }
                    if input.flush().await.is_err() {
                        break;
                    }
                }
            });
        }
        StartExecResults::Detached => {
            return Err(anyhow::anyhow!("Exec detached unexpectedly"));
        }
    }

    Ok((input_tx, exec_id))
}

/// Resize an exec session TTY
pub async fn resize_exec(docker: &Docker, exec_id: &str, cols: u16, rows: u16) -> Result<()> {
    docker
        .resize_exec(
            exec_id,
            ResizeExecOptions {
                width: cols,
                height: rows,
            },
        )
        .await
        .context("Failed to resize exec")?;
    Ok(())
}

/// List volumes
pub async fn list_volumes(docker: &Docker) -> Result<Vec<VolumeInfo>> {
    let volumes = docker
        .list_volumes::<String>(None)
        .await
        .context("Failed to list volumes")?;

    let volume_list = volumes.volumes.unwrap_or_default();
    Ok(volume_list
        .into_iter()
        .map(|v| VolumeInfo {
            name: v.name,
            driver: v.driver,
            mountpoint: v.mountpoint,
        })
        .collect())
}

/// Calculate CPU usage percentage from stats delta
fn calculate_cpu_percent(stats: &bollard::container::Stats) -> f64 {
    let cpu_delta = stats.cpu_stats.cpu_usage.total_usage as f64
        - stats.precpu_stats.cpu_usage.total_usage as f64;
    let sys_delta = stats.cpu_stats.system_cpu_usage.unwrap_or(0) as f64
        - stats.precpu_stats.system_cpu_usage.unwrap_or(0) as f64;
    let num_cpus = stats.cpu_stats.online_cpus.unwrap_or(1) as f64;

    if sys_delta > 0.0 && cpu_delta >= 0.0 {
        (cpu_delta / sys_delta) * num_cpus * 100.0
    } else {
        0.0
    }
}

/// Format a Unix timestamp to a relative time string
fn format_timestamp(ts: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let diff = now - ts;

    if diff < 60 {
        format!("{}s ago", diff)
    } else if diff < 3600 {
        format!("{}m ago", diff / 60)
    } else if diff < 86400 {
        format!("{}h ago", diff / 3600)
    } else {
        format!("{}d ago", diff / 86400)
    }
}
