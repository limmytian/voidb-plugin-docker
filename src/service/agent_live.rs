//! Direct Docker handles used by persistent agent sessions.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use bollard::Docker;
use bollard::container::{
    AttachContainerOptions, LogOutput, LogsOptions, ResizeContainerTtyOptions, StatsOptions,
};
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::system::EventsOptions;
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::config::DockerConfig;
use crate::docker_ops;

pub struct DockerAgentService {
    docker: Docker,
}

pub struct DockerExecSpec {
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub user: Option<String>,
    pub working_dir: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

impl DockerAgentService {
    pub async fn connect(config: &DockerConfig) -> Result<Self> {
        let docker = docker_ops::create_client(config)?;
        docker_ops::ping(&docker).await?;
        Ok(Self { docker })
    }

    pub fn logs(
        &self,
        container_id: &str,
        tail: usize,
        since: Option<i64>,
        timestamps: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<DockerLogChunk>> + Send + '_>> {
        let stream = self.docker.logs(
            container_id,
            Some(LogsOptions::<String> {
                follow: true,
                stdout: true,
                stderr: true,
                timestamps,
                tail: tail.to_string(),
                since: since.unwrap_or_default(),
                ..Default::default()
            }),
        );
        Box::pin(stream.map(|item| {
            let output = item.context("Docker log stream failed")?;
            let (stream, bytes) = log_output_parts(output)?;
            Ok(DockerLogChunk {
                stream,
                text: String::from_utf8_lossy(&bytes).to_string(),
            })
        }))
    }

    pub fn stats(
        &self,
        container_id: &str,
    ) -> Pin<Box<dyn Stream<Item = Result<DockerStatsSnapshot>> + Send + '_>> {
        let stream = self.docker.stats(
            container_id,
            Some(StatsOptions {
                stream: true,
                one_shot: false,
            }),
        );
        Box::pin(stream.map(|item| {
            let stats = item.context("Docker stats stream failed")?;
            let cpu_delta = stats.cpu_stats.cpu_usage.total_usage as f64
                - stats.precpu_stats.cpu_usage.total_usage as f64;
            let system_delta = stats.cpu_stats.system_cpu_usage.unwrap_or(0) as f64
                - stats.precpu_stats.system_cpu_usage.unwrap_or(0) as f64;
            let cpus = stats.cpu_stats.online_cpus.unwrap_or(1) as f64;
            let cpu_percent = if system_delta > 0.0 && cpu_delta >= 0.0 {
                (cpu_delta / system_delta) * cpus * 100.0
            } else {
                0.0
            };
            let memory_usage = stats.memory_stats.usage.unwrap_or(0);
            let memory_limit = stats.memory_stats.limit.unwrap_or(0);
            let memory_percent = if memory_limit > 0 {
                memory_usage as f64 / memory_limit as f64 * 100.0
            } else {
                0.0
            };
            let (network_rx, network_tx) = stats
                .networks
                .as_ref()
                .map(|networks| {
                    networks.values().fold((0u64, 0u64), |(rx, tx), network| {
                        (
                            rx.saturating_add(network.rx_bytes),
                            tx.saturating_add(network.tx_bytes),
                        )
                    })
                })
                .unwrap_or((0, 0));
            Ok(DockerStatsSnapshot {
                cpu_percent,
                memory_usage,
                memory_limit,
                memory_percent,
                network_rx,
                network_tx,
            })
        }))
    }

    pub fn events(
        &self,
        since: Option<String>,
        filters: HashMap<String, Vec<String>>,
    ) -> Pin<Box<dyn Stream<Item = Result<DockerDaemonEvent>> + Send + '_>> {
        let stream = self.docker.events(Some(EventsOptions::<String> {
            since,
            until: None,
            filters,
        }));
        Box::pin(stream.map(|item| {
            let event = item.context("Docker daemon event stream failed")?;
            let actor_id = event.actor.as_ref().and_then(|actor| actor.id.clone());
            let mut attribute_keys = event
                .actor
                .as_ref()
                .and_then(|actor| actor.attributes.as_ref())
                .map(|attributes| attributes.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            attribute_keys.sort();
            attribute_keys.truncate(64);
            Ok(DockerDaemonEvent {
                event_type: enum_string(event.typ),
                action: event.action,
                actor_id,
                attribute_keys,
                scope: enum_string(event.scope),
                time: event.time,
                time_nano: event.time_nano,
            })
        }))
    }

    pub async fn open_exec(&self, spec: DockerExecSpec) -> Result<DockerTerminalParts> {
        let created = self
            .docker
            .create_exec(
                &spec.container_id,
                CreateExecOptions {
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(spec.tty),
                    cmd: Some(spec.command),
                    privileged: Some(false),
                    user: spec.user,
                    working_dir: spec.working_dir,
                    ..Default::default()
                },
            )
            .await
            .context("Failed to create controlled Docker exec")?;
        if spec.tty {
            self.docker
                .resize_exec(
                    &created.id,
                    ResizeExecOptions {
                        width: spec.cols,
                        height: spec.rows,
                    },
                )
                .await
                .context("Failed to set Docker exec terminal size")?;
        }
        let started = self
            .docker
            .start_exec(
                &created.id,
                Some(StartExecOptions {
                    detach: false,
                    tty: spec.tty,
                    output_capacity: Some(256 * 1024),
                }),
            )
            .await
            .context("Failed to start controlled Docker exec")?;
        let StartExecResults::Attached { output, input } = started else {
            return Err(anyhow!("Docker exec detached unexpectedly"));
        };
        Ok(DockerTerminalParts {
            output: DockerTerminalOutput { stream: output },
            control: Arc::new(DockerTerminalControl {
                docker: self.docker.clone(),
                target: DockerTerminalTarget::Exec {
                    exec_id: created.id,
                },
                input: Mutex::new(Some(input)),
                tty: spec.tty,
            }),
        })
    }

    pub async fn open_attach(
        &self,
        container_id: &str,
        logs: bool,
        tty: bool,
        cols: u16,
        rows: u16,
    ) -> Result<DockerTerminalParts> {
        let attached = self
            .docker
            .attach_container(
                container_id,
                Some(AttachContainerOptions::<String> {
                    stdin: Some(true),
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    logs: Some(logs),
                    detach_keys: Some("ctrl-]".into()),
                }),
            )
            .await
            .context("Failed to attach to Docker container")?;
        if tty {
            self.docker
                .resize_container_tty(
                    container_id,
                    ResizeContainerTtyOptions {
                        width: cols,
                        height: rows,
                    },
                )
                .await
                .context("Failed to set attached Docker terminal size")?;
        }
        Ok(DockerTerminalParts {
            output: DockerTerminalOutput {
                stream: attached.output,
            },
            control: Arc::new(DockerTerminalControl {
                docker: self.docker.clone(),
                target: DockerTerminalTarget::Attach {
                    container_id: container_id.to_string(),
                },
                input: Mutex::new(Some(attached.input)),
                tty,
            }),
        })
    }
}

#[derive(Debug)]
pub struct DockerLogChunk {
    pub stream: &'static str,
    pub text: String,
}

#[derive(Debug)]
pub struct DockerStatsSnapshot {
    pub cpu_percent: f64,
    pub memory_usage: u64,
    pub memory_limit: u64,
    pub memory_percent: f64,
    pub network_rx: u64,
    pub network_tx: u64,
}

#[derive(Debug)]
pub struct DockerDaemonEvent {
    pub event_type: Option<String>,
    pub action: Option<String>,
    pub actor_id: Option<String>,
    pub attribute_keys: Vec<String>,
    pub scope: Option<String>,
    pub time: Option<i64>,
    pub time_nano: Option<i64>,
}

pub struct DockerTerminalParts {
    pub output: DockerTerminalOutput,
    pub control: Arc<DockerTerminalControl>,
}

pub struct DockerTerminalOutput {
    stream: Pin<Box<dyn Stream<Item = Result<LogOutput, bollard::errors::Error>> + Send>>,
}

impl DockerTerminalOutput {
    pub async fn next(&mut self) -> Option<Result<DockerTerminalChunk>> {
        self.stream.next().await.map(|item| {
            let output = item.context("Docker terminal output failed")?;
            let (stream, bytes) = log_output_parts(output)?;
            Ok(DockerTerminalChunk { stream, bytes })
        })
    }
}

pub struct DockerTerminalChunk {
    pub stream: &'static str,
    pub bytes: Vec<u8>,
}

enum DockerTerminalTarget {
    Exec { exec_id: String },
    Attach { container_id: String },
}

type DockerInput = Pin<Box<dyn AsyncWrite + Send>>;

pub struct DockerTerminalControl {
    docker: Docker,
    target: DockerTerminalTarget,
    input: Mutex<Option<DockerInput>>,
    tty: bool,
}

impl DockerTerminalControl {
    pub async fn write(&self, data: &[u8]) -> Result<()> {
        let mut input = self.input.lock().await;
        let writer = input
            .as_mut()
            .ok_or_else(|| anyhow!("Docker terminal input is closed"))?;
        writer
            .as_mut()
            .write_all(data)
            .await
            .context("Failed to write Docker terminal input")?;
        writer
            .as_mut()
            .flush()
            .await
            .context("Failed to flush Docker terminal input")
    }

    pub async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        if !self.tty {
            return Err(anyhow!("Docker terminal resize requires tty=true"));
        }
        match &self.target {
            DockerTerminalTarget::Exec { exec_id } => self
                .docker
                .resize_exec(
                    exec_id,
                    ResizeExecOptions {
                        width: cols,
                        height: rows,
                    },
                )
                .await
                .context("Failed to resize Docker exec"),
            DockerTerminalTarget::Attach { container_id } => self
                .docker
                .resize_container_tty(
                    container_id,
                    ResizeContainerTtyOptions {
                        width: cols,
                        height: rows,
                    },
                )
                .await
                .context("Failed to resize Docker attach"),
        }
    }

    pub async fn signal_exec(&self, signal: &str) -> Result<()> {
        let DockerTerminalTarget::Exec { exec_id } = &self.target else {
            return Err(anyhow!("Signals are not supported for Docker attach"));
        };
        terminate_exec_process(&self.docker, exec_id, signal).await
    }

    pub async fn close(&self) -> Result<()> {
        let mut input = self.input.lock().await;
        if let Some(mut writer) = input.take() {
            if self.tty && matches!(self.target, DockerTerminalTarget::Exec { .. }) {
                let _ = writer.as_mut().write_all(b"exit\r").await;
                let _ = writer.as_mut().flush().await;
            }
            let _ = writer.as_mut().shutdown().await;
        }
        drop(input);
        if let DockerTerminalTarget::Exec { exec_id } = &self.target {
            for _ in 0..5 {
                let inspect = self
                    .docker
                    .inspect_exec(exec_id)
                    .await
                    .context("Failed to inspect Docker exec during close")?;
                if !inspect.running.unwrap_or(false) {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            terminate_exec_process(&self.docker, exec_id, "TERM").await?;
            for _ in 0..10 {
                let inspect = self
                    .docker
                    .inspect_exec(exec_id)
                    .await
                    .context("Failed to verify Docker exec termination")?;
                if !inspect.running.unwrap_or(false) {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            terminate_exec_process(&self.docker, exec_id, "KILL").await?;
            for _ in 0..10 {
                let inspect = self
                    .docker
                    .inspect_exec(exec_id)
                    .await
                    .context("Failed to verify forced Docker exec termination")?;
                if !inspect.running.unwrap_or(false) {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            return Err(anyhow!("Docker exec remained active after forced cleanup"));
        }
        Ok(())
    }
}

async fn terminate_exec_process(docker: &Docker, exec_id: &str, signal: &str) -> Result<()> {
    let signal = match signal.trim().to_ascii_uppercase().as_str() {
        "INT" | "SIGINT" => "INT",
        "TERM" | "SIGTERM" => "TERM",
        "KILL" | "SIGKILL" => "KILL",
        other => return Err(anyhow!("Unsupported Docker exec signal '{other}'")),
    };
    let inspect = docker
        .inspect_exec(exec_id)
        .await
        .context("Failed to inspect Docker exec process")?;
    if !inspect.running.unwrap_or(false) {
        return Ok(());
    }
    let pid = inspect
        .pid
        .filter(|pid| *pid > 0)
        .ok_or_else(|| anyhow!("Docker exec process ID is unavailable"))?;
    let container_id = inspect
        .container_id
        .filter(|id| !id.is_empty())
        .ok_or_else(|| anyhow!("Docker exec container ID is unavailable"))?;
    let killer = docker
        .create_exec(
            &container_id,
            CreateExecOptions {
                attach_stdin: Some(false),
                attach_stdout: Some(false),
                attach_stderr: Some(false),
                tty: Some(false),
                cmd: Some(vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "kill -\"$1\" \"$2\"".to_string(),
                    "voidb-agent".to_string(),
                    signal.to_string(),
                    pid.to_string(),
                ]),
                privileged: Some(false),
                ..Default::default()
            },
        )
        .await
        .context("Failed to create Docker exec cleanup process")?;
    docker
        .start_exec(
            &killer.id,
            Some(StartExecOptions {
                detach: true,
                tty: false,
                output_capacity: None,
            }),
        )
        .await
        .context("Failed to start Docker exec cleanup process")?;
    Ok(())
}

fn log_output_parts(output: LogOutput) -> Result<(&'static str, Vec<u8>)> {
    match output {
        LogOutput::StdOut { message } => Ok(("stdout", message.to_vec())),
        LogOutput::StdErr { message } => Ok(("stderr", message.to_vec())),
        LogOutput::Console { message } => Ok(("console", message.to_vec())),
        LogOutput::StdIn { .. } => Err(anyhow!("Unexpected Docker stdin output frame")),
    }
}

fn enum_string<T: serde::Serialize>(value: Option<T>) -> Option<String> {
    value.and_then(|value| {
        serde_json::to_value(value)
            .ok()
            .and_then(|value| match value {
                Value::String(value) => Some(value),
                _ => None,
            })
    })
}
