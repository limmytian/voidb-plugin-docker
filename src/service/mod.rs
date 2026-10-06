//! Docker service layer.
//!
//! Provides `DockerService`, the service facade for the Docker plugin.
//! Follows the MySqlService convention with streaming lifecycle management:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - `poll_exec()` drains exec output bytes separately
//! - Streaming operations (logs, stats, exec) are managed via AbortHandles
//! - Render notifications fire after every event emission

pub mod agent_live;
pub mod commands;
pub mod events;

pub use commands::DockerCommand;
pub use events::{DockerEvent, ExecEvent};

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::config::DockerConfig;
use crate::docker_ops;
use voidb_core::TabManager;

/// Docker service facade.
///
/// Owns the command sender and event receiver channels. The background
/// task runs on the shared tokio runtime via `runtime.spawn()`.
///
/// # Send + Sync
///
/// `DockerService` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`.
pub struct DockerService {
    cmd_tx: mpsc::UnboundedSender<DockerCommand>,
    event_rx: mpsc::UnboundedReceiver<DockerEvent>,
    exec_rx: mpsc::UnboundedReceiver<ExecEvent>,
    _task: tokio::task::JoinHandle<()>,
}

impl DockerService {
    /// Create a new DockerService with a background processing task.
    pub fn new(
        _config: DockerConfig,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<DockerCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<DockerEvent>();
        let (exec_tx, exec_rx) = mpsc::unbounded_channel::<ExecEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, exec_tx, tabs));

        Self {
            cmd_tx,
            event_rx,
            exec_rx,
            _task: task,
        }
    }

    /// Send a command to the background service task (non-blocking).
    pub fn send(&self, cmd: DockerCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    /// Poll for the next event from the service (non-blocking).
    pub fn poll_event(&mut self) -> Option<DockerEvent> {
        self.event_rx.try_recv().ok()
    }

    /// Poll for exec output bytes (non-blocking).
    pub fn poll_exec(&mut self) -> Option<ExecEvent> {
        self.exec_rx.try_recv().ok()
    }

    /// Background task that processes commands and manages streaming lifecycles.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<DockerCommand>,
        event_tx: mpsc::UnboundedSender<DockerEvent>,
        exec_event_tx: mpsc::UnboundedSender<ExecEvent>,
        tabs: Arc<dyn TabManager>,
    ) {
        let mut docker: Option<bollard::Docker> = None;
        let mut log_handle: Option<AbortHandle> = None;
        let mut stats_handle: Option<AbortHandle> = None;
        let mut exec_handle: Option<AbortHandle> = None;
        let mut exec_input_tx: Option<mpsc::UnboundedSender<Vec<u8>>> = None;
        let mut exec_id: Option<String> = None;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                // === Connection lifecycle ===
                DockerCommand::Connect { config, reply } => {
                    match docker_ops::create_client(&config) {
                        Ok(client) => match docker_ops::ping(&client).await {
                            Ok(version) => {
                                docker = Some(client);
                                let _ = event_tx.send(DockerEvent::OperationComplete(format!(
                                    "Connected: {}",
                                    version
                                )));
                                let _ = tabs.request_render();
                                let _ = reply.send(Ok(version));
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                let _ = event_tx.send(DockerEvent::Error(msg.clone()));
                                let _ = tabs.request_render();
                                let _ = reply.send(Err(msg));
                            }
                        },
                        Err(e) => {
                            let msg = e.to_string();
                            let _ = event_tx.send(DockerEvent::Error(msg.clone()));
                            let _ = tabs.request_render();
                            let _ = reply.send(Err(msg));
                        }
                    }
                }

                DockerCommand::Disconnect => {
                    Self::cancel_streams(
                        &mut log_handle,
                        &mut stats_handle,
                        &mut exec_handle,
                        &mut exec_input_tx,
                        &mut exec_id,
                    );
                    drop(docker.take());
                    break;
                }

                // === List operations ===
                DockerCommand::ListContainers { all } => {
                    if let Some(ref d) = docker {
                        match docker_ops::list_containers(d, all).await {
                            Ok(containers) => {
                                let _ = event_tx.send(DockerEvent::ContainersLoaded(containers));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                DockerCommand::ListImages => {
                    if let Some(ref d) = docker {
                        match docker_ops::list_images(d).await {
                            Ok(images) => {
                                let _ = event_tx.send(DockerEvent::ImagesLoaded(images));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                DockerCommand::ListNetworks => {
                    if let Some(ref d) = docker {
                        match docker_ops::list_networks(d).await {
                            Ok(networks) => {
                                let _ = event_tx.send(DockerEvent::NetworksLoaded(networks));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                DockerCommand::ListVolumes => {
                    if let Some(ref d) = docker {
                        match docker_ops::list_volumes(d).await {
                            Ok(volumes) => {
                                let _ = event_tx.send(DockerEvent::VolumesLoaded(volumes));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                // === Container operations ===
                DockerCommand::ContainerAction { id, action } => {
                    if let Some(ref d) = docker {
                        let result = match action.as_str() {
                            "start" => docker_ops::start_container(d, &id).await,
                            "stop" => docker_ops::stop_container(d, &id).await,
                            "restart" => docker_ops::restart_container(d, &id).await,
                            "remove" => docker_ops::remove_container(d, &id).await,
                            other => Err(anyhow::anyhow!("Unknown action: {}", other)),
                        };
                        match result {
                            Ok(()) => {
                                let _ = event_tx.send(DockerEvent::OperationComplete(format!(
                                    "Container {} {}",
                                    action, id
                                )));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                DockerCommand::InspectContainer { id } => {
                    if let Some(ref d) = docker {
                        match docker_ops::inspect_container(d, &id).await {
                            Ok(inspect) => {
                                let _ =
                                    event_tx.send(DockerEvent::InspectLoaded(Box::new(inspect)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                // === Streaming: Logs ===
                DockerCommand::StartLogs { container_id, tail } => {
                    if let Some(ref d) = docker {
                        // Cancel existing log stream
                        if let Some(h) = log_handle.take() {
                            h.abort();
                        }
                        let d = d.clone();
                        let tx = event_tx.clone();
                        let tabs_c = tabs.clone();
                        let handle = tokio::spawn(async move {
                            docker_ops::stream_logs(d, container_id, tail, tx).await;
                            let _ = tabs_c.request_render();
                        });
                        log_handle = Some(handle.abort_handle());
                    }
                }

                DockerCommand::StopLogs => {
                    if let Some(h) = log_handle.take() {
                        h.abort();
                    }
                }

                // === Streaming: Stats ===
                DockerCommand::StartStats { container_id } => {
                    if let Some(ref d) = docker {
                        if let Some(h) = stats_handle.take() {
                            h.abort();
                        }
                        let d = d.clone();
                        let tx = event_tx.clone();
                        let tabs_c = tabs.clone();
                        let handle = tokio::spawn(async move {
                            docker_ops::stream_stats(d, container_id, tx).await;
                            let _ = tabs_c.request_render();
                        });
                        stats_handle = Some(handle.abort_handle());
                    }
                }

                DockerCommand::StopStats => {
                    if let Some(h) = stats_handle.take() {
                        h.abort();
                    }
                }

                // === Streaming: Exec ===
                DockerCommand::StartExec {
                    container_id,
                    cols,
                    rows,
                } => {
                    if let Some(ref d) = docker {
                        // Cancel existing exec
                        if let Some(h) = exec_handle.take() {
                            h.abort();
                        }
                        exec_input_tx = None;
                        exec_id = None;

                        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();
                        let exec_evt_tx = exec_event_tx.clone();
                        let tabs_c = tabs.clone();

                        match docker_ops::start_exec_session(
                            d,
                            &container_id,
                            cols,
                            rows,
                            output_tx,
                        )
                        .await
                        {
                            Ok((input_sender, eid)) => {
                                exec_input_tx = Some(input_sender);
                                exec_id = Some(eid);

                                let _ = exec_evt_tx.send(ExecEvent::Started);
                                let _ = tabs_c.request_render();

                                // Forward output bytes
                                let handle = tokio::spawn(async move {
                                    while let Some(bytes) = output_rx.recv().await {
                                        let _ = exec_evt_tx.send(ExecEvent::Output(bytes));
                                        let _ = tabs_c.request_render();
                                    }
                                    let _ = exec_evt_tx.send(ExecEvent::Ended);
                                });
                                exec_handle = Some(handle.abort_handle());
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                                let _ = tabs.request_render();
                            }
                        }
                    }
                }

                DockerCommand::ExecInput { data } => {
                    if let Some(ref tx) = exec_input_tx {
                        let _ = tx.send(data);
                    }
                }

                DockerCommand::ResizeExec { cols, rows } => {
                    if let Some(ref d) = docker
                        && let Some(ref eid) = exec_id
                    {
                        let _ = docker_ops::resize_exec(d, eid, cols, rows).await;
                    }
                }

                DockerCommand::StopExec => {
                    if let Some(h) = exec_handle.take() {
                        h.abort();
                    }
                    exec_input_tx = None;
                    exec_id = None;
                }

                // === Image operations ===
                DockerCommand::PullImage { image } => {
                    if let Some(ref d) = docker {
                        let d = d.clone();
                        let tx = event_tx.clone();
                        let tabs_c = tabs.clone();
                        tokio::spawn(async move {
                            docker_ops::pull_image(d, image, tx).await;
                            let _ = tabs_c.request_render();
                        });
                    }
                }

                DockerCommand::RemoveImage { id } => {
                    if let Some(ref d) = docker {
                        match docker_ops::remove_image(d, &id).await {
                            Ok(()) => {
                                let _ = event_tx.send(DockerEvent::OperationComplete(format!(
                                    "Image {} removed",
                                    id
                                )));
                            }
                            Err(e) => {
                                let _ = event_tx.send(DockerEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }
            }
        }

        // Cleanup on exit
        Self::cancel_streams(
            &mut log_handle,
            &mut stats_handle,
            &mut exec_handle,
            &mut exec_input_tx,
            &mut exec_id,
        );
    }

    fn cancel_streams(
        log_handle: &mut Option<AbortHandle>,
        stats_handle: &mut Option<AbortHandle>,
        exec_handle: &mut Option<AbortHandle>,
        exec_input_tx: &mut Option<mpsc::UnboundedSender<Vec<u8>>>,
        exec_id: &mut Option<String>,
    ) {
        if let Some(h) = log_handle.take() {
            h.abort();
        }
        if let Some(h) = stats_handle.take() {
            h.abort();
        }
        if let Some(h) = exec_handle.take() {
            h.abort();
        }
        *exec_input_tx = None;
        *exec_id = None;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<DockerService>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<DockerCommand>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<DockerEvent>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<DockerService>>();
    }
}
