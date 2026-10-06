use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use voidb_core::{
    ActorRef, ActorType, AgentContextShare, AgentContextSharePolicy, AgentContextShareStatus,
    AgentContextShareStore, AgentOperation, AgentOperationConfirmation, AgentOperationRequest,
    AgentOperationTarget, AppConfig, AssistBoundedText, AssistContextPolicy, AssistContextSnapshot,
    AssistOwnerLease, AssistPluginState, AssistSessionBinding, AssistWithheldField,
    AssistWithholdingReason, DEFAULT_ASSIST_REQUEST_TTL_SECONDS, PluginSessionHealth,
    PluginSessionPurpose, PluginSessionRegistration, PluginSessionScope, RedactionStatus, TabInfo,
    TabManager, TuiLaunchPlan, retained_tui_quality_gate,
};
#[cfg(test)]
use voidb_core::{AgentOperationRisk, AgentPrincipal, AssistPermission};

use crate::config::{DockerConfig, DockerConnection};
use crate::service::{DockerCommand, DockerEvent, DockerService, ExecEvent};
use crate::types::{ContainerInfo, ContainerStats, ImageInfo, NetworkInfo, VolumeInfo};

const LOG_LINE_LIMIT: usize = 2_000;
const LOG_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const EXEC_LINE_LIMIT: usize = 1_000;
const EXEC_BYTE_LIMIT: usize = 1024 * 1024;
const OPERATION_SYNC_INTERVAL: Duration = Duration::from_millis(250);
pub const DOCKER_AGENT_CONTEXT_STORE_DIR_ENV: &str = "VOIDB_DOCKER_AGENT_CONTEXT_DIR";
#[doc(hidden)]
pub const LEGACY_DOCKER_ASSIST_STORE_DIR_ENV: &str = "VOIDB_DOCKER_ASSIST_DIR";

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DockerTuiSource {
    Profile,
    Connection,
    Fixture,
}

#[derive(Debug, Clone)]
pub struct DockerTuiLaunch {
    pub profile_label: String,
    pub config: Option<DockerConfig>,
    pub source: DockerTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

#[derive(Debug, Deserialize)]
struct DockerTuiFixture {
    profile_label: Option<String>,
    endpoint_label: String,
    connection_kind: String,
    #[serde(default)]
    containers: Vec<FixtureContainer>,
    #[serde(default)]
    images: Vec<FixtureImage>,
    #[serde(default)]
    networks: Vec<FixtureNetwork>,
    #[serde(default)]
    volumes: Vec<FixtureVolume>,
    #[serde(default)]
    logs: Vec<FixtureLogLine>,
    stats: Option<FixtureStats>,
    status: Option<String>,
    permission_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureContainer {
    id: String,
    name: String,
    image: String,
    state: String,
    status: String,
    #[serde(default)]
    ports: String,
    #[serde(default)]
    created: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureImage {
    id: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    created: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureNetwork {
    id: String,
    name: String,
    driver: String,
    scope: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureVolume {
    name: String,
    driver: String,
    mountpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureLogLine {
    #[serde(default)]
    is_stderr: bool,
    text: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureStats {
    cpu_percent: f64,
    mem_usage: u64,
    mem_limit: u64,
    mem_percent: f64,
    net_rx: u64,
    net_tx: u64,
}

#[derive(Debug, Clone)]
struct DockerTuiData {
    profile_label: String,
    endpoint_label: String,
    connection_kind: String,
    containers: Vec<ContainerView>,
    images: Vec<ImageView>,
    networks: Vec<NetworkView>,
    volumes: Vec<VolumeView>,
    logs: Vec<LogLineView>,
    stats: Option<StatsView>,
    status: String,
    permission_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerView {
    id: String,
    name: String,
    image: String,
    state: String,
    status: String,
    ports: String,
    created: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageView {
    id: String,
    tags: Vec<String>,
    size: u64,
    created: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NetworkView {
    id: String,
    name: String,
    driver: String,
    scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VolumeView {
    name: String,
    driver: String,
    mountpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogLineView {
    is_stderr: bool,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
struct StatsView {
    cpu_percent: f64,
    mem_usage: u64,
    mem_limit: u64,
    mem_percent: f64,
    net_rx: u64,
    net_tx: u64,
}

impl From<ContainerInfo> for ContainerView {
    fn from(container: ContainerInfo) -> Self {
        Self {
            id: container.id,
            name: container.name,
            image: container.image,
            state: container.state,
            status: container.status,
            ports: container.ports,
            created: container.created,
        }
    }
}

impl From<FixtureContainer> for ContainerView {
    fn from(container: FixtureContainer) -> Self {
        Self {
            id: container.id,
            name: container.name,
            image: container.image,
            state: container.state,
            status: container.status,
            ports: container.ports,
            created: container.created,
        }
    }
}

impl From<ImageInfo> for ImageView {
    fn from(image: ImageInfo) -> Self {
        Self {
            id: image.id,
            tags: image.tags,
            size: image.size,
            created: image.created,
        }
    }
}

impl From<FixtureImage> for ImageView {
    fn from(image: FixtureImage) -> Self {
        Self {
            id: image.id,
            tags: image.tags,
            size: image.size,
            created: image.created,
        }
    }
}

impl From<NetworkInfo> for NetworkView {
    fn from(network: NetworkInfo) -> Self {
        Self {
            id: network.id,
            name: network.name,
            driver: network.driver,
            scope: network.scope,
        }
    }
}

impl From<FixtureNetwork> for NetworkView {
    fn from(network: FixtureNetwork) -> Self {
        Self {
            id: network.id,
            name: network.name,
            driver: network.driver,
            scope: network.scope,
        }
    }
}

impl From<VolumeInfo> for VolumeView {
    fn from(volume: VolumeInfo) -> Self {
        Self {
            name: volume.name,
            driver: volume.driver,
            mountpoint: volume.mountpoint,
        }
    }
}

impl From<FixtureVolume> for VolumeView {
    fn from(volume: FixtureVolume) -> Self {
        Self {
            name: volume.name,
            driver: volume.driver,
            mountpoint: volume.mountpoint,
        }
    }
}

impl From<ContainerStats> for StatsView {
    fn from(stats: ContainerStats) -> Self {
        Self {
            cpu_percent: stats.cpu_percent,
            mem_usage: stats.mem_usage,
            mem_limit: stats.mem_limit,
            mem_percent: stats.mem_percent,
            net_rx: stats.net_rx,
            net_tx: stats.net_tx,
        }
    }
}

impl From<FixtureStats> for StatsView {
    fn from(stats: FixtureStats) -> Self {
        Self {
            cpu_percent: stats.cpu_percent,
            mem_usage: stats.mem_usage,
            mem_limit: stats.mem_limit,
            mem_percent: stats.mem_percent,
            net_rx: stats.net_rx,
            net_tx: stats.net_tx,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceKind {
    Containers,
    Images,
    Networks,
    Volumes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
    Help,
    Error,
    Exec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceItem {
    Container(ContainerView),
    Image(ImageView),
    Network(NetworkView),
    Volume(VolumeView),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationKind {
    StartContainer,
    StopContainer,
    RestartContainer,
    RemoveContainer,
    ExecShell,
    RemoveImage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationPlanView {
    kind: OperationKind,
    target_id: String,
    target_label: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
}

#[derive(Debug, Clone)]
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_docker_tui_preflight(launch: &DockerTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_docker_tui_evidence(launch: &DockerTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("docker tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let containers = data
        .containers
        .iter()
        .map(|container| {
            json!({
                "id": container.id,
                "name": container.name,
                "image": container.image,
                "state": container.state,
                "status": container.status,
                "ports": container.ports,
                "created": container.created
            })
        })
        .collect::<Vec<_>>();
    let images = data
        .images
        .iter()
        .map(|image| {
            json!({
                "id": image.id,
                "tags": image.tags,
                "size": image.size,
                "created": image.created
            })
        })
        .collect::<Vec<_>>();
    let networks = data
        .networks
        .iter()
        .map(|network| {
            json!({
                "id": network.id,
                "name": network.name,
                "driver": network.driver,
                "scope": network.scope
            })
        })
        .collect::<Vec<_>>();
    let volumes = data
        .volumes
        .iter()
        .map(|volume| {
            json!({
                "name": volume.name,
                "driver": volume.driver,
                "mountpoint": redacted_mountpoint(&volume.mountpoint)
            })
        })
        .collect::<Vec<_>>();
    let logs = data
        .logs
        .iter()
        .map(|line| {
            json!({
                "stream": if line.is_stderr { "stderr" } else { "stdout" },
                "text": line.text
            })
        })
        .collect::<Vec<_>>();

    let lifecycle_target = data
        .containers
        .first()
        .map(|container| container.name.clone())
        .unwrap_or_else(|| "fixture-container".to_string());
    let mut evidence = json!({
        "schema_version": 1,
        "kind": "docker_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "docker",
            &["fixture-docker-ops", "fixture local daemon"],
            &["permission_error", "permission denied"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "endpoint_label": data.endpoint_label,
            "connection_kind": data.connection_kind,
            "active_resource": "containers",
            "containers": containers,
            "images": images,
            "networks": networks,
            "volumes": volumes,
            "logs": {
                "line_count": logs.len(),
                "lines": logs,
                "dropped_lines": 0,
                "byte_limit": LOG_BYTE_LIMIT
            },
            "stats": data.stats.as_ref().map(|stats| {
                json!({
                    "cpu_percent": stats.cpu_percent,
                    "mem_usage": stats.mem_usage,
                    "mem_limit": stats.mem_limit,
                    "mem_percent": stats.mem_percent,
                    "net_rx": stats.net_rx,
                    "net_tx": stats.net_tx
                })
            }),
            "permission_error": data.permission_error
        },
        "plan_transcript": [
            {
                "action": "stop_container",
                "target": lifecycle_target,
                "risk": "destructive",
                "first_confirmation": "summary_visible",
                "second_confirmation": "required_before_service_command",
                "readonly_behavior": "plan_visible_but_blocked"
            },
            {
                "action": "exec_shell",
                "target": lifecycle_target,
                "risk": "side_effecting",
                "escape": "Ctrl+] then q",
                "stdin_capture": "redacted"
            }
        ],
        "coverage": [
            "startup",
            "container_browse",
            "image_network_volume_browse",
            "metadata_preview",
            "filter",
            "log_stream_bounds",
            "stats_preview",
            "lifecycle_plan",
            "destructive_double_confirmation",
            "readonly_block",
            "exec_escape_prompt",
            "context_share_snapshot",
            "external_agent_operation_review",
            "operation_decision_record",
            "current_pty_rejected",
            "permission_error",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "external_agent_interaction": {
            "store_env": DOCKER_AGENT_CONTEXT_STORE_DIR_ENV,
            "broker_policy": "non_pty",
            "context_share": {
                "active_resource": "containers",
                "target": lifecycle_target,
                "bounded_context": true,
                "withheld_fields": [
                    "docker.client_handle",
                    "docker.profile_config",
                    "volumes.mountpoint",
                    "raw_container_env"
                ]
            },
            "operation_review": {
                "capability": "docker.container_action",
                "current_pty_allowed": false,
                "decision_record": "AgentOperationConfirmation",
                "service_execution_requires_existing_plan_confirmation": true
            }
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_docker_tui(launch: DockerTuiLaunch) -> Result<()> {
    let mut app = DockerTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &DockerTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.connection_kind,
                    "endpoint_label": data.endpoint_label,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "endpoint_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "docker tui",
        "plugin_id": "docker",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "connection": connection,
        "privacy": {
            "diagnostics_include_registry_tokens": false,
            "diagnostics_include_container_env": false,
            "diagnostics_include_raw_exec_input": false,
            "volume_mountpoints_are_redacted_in_evidence": true
        },
        "stream_bounds": {
            "logs": { "max_lines": LOG_LINE_LIMIT, "max_bytes": LOG_BYTE_LIMIT },
            "exec": { "max_lines": EXEC_LINE_LIMIT, "max_bytes": EXEC_BYTE_LIMIT }
        },
        "service_boundary": "DockerService::Channel",
        "modes": [
            "containers",
            "images",
            "networks",
            "volumes",
            "details",
            "filter",
            "logs",
            "stats",
            "operation_plan",
            "exec_escape_prompt",
            "permission_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut DockerTuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        dirty |= app.sync_agent_operation();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized docker view to {cols}x{rows}");
                    if app.mode == Mode::Exec
                        && let Some(service) = &app.service
                    {
                        service.send(DockerCommand::ResizeExec { cols, rows });
                    }
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn load_fixture(path: &str) -> Result<DockerTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: DockerTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(DockerTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-docker".to_string()),
        endpoint_label: fixture.endpoint_label,
        connection_kind: fixture.connection_kind,
        containers: fixture
            .containers
            .into_iter()
            .map(ContainerView::from)
            .collect(),
        images: fixture.images.into_iter().map(ImageView::from).collect(),
        networks: fixture
            .networks
            .into_iter()
            .map(NetworkView::from)
            .collect(),
        volumes: fixture.volumes.into_iter().map(VolumeView::from).collect(),
        logs: fixture
            .logs
            .into_iter()
            .map(|line| LogLineView {
                is_stderr: line.is_stderr,
                text: line.text,
            })
            .collect(),
        stats: fixture.stats.map(StatsView::from),
        status: fixture
            .status
            .unwrap_or_else(|| "fixture docker operations ready".to_string()),
        permission_error: fixture.permission_error,
    })
}

fn config_data(profile_label: String, config: &DockerConfig) -> DockerTuiData {
    let (connection_kind, endpoint_label) = connection_labels(config);
    DockerTuiData {
        profile_label,
        endpoint_label,
        connection_kind,
        containers: Vec::new(),
        images: Vec::new(),
        networks: Vec::new(),
        volumes: Vec::new(),
        logs: Vec::new(),
        stats: None,
        status: "connecting through DockerService channel mode".to_string(),
        permission_error: None,
    }
}

struct DockerTuiApp {
    profile_label: String,
    endpoint_label: String,
    connection_kind: String,
    source: DockerTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    active: ResourceKind,
    containers: Vec<ContainerView>,
    images: Vec<ImageView>,
    networks: Vec<NetworkView>,
    volumes: Vec<VolumeView>,
    selected: usize,
    filter: String,
    service: Option<DockerService>,
    operation_plan: Option<OperationPlanView>,
    status: String,
    mode: Mode,
    return_mode: Mode,
    logs: BoundedLines,
    exec_output: BoundedLines,
    stats: Option<StatsView>,
    active_log_container: Option<String>,
    active_stats_container: Option<String>,
    exec_target: Option<String>,
    exec_escape_armed: bool,
    context_share_store: AgentContextShareStore,
    context_share: Option<AgentContextShare>,
    context_share_owner_lease: Option<AssistOwnerLease>,
    operation_request: Option<AgentOperationRequest>,
    operation_request_seen_id: Option<String>,
    operation_confirmation: Option<AgentOperationConfirmation>,
    context_share_sequence: u64,
    last_operation_sync: Instant,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl DockerTuiApp {
    fn new(launch: DockerTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("docker tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("docker tui requires Docker config outside fixture mode")?;
            let tabs = Arc::new(StandaloneDockerTabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            let service = DockerService::new(config.clone(), tabs, runtime);
            let (reply, _reply_rx) = oneshot::channel();
            service.send(DockerCommand::Connect { config, reply });
            Some(service)
        } else {
            None
        };

        let mut logs = BoundedLines::new(LOG_LINE_LIMIT, LOG_BYTE_LIMIT);
        for line in &data.logs {
            let prefix = if line.is_stderr { "stderr" } else { "stdout" };
            logs.push_line(format!("{prefix}: {}", line.text));
        }
        let status = data
            .permission_error
            .as_deref()
            .map(|error| format!("{} | {}", data.status, safe_error_summary(error)))
            .unwrap_or_else(|| data.status.clone());

        Ok(Self {
            profile_label: data.profile_label,
            endpoint_label: data.endpoint_label,
            connection_kind: data.connection_kind,
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            active: ResourceKind::Containers,
            containers: data.containers,
            images: data.images,
            networks: data.networks,
            volumes: data.volumes,
            selected: 0,
            filter: String::new(),
            service,
            operation_plan: None,
            status,
            mode: Mode::Browser,
            return_mode: Mode::Browser,
            logs,
            exec_output: BoundedLines::new(EXEC_LINE_LIMIT, EXEC_BYTE_LIMIT),
            stats: data.stats,
            active_log_container: None,
            active_stats_container: None,
            exec_target: None,
            exec_escape_armed: false,
            context_share_store: docker_context_share_store()?,
            context_share: None,
            context_share_owner_lease: None,
            operation_request: None,
            operation_request_seen_id: None,
            operation_confirmation: None,
            context_share_sequence: 0,
            last_operation_sync: Instant::now() - OPERATION_SYNC_INTERVAL,
            should_quit: false,
            render_quit,
        })
    }

    fn shutdown(&mut self) {
        self.cancel_context_share_on_shutdown();
        if let Some(service) = &self.service {
            service.send(DockerCommand::StopLogs);
            service.send(DockerCommand::StopStats);
            service.send(DockerCommand::StopExec);
            service.send(DockerCommand::Disconnect);
        }
    }

    fn cancel_context_share_on_shutdown(&mut self) {
        let Some(share_id) = self.context_share.as_ref().map(|share| share.id.clone()) else {
            return;
        };
        let now = Utc::now();
        let _ = self.context_share_store.update_plugin_state(
            &share_id,
            AssistPluginState {
                mode: mode_label(self.mode).to_string(),
                health: PluginSessionHealth::Closed,
                status: "Docker TUI owner closed".to_string(),
                updated_at: now,
                metadata: json!({}),
                redaction: RedactionStatus::NotRequired,
            },
        );
        let _ = self.context_share_store.cancel(&share_id);
        self.operation_request = None;
        if self.operation_confirmation.take().is_some() {
            self.operation_plan = None;
        }
        self.context_share_owner_lease = None;
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        while let Some(event) = service.poll_exec() {
            changed = true;
            self.handle_exec_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: DockerEvent) {
        match event {
            DockerEvent::ContainersLoaded(containers) => {
                self.containers = containers.into_iter().map(ContainerView::from).collect();
                self.containers.sort_by(container_sort);
                self.selected = self
                    .selected
                    .min(self.filtered_items().len().saturating_sub(1));
                self.status = format!("listed {} containers", self.containers.len());
            }
            DockerEvent::ImagesLoaded(images) => {
                self.images = images.into_iter().map(ImageView::from).collect();
                self.images.sort_by(image_sort);
                self.selected = self
                    .selected
                    .min(self.filtered_items().len().saturating_sub(1));
                self.status = format!("listed {} images", self.images.len());
            }
            DockerEvent::NetworksLoaded(networks) => {
                self.networks = networks.into_iter().map(NetworkView::from).collect();
                self.networks
                    .sort_by(|left, right| left.name.cmp(&right.name));
                self.selected = self
                    .selected
                    .min(self.filtered_items().len().saturating_sub(1));
                self.status = format!("listed {} networks", self.networks.len());
            }
            DockerEvent::VolumesLoaded(volumes) => {
                self.volumes = volumes.into_iter().map(VolumeView::from).collect();
                self.volumes
                    .sort_by(|left, right| left.name.cmp(&right.name));
                self.selected = self
                    .selected
                    .min(self.filtered_items().len().saturating_sub(1));
                self.status = format!("listed {} volumes", self.volumes.len());
            }
            DockerEvent::InspectLoaded(_) => {
                self.status = "inspect data loaded; raw env and mounts stay redacted".to_string();
            }
            DockerEvent::StatsUpdate(stats) => {
                self.stats = Some(StatsView::from(stats));
                self.status = "stats stream updated".to_string();
            }
            DockerEvent::LogLine { is_stderr, text } => {
                let prefix = if is_stderr { "stderr" } else { "stdout" };
                self.logs
                    .push_line(format!("{prefix}: {}", text.trim_end()));
                self.status = format!(
                    "log stream updated; retained {} lines, dropped {}",
                    self.logs.len(),
                    self.logs.dropped_lines
                );
            }
            DockerEvent::PullProgress { status, progress } => {
                self.status = if progress.is_empty() {
                    format!("pull: {status}")
                } else {
                    format!("pull: {status} {progress}")
                };
            }
            DockerEvent::PullComplete => {
                self.status = "image pull complete".to_string();
                self.refresh_current();
            }
            DockerEvent::OperationComplete(message) => {
                let connected = message.starts_with("Connected:");
                self.status = safe_error_summary(&message);
                self.operation_plan = None;
                if connected {
                    self.refresh_all();
                } else {
                    self.refresh_current();
                }
            }
            DockerEvent::Error(message) => {
                self.mode = Mode::Error;
                self.status = safe_error_summary(&message);
            }
        }
    }

    fn handle_exec_event(&mut self, event: ExecEvent) {
        match event {
            ExecEvent::Started => {
                self.mode = Mode::Exec;
                self.exec_escape_armed = false;
                self.status = "exec attached; escape with Ctrl+] then q".to_string();
            }
            ExecEvent::Output(bytes) => {
                self.exec_output.push_text(&String::from_utf8_lossy(&bytes));
            }
            ExecEvent::Ended => {
                self.mode = Mode::Browser;
                self.exec_target = None;
                self.status = "exec session ended".to_string();
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if self.mode == Mode::Exec {
            self.handle_exec_key(key);
            return;
        }
        if self.mode == Mode::Help {
            self.mode = self.return_mode;
            self.status = format!("returned to {}", mode_label(self.mode));
            return;
        }
        if self.mode == Mode::Error {
            self.mode = Mode::Browser;
            self.status = "returned to docker browser".to_string();
            return;
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.next_resource(),
            KeyCode::BackTab => self.previous_resource(),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => {
                self.selected = self.filtered_items().len().saturating_sub(1);
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.return_mode = self.mode;
                self.mode = Mode::Filter;
                self.status = "filter: type text, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('r') => self.refresh_current(),
            KeyCode::Char('p') => self.plan_container_action(OperationKind::StartContainer),
            KeyCode::Char('s') => self.plan_container_action(OperationKind::StopContainer),
            KeyCode::Char('R') => self.plan_container_action(OperationKind::RestartContainer),
            KeyCode::Char('x') | KeyCode::Delete => self.plan_remove(),
            KeyCode::Char('e') => self.plan_container_action(OperationKind::ExecShell),
            KeyCode::Char('l') => self.start_logs(),
            KeyCode::Char('t') => self.start_stats(),
            KeyCode::Char('c') => self.cancel_streams(),
            KeyCode::Char('a') => self.share_agent_context(),
            KeyCode::Char('y')
                if self.operation_request.is_some() && self.operation_plan.is_none() =>
            {
                self.stage_agent_operation()
            }
            KeyCode::Char('n')
                if self.operation_request.is_some() && self.operation_plan.is_none() =>
            {
                self.deny_agent_operation()
            }
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Esc => self.cancel_plan(),
            KeyCode::Char('?') => self.show_help(),
            _ => {
                self.status =
                    "docker: Tab resources, j/k move, i inspect, l logs, t stats, a share context, y/n review agent operation, p/s/R/x/e plan, q quit"
                        .to_string();
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = format!("filter applied: {}", self.filter_label());
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.selected = 0;
            }
            KeyCode::Char(ch) => {
                self.filter.push(ch);
                self.selected = 0;
            }
            _ => {}
        }
    }

    fn handle_exec_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char(']') {
            self.exec_escape_armed = true;
            self.status =
                "exec escape armed; press q to close or any other key to continue".to_string();
            return;
        }

        if self.exec_escape_armed {
            if key.code == KeyCode::Char('q') {
                if let Some(service) = &self.service {
                    service.send(DockerCommand::StopExec);
                }
                self.mode = Mode::Browser;
                self.exec_target = None;
                self.exec_escape_armed = false;
                self.status = "exec close requested".to_string();
                return;
            }
            self.exec_escape_armed = false;
        }

        let Some(bytes) = exec_key_bytes(key) else {
            return;
        };
        if let Some(service) = &self.service {
            service.send(DockerCommand::ExecInput { data: bytes });
        } else {
            self.exec_output
                .push_line("fixture exec input redacted".to_string());
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        let last = len as isize - 1;
        self.selected = (self.selected as isize + delta).clamp(0, last) as usize;
    }

    fn next_resource(&mut self) {
        self.active = match self.active {
            ResourceKind::Containers => ResourceKind::Images,
            ResourceKind::Images => ResourceKind::Networks,
            ResourceKind::Networks => ResourceKind::Volumes,
            ResourceKind::Volumes => ResourceKind::Containers,
        };
        self.selected = 0;
        self.status = format!("resource view: {}", self.active.label());
    }

    fn previous_resource(&mut self) {
        self.active = match self.active {
            ResourceKind::Containers => ResourceKind::Volumes,
            ResourceKind::Images => ResourceKind::Containers,
            ResourceKind::Networks => ResourceKind::Images,
            ResourceKind::Volumes => ResourceKind::Networks,
        };
        self.selected = 0;
        self.status = format!("resource view: {}", self.active.label());
    }

    fn refresh_all(&mut self) {
        if let Some(service) = &self.service {
            service.send(DockerCommand::ListContainers { all: true });
            service.send(DockerCommand::ListImages);
            service.send(DockerCommand::ListNetworks);
            service.send(DockerCommand::ListVolumes);
            self.status = "refreshing docker resources".to_string();
        } else {
            self.status = "fixture refresh: all docker resources".to_string();
        }
    }

    fn refresh_current(&mut self) {
        if let Some(service) = &self.service {
            match self.active {
                ResourceKind::Containers => {
                    service.send(DockerCommand::ListContainers { all: true })
                }
                ResourceKind::Images => service.send(DockerCommand::ListImages),
                ResourceKind::Networks => service.send(DockerCommand::ListNetworks),
                ResourceKind::Volumes => service.send(DockerCommand::ListVolumes),
            }
            self.status = format!("refreshing {}", self.active.label());
        } else {
            self.status = format!("fixture refresh: {}", self.active.label());
        }
    }

    fn inspect_selected(&mut self) {
        match self.selected_item() {
            Some(ResourceItem::Container(container)) => {
                if let Some(service) = &self.service {
                    service.send(DockerCommand::InspectContainer {
                        id: container.id.clone(),
                    });
                    self.status = format!("loading inspect summary for {}", container.name);
                } else {
                    self.status = format!("fixture inspect: {}", container.name);
                }
            }
            Some(item) => {
                self.status = format!("metadata preview: {}", item.safe_label());
            }
            None => self.status = "nothing selected".to_string(),
        }
    }

    fn start_logs(&mut self) {
        let Some(container) = self.selected_container() else {
            self.status = "select a container to follow logs".to_string();
            return;
        };
        if let Some(service) = &self.service {
            service.send(DockerCommand::StopLogs);
            service.send(DockerCommand::StartLogs {
                container_id: container.id.clone(),
                tail: 200,
            });
            self.logs.clear();
            self.active_log_container = Some(container.name.clone());
            self.status = format!("following logs for {}; cancel with c", container.name);
        } else {
            self.active_log_container = Some(container.name.clone());
            self.status = format!("fixture logs shown for {}", container.name);
        }
    }

    fn start_stats(&mut self) {
        let Some(container) = self.selected_container() else {
            self.status = "select a container to follow stats".to_string();
            return;
        };
        if let Some(service) = &self.service {
            service.send(DockerCommand::StopStats);
            service.send(DockerCommand::StartStats {
                container_id: container.id.clone(),
            });
            self.active_stats_container = Some(container.name.clone());
            self.status = format!("following stats for {}; cancel with c", container.name);
        } else {
            self.active_stats_container = Some(container.name.clone());
            self.status = format!("fixture stats shown for {}", container.name);
        }
    }

    fn cancel_streams(&mut self) {
        if let Some(service) = &self.service {
            service.send(DockerCommand::StopLogs);
            service.send(DockerCommand::StopStats);
        }
        self.active_log_container = None;
        self.active_stats_container = None;
        self.status = "stream cancellation requested".to_string();
    }

    fn share_agent_context(&mut self) {
        let policy = AssistContextPolicy::default();
        let snapshot = match self.build_context_share_snapshot(&policy) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.status = format!("context snapshot failed: {error}");
                return;
            }
        };
        self.context_share_sequence = self.context_share_sequence.saturating_add(1);
        let now = Utc::now();
        let share_id = format!(
            "context:docker:{}:{}",
            now.timestamp_millis(),
            self.context_share_sequence
        );
        let mut share = match AgentContextShare::new_context_share(
            share_id,
            format!("Docker {} current-view context", self.active.label()),
            snapshot.binding.clone(),
            ActorRef {
                id: "docker-tui".to_string(),
                actor_type: ActorType::Human,
            },
            None,
            policy,
            now,
            now + chrono::Duration::seconds(DEFAULT_ASSIST_REQUEST_TTL_SECONDS),
        ) {
            Ok(share) => share,
            Err(error) => {
                self.status = format!("context share failed: {error}");
                return;
            }
        };
        share.preview = Some(snapshot.preview());
        if let Err(error) = share.transition_to(AgentContextShareStatus::Pending) {
            self.status = format!("context share failed: {error}");
            return;
        }
        let plugin_state = AssistPluginState {
            mode: mode_label(self.mode).to_string(),
            health: PluginSessionHealth::Ready,
            status: safe_error_summary(&self.status),
            updated_at: now,
            metadata: self.context_share_metadata(),
            redaction: RedactionStatus::Applied,
        };
        match self.context_share_store.share_with_owner_lease(
            share.clone(),
            snapshot,
            Some(plugin_state),
        ) {
            Ok((record, owner_lease)) => {
                if let Some(previous_id) = self
                    .context_share
                    .as_ref()
                    .map(|previous| previous.id.clone())
                {
                    let _ = self.context_share_store.cancel(&previous_id);
                }
                self.context_share_owner_lease = Some(owner_lease);
                if self.operation_confirmation.take().is_some() {
                    self.operation_plan = None;
                }
                self.context_share = Some(record.request);
                self.operation_request = None;
                self.operation_request_seen_id = None;
                self.last_operation_sync = Instant::now() - OPERATION_SYNC_INTERVAL;
                self.status = format!("Docker context shared: {}", share.id);
            }
            Err(error) => {
                self.status = format!("context share write failed: {error}");
            }
        }
    }

    fn sync_agent_operation(&mut self) -> bool {
        if self.last_operation_sync.elapsed() < OPERATION_SYNC_INTERVAL {
            return false;
        }
        self.last_operation_sync = Instant::now();
        let Some(share_id) = self.context_share.as_ref().map(|share| share.id.clone()) else {
            return false;
        };
        let detail = match self.context_share_store.detail(&share_id) {
            Ok(detail) => detail,
            Err(_) => {
                if self.operation_request.is_some() {
                    self.operation_request = None;
                    self.status = "agent operation synchronization unavailable".to_string();
                    return true;
                }
                return false;
            }
        };
        self.context_share = Some(detail.record.request.clone());
        if detail.record.request.status.is_terminal() {
            let changed = self.operation_request.take().is_some()
                || self.operation_confirmation.take().is_some();
            if changed {
                self.operation_plan = None;
                self.status = "context share ended; pending agent operation cleared".to_string();
            }
            return changed;
        }
        let Some(operation) = detail.record.latest_pending_operation_request().cloned() else {
            if self.operation_request.take().is_some() {
                self.status = "agent operation decision synchronized".to_string();
                return true;
            }
            return false;
        };
        if operation.actions.len() != 1 {
            if self.operation_request_seen_id.as_deref() == Some(operation.id.as_str()) {
                return false;
            }
            self.operation_request_seen_id = Some(operation.id);
            self.operation_request = None;
            self.status = "agent operation rejected: non-PTY review requires exactly one operation"
                .to_string();
            return true;
        }
        if self.operation_request_seen_id.as_deref() == Some(operation.id.as_str()) {
            return false;
        }
        self.operation_request_seen_id = Some(operation.id.clone());
        self.status = format!(
            "agent operation ready for y/n review: {}",
            operation.summary
        );
        self.operation_request = Some(operation);
        true
    }

    fn stage_agent_operation(&mut self) {
        let Some(operation_request) = self.operation_request.clone() else {
            self.status = "no agent operation to stage".to_string();
            return;
        };
        let operation = match operation_request.actions.as_slice() {
            [operation] => operation,
            _ => {
                self.status =
                    "agent operation rejected: expected exactly one operation".to_string();
                return;
            }
        };
        if let Err(error) = self.stage_agent_operation_action(operation) {
            self.status = format!("agent operation rejected: {error}");
            return;
        }
        let confirmation = match self.operation_confirmation(
            "staged_for_plan_review",
            "staged existing Docker operation plan; service command still requires plan confirmation",
        ) {
            Ok(confirmation) => confirmation,
            Err(error) => {
                self.operation_plan = None;
                self.status = format!("operation review failed: {error}");
                return;
            }
        };
        let share_id = confirmation.request_id.clone();
        match self
            .context_share_store
            .confirm_operation(&share_id, confirmation.clone())
        {
            Ok(_) => {
                self.operation_confirmation = Some(confirmation);
                self.operation_request = None;
                self.status = "agent operation staged; review the Docker plan".to_string();
            }
            Err(error) => {
                self.operation_plan = None;
                self.status = format!("operation decision write failed: {error}");
            }
        }
    }

    fn deny_agent_operation(&mut self) {
        let confirmation = match self.operation_confirmation(
            "denied_by_user",
            "operator denied the requested operation before plan staging",
        ) {
            Ok(confirmation) => confirmation,
            Err(error) => {
                self.status = format!("operation denial failed: {error}");
                return;
            }
        };
        let share_id = confirmation.request_id.clone();
        match self
            .context_share_store
            .confirm_operation(&share_id, confirmation.clone())
        {
            Ok(_) => {
                self.operation_confirmation = Some(confirmation);
                self.operation_request = None;
                self.status = "agent operation denied; no Docker plan was staged".to_string();
            }
            Err(error) => {
                self.status = format!("operation denial write failed: {error}");
            }
        }
    }

    fn operation_confirmation(
        &self,
        status: &str,
        note: &str,
    ) -> Result<AgentOperationConfirmation> {
        let share = self
            .context_share
            .as_ref()
            .context("no active context share")?;
        let operation_request = self
            .operation_request
            .as_ref()
            .context("no agent operation request")?;
        let operation = match operation_request.actions.as_slice() {
            [operation] => operation,
            _ => {
                return Err(anyhow!(
                    "agent operation request must contain exactly one operation"
                ));
            }
        };
        Ok(AgentOperationConfirmation {
            request_id: share.id.clone(),
            response_id: operation_request.id.clone(),
            action_index: 0,
            target: operation_target_label(operation),
            uses_current_pty: false,
            generation: share.binding.generation,
            confirmed_at: Utc::now(),
            expires_at: Some(share.expires_at),
            command_summary: operation_summary(operation),
            capability_id: operation_capability_id(operation),
            status: status.to_string(),
            note: note.to_string(),
            redaction: RedactionStatus::Applied,
        })
    }

    fn stage_agent_operation_action(&mut self, action: &AgentOperation) -> Result<()> {
        let AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } = action
        else {
            return Err(anyhow!("agent operation is guidance only"));
        };
        if capability_id != "docker.container_action" {
            return Err(anyhow!(
                "unsupported Docker operation capability {capability_id}"
            ));
        }
        let action_name = input_summary
            .get("action")
            .and_then(Value::as_str)
            .context("Docker operation requires action")?;
        let kind = match action_name {
            "start" => OperationKind::StartContainer,
            "stop" => OperationKind::StopContainer,
            "restart" => OperationKind::RestartContainer,
            "remove" => OperationKind::RemoveContainer,
            _ => return Err(anyhow!("unsupported Docker operation {action_name}")),
        };
        let selected = self.selected_container();
        let target_id = input_summary
            .get("target_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| selected.as_ref().map(|container| container.id.clone()))
            .context("Docker operation requires target_id")?;
        let target_label = input_summary
            .get("target_label")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| selected.map(|container| container.name))
            .unwrap_or_else(|| target_id.clone());
        let confirmations_required = if matches!(
            kind,
            OperationKind::StopContainer
                | OperationKind::RestartContainer
                | OperationKind::RemoveContainer
        ) {
            2
        } else {
            1
        };
        let risk = if confirmations_required == 2 {
            "destructive"
        } else {
            "side_effecting"
        };
        self.operation_plan = Some(OperationPlanView {
            kind,
            target_id,
            target_label,
            risk,
            confirmations_required,
            confirmations: 0,
        });
        Ok(())
    }

    fn plan_container_action(&mut self, kind: OperationKind) {
        let Some(container) = self.selected_container() else {
            self.status = "select a container for that operation".to_string();
            return;
        };
        let confirmations_required = if matches!(
            kind,
            OperationKind::StopContainer
                | OperationKind::RestartContainer
                | OperationKind::RemoveContainer
        ) {
            2
        } else {
            1
        };
        let risk = if confirmations_required == 2 {
            "destructive"
        } else {
            "side_effecting"
        };
        let label = container.name.clone();
        self.operation_plan = Some(OperationPlanView {
            kind,
            target_id: container.id,
            target_label: label.clone(),
            risk,
            confirmations_required,
            confirmations: 0,
        });
        self.status = format!(
            "plan staged: {} on {}; press y{}",
            self.operation_plan
                .as_ref()
                .map(|plan| plan.kind.label())
                .unwrap_or("operation"),
            label,
            if confirmations_required == 2 {
                " twice"
            } else {
                ""
            }
        );
    }

    fn plan_remove(&mut self) {
        match self.selected_item() {
            Some(ResourceItem::Container(_)) => {
                self.plan_container_action(OperationKind::RemoveContainer);
            }
            Some(ResourceItem::Image(image)) => {
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::RemoveImage,
                    target_id: image.id.clone(),
                    target_label: image.display_tag(),
                    risk: "destructive",
                    confirmations_required: 2,
                    confirmations: 0,
                });
                self.status = format!(
                    "remove image plan staged: {}; press y twice",
                    image.display_tag()
                );
            }
            Some(ResourceItem::Network(_)) => {
                self.status = "network remove is not exposed by this service yet".to_string();
            }
            Some(ResourceItem::Volume(_)) => {
                self.status = "volume remove is not exposed by this service yet".to_string();
            }
            None => self.status = "nothing selected".to_string(),
        }
    }

    fn confirm_plan(&mut self) {
        if !self.agent_operation_plan_is_current() {
            return;
        }
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {} is {}; press y again to execute",
                plan.kind.label(),
                plan.target_label,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}",
                plan.kind.label(),
                plan.target_label
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(service) = &self.service {
            match plan.kind {
                OperationKind::StartContainer => service.send(DockerCommand::ContainerAction {
                    id: plan.target_id,
                    action: "start".to_string(),
                }),
                OperationKind::StopContainer => service.send(DockerCommand::ContainerAction {
                    id: plan.target_id,
                    action: "stop".to_string(),
                }),
                OperationKind::RestartContainer => service.send(DockerCommand::ContainerAction {
                    id: plan.target_id,
                    action: "restart".to_string(),
                }),
                OperationKind::RemoveContainer => service.send(DockerCommand::ContainerAction {
                    id: plan.target_id,
                    action: "remove".to_string(),
                }),
                OperationKind::ExecShell => {
                    self.exec_target = Some(plan.target_label.clone());
                    service.send(DockerCommand::StartExec {
                        container_id: plan.target_id,
                        cols: 100,
                        rows: 30,
                    });
                }
                OperationKind::RemoveImage => {
                    service.send(DockerCommand::RemoveImage { id: plan.target_id })
                }
            }
            self.status = "operation sent to DockerService".to_string();
        } else {
            self.status = format!(
                "fixture executed {} on {}",
                plan.kind.label(),
                plan.target_label
            );
        }
        self.operation_confirmation = None;
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.operation_confirmation = None;
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn agent_operation_plan_is_current(&mut self) -> bool {
        let Some(confirmation) = self.operation_confirmation.as_ref() else {
            return true;
        };
        let now = Utc::now();
        let current = self.context_share.as_ref().is_some_and(|share| {
            !share.status.is_terminal()
                && share.binding.generation == confirmation.generation
                && share.expires_at > now
                && confirmation
                    .expires_at
                    .is_none_or(|expires_at| expires_at > now)
        });
        if current {
            return true;
        }
        self.operation_plan = None;
        self.operation_confirmation = None;
        self.status = "agent operation plan expired or became stale; plan cleared".to_string();
        false
    }

    fn show_help(&mut self) {
        self.return_mode = self.mode;
        self.mode = Mode::Help;
        self.status = "help open".to_string();
    }

    fn filtered_items(&self) -> Vec<ResourceItem> {
        let filter = self.filter.to_lowercase();
        self.all_items()
            .into_iter()
            .filter(|item| filter.is_empty() || item.search_text().contains(&filter))
            .collect()
    }

    fn all_items(&self) -> Vec<ResourceItem> {
        match self.active {
            ResourceKind::Containers => self
                .containers
                .iter()
                .cloned()
                .map(ResourceItem::Container)
                .collect(),
            ResourceKind::Images => self
                .images
                .iter()
                .cloned()
                .map(ResourceItem::Image)
                .collect(),
            ResourceKind::Networks => self
                .networks
                .iter()
                .cloned()
                .map(ResourceItem::Network)
                .collect(),
            ResourceKind::Volumes => self
                .volumes
                .iter()
                .cloned()
                .map(ResourceItem::Volume)
                .collect(),
        }
    }

    fn selected_item(&self) -> Option<ResourceItem> {
        self.filtered_items().get(self.selected).cloned()
    }

    fn selected_container(&self) -> Option<ContainerView> {
        match self.selected_item() {
            Some(ResourceItem::Container(container)) => Some(container),
            _ => None,
        }
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }

    fn build_context_share_snapshot(
        &self,
        policy: &AssistContextPolicy,
    ) -> std::result::Result<AssistContextSnapshot, voidb_core::AssistContractError> {
        policy.validate()?;
        let descriptor = PluginSessionRegistration::new(
            "docker",
            format!("docker-tui:{}", self.profile_label),
            PluginSessionPurpose::InfrastructureClient,
            PluginSessionScope::LocalProcess,
        )
        .with_health(PluginSessionHealth::Ready)
        .with_authenticated(matches!(
            self.source,
            DockerTuiSource::Profile | DockerTuiSource::Connection
        ))
        .with_destructive_capable(!self.readonly)
        .with_stream_capable(true)
        .with_metadata(self.context_share_metadata(), RedactionStatus::Applied)
        .descriptor;
        let status_line = AssistBoundedText::capture(
            &safe_error_summary(&self.status),
            512,
            RedactionStatus::Applied,
        )?;
        let metadata_text =
            serde_json::to_string(&self.context_share_metadata()).map_err(|_| {
                voidb_core::AssistContractError::InvalidRequest(
                    "failed to encode Docker context-share metadata".to_string(),
                )
            })?;
        let transcript_tail = AssistBoundedText::capture(
            &metadata_text,
            policy.metadata_bytes,
            RedactionStatus::Applied,
        )?;
        Ok(AssistContextSnapshot {
            binding: AssistSessionBinding::from_descriptor(&descriptor),
            captured_at: Utc::now(),
            mode: format!("docker:{}", self.active.label()),
            health: PluginSessionHealth::Ready,
            terminal: None,
            visible_screen: None,
            transcript_tail: Some(transcript_tail),
            status_line: Some(status_line),
            withheld_fields: vec![
                AssistWithheldField {
                    field: "docker.client_handle".to_string(),
                    reason: AssistWithholdingReason::Policy,
                },
                AssistWithheldField {
                    field: "docker.profile_config".to_string(),
                    reason: AssistWithholdingReason::SecretMaterial,
                },
                AssistWithheldField {
                    field: "volumes.mountpoint".to_string(),
                    reason: AssistWithholdingReason::SensitiveMetadata,
                },
            ],
            metadata: self.context_share_metadata(),
            redaction: RedactionStatus::Applied,
        })
    }

    fn context_share_metadata(&self) -> Value {
        let selected = self.selected_item().map(|item| item.safe_label());
        json!({
            "profile_label": self.profile_label,
            "endpoint_label": self.endpoint_label,
            "connection_kind": self.connection_kind,
            "source": self.source,
            "readonly": self.readonly,
            "active_resource": self.active.label(),
            "selected": selected,
            "counts": {
                "containers": self.containers.len(),
                "images": self.images.len(),
                "networks": self.networks.len(),
                "volumes": self.volumes.len()
            },
            "streams": {
                "active_log_container": self.active_log_container.clone(),
                "active_stats_container": self.active_stats_container.clone(),
                "log_lines": self.logs.len(),
                "log_dropped_lines": self.logs.dropped_lines,
                "log_bytes": self.logs.bytes,
                "exec_lines": self.exec_output.len(),
                "exec_dropped_lines": self.exec_output.dropped_lines
            },
            "withheld": [
                "docker.client_handle",
                "docker.profile_config",
                "volumes.mountpoint",
                "raw_container_env"
            ]
        })
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(12),
                Constraint::Length(8),
                Constraint::Length(3),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
            .split(vertical[1]);
        frame.render_widget(self.resource_list(body[0]), body[0]);
        frame.render_widget(self.detail_panel(), body[1]);

        frame.render_widget(self.stream_panel(), vertical[2]);
        frame.render_widget(self.status_panel(), vertical[3]);

        match self.mode {
            Mode::Help => self.draw_help(frame, area),
            Mode::Error => self.draw_error(frame, area),
            Mode::Exec => self.draw_exec_banner(frame, area),
            Mode::Browser | Mode::Filter => {}
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let tabs = ResourceKind::ALL
            .iter()
            .map(|kind| {
                if *kind == self.active {
                    format!("[{}]", kind.label())
                } else {
                    kind.label().to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "Docker Operations",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} endpoint={}",
                    self.profile_label, self.endpoint_label
                )),
            ]),
            Line::from(format!(
                "{}  source={} kind={} purpose={} readonly={} restore={} filter={}",
                tabs,
                source,
                self.connection_kind,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn resource_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_items();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.selected, visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, item)| {
                let marker = if idx == self.selected { ">" } else { " " };
                let style = if idx == self.selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!("{marker} {}", item.list_label()),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if lines.is_empty() {
            vec![Line::from("No resources loaded. Press r to refresh.")]
        } else {
            lines
        })
        .block(
            Block::default()
                .title(format!(
                    " {} ({}/{}) ",
                    self.active.label(),
                    self.selected.saturating_add(1).min(items.len()),
                    items.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn detail_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.selected_item() {
            Some(ResourceItem::Container(container)) => vec![
                Line::from(format!("container: {}", container.name)),
                Line::from(format!("id: {}", container.id)),
                Line::from(format!("image: {}", container.image)),
                Line::from(format!(
                    "state: {}  status: {}",
                    container.state, container.status
                )),
                Line::from(format!("ports: {}", empty_dash(&container.ports))),
                Line::from(format!("created: {}", empty_dash(&container.created))),
            ],
            Some(ResourceItem::Image(image)) => vec![
                Line::from(format!("image: {}", image.display_tag())),
                Line::from(format!("id: {}", image.id)),
                Line::from(format!("size: {}", format_size(image.size))),
                Line::from(format!("created: {}", empty_dash(&image.created))),
            ],
            Some(ResourceItem::Network(network)) => vec![
                Line::from(format!("network: {}", network.name)),
                Line::from(format!("id: {}", network.id)),
                Line::from(format!("driver: {}", network.driver)),
                Line::from(format!("scope: {}", network.scope)),
            ],
            Some(ResourceItem::Volume(volume)) => vec![
                Line::from(format!("volume: {}", volume.name)),
                Line::from(format!("driver: {}", volume.driver)),
                Line::from(format!(
                    "mountpoint: {}",
                    redacted_mountpoint(&volume.mountpoint)
                )),
            ],
            None => vec![Line::from("Select a resource to inspect metadata.")],
        };

        if let Some(stats) = &self.stats {
            lines.push(Line::from(""));
            lines.push(Line::from(format!(
                "stats: cpu {:.1}%  mem {} / {} ({:.1}%)",
                stats.cpu_percent,
                format_size(stats.mem_usage),
                format_size(stats.mem_limit),
                stats.mem_percent
            )));
            lines.push(Line::from(format!(
                "network: rx {}  tx {}",
                format_size(stats.net_rx),
                format_size(stats.net_tx)
            )));
        }

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!("target: {}", plan.target_label)));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn stream_panel(&self) -> Paragraph<'_> {
        let title = if self.mode == Mode::Exec {
            format!(
                " Exec {} ",
                self.exec_target.as_deref().unwrap_or("session")
            )
        } else {
            format!(
                " Logs {} ",
                self.active_log_container.as_deref().unwrap_or("inactive")
            )
        };
        let source = if self.mode == Mode::Exec {
            &self.exec_output
        } else {
            &self.logs
        };
        let mut lines = source
            .tail(6)
            .into_iter()
            .map(Line::from)
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(Line::from(
                "No stream data. Press l for logs, t for stats, e then y for exec.",
            ));
        }
        if source.dropped_lines > 0 {
            lines.push(Line::from(Span::styled(
                format!(
                    "dropped {} lines due to stream bounds",
                    source.dropped_lines
                ),
                Style::default().fg(Color::Yellow),
            )));
        }
        Paragraph::new(lines)
            .block(Block::default().title(title).borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let text = vec![Line::from(format!(
            "{} | mode={} | logs {} lines/{} dropped | exec {} lines/{} dropped",
            self.primary_status(),
            mode_label(self.mode),
            self.logs.len(),
            self.logs.dropped_lines,
            self.exec_output.len(),
            self.exec_output.dropped_lines
        ))];
        let block = Block::default().title(" Status ").borders(Borders::ALL);
        Paragraph::new(text).block(block)
    }

    fn primary_status(&self) -> String {
        self.operation_request
            .as_ref()
            .map(|request| {
                format!(
                    "agent operation: {} [y stage, n deny]",
                    safe_error_summary(&request.summary)
                )
            })
            .unwrap_or_else(|| self.status.clone())
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(70, 56, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from("Docker operations TUI"),
            Line::from("Tab / Shift+Tab: switch resource type"),
            Line::from("j/k: move  /: filter  r: refresh  i: inspect"),
            Line::from("l: follow logs  t: follow stats  c: cancel streams"),
            Line::from("a: share bounded current-view context"),
            Line::from("pending agent operation: y stage plan  n deny"),
            Line::from("p: start  s: stop  R: restart  x: remove  e: exec shell"),
            Line::from("y: confirm plan  Esc: cancel plan  q: quit"),
            Line::from("Exec mode forwards keys only after explicit confirmation."),
            Line::from("Exec escape: Ctrl+] then q"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_error(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(70, 40, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Docker target error",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
                Line::from(self.status.clone()),
                Line::from("Press any key to return to the browser."),
            ])
            .block(Block::default().title(" Error ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_exec_banner(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(58, 18, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Exec input is active",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!(
                    "target: {}",
                    self.exec_target.as_deref().unwrap_or("docker container")
                )),
                Line::from("stdin is forwarded to the container."),
                Line::from("Escape: Ctrl+] then q"),
            ])
            .block(Block::default().title(" Exec ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
            popup,
        );
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_text(&mut self, text: &str) {
        for line in text.lines() {
            self.push_line(line.to_string());
        }
        if text.ends_with('\n') && text.lines().next().is_none() {
            self.push_line(String::new());
        }
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn tail(&self, count: usize) -> Vec<String> {
        self.lines
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

impl ResourceKind {
    const ALL: [ResourceKind; 4] = [
        ResourceKind::Containers,
        ResourceKind::Images,
        ResourceKind::Networks,
        ResourceKind::Volumes,
    ];

    fn label(self) -> &'static str {
        match self {
            ResourceKind::Containers => "containers",
            ResourceKind::Images => "images",
            ResourceKind::Networks => "networks",
            ResourceKind::Volumes => "volumes",
        }
    }
}

impl ResourceItem {
    fn list_label(&self) -> String {
        match self {
            ResourceItem::Container(container) => format!(
                "{:<12} {:<18} {:<10} {:<24} {}",
                container.id, container.name, container.state, container.status, container.image
            ),
            ResourceItem::Image(image) => format!(
                "{:<12} {:>10} {:<16} {}",
                image.id,
                format_size(image.size),
                empty_dash(&image.created),
                image.display_tag()
            ),
            ResourceItem::Network(network) => format!(
                "{:<12} {:<20} {:<10} {}",
                network.id, network.name, network.driver, network.scope
            ),
            ResourceItem::Volume(volume) => format!(
                "{:<24} {:<10} {}",
                volume.name,
                volume.driver,
                redacted_mountpoint(&volume.mountpoint)
            ),
        }
    }

    fn safe_label(&self) -> String {
        match self {
            ResourceItem::Container(container) => container.name.clone(),
            ResourceItem::Image(image) => image.display_tag(),
            ResourceItem::Network(network) => network.name.clone(),
            ResourceItem::Volume(volume) => volume.name.clone(),
        }
    }

    fn search_text(&self) -> String {
        match self {
            ResourceItem::Container(container) => format!(
                "{} {} {} {} {}",
                container.id, container.name, container.image, container.state, container.status
            ),
            ResourceItem::Image(image) => format!("{} {}", image.id, image.tags.join(" ")),
            ResourceItem::Network(network) => {
                format!(
                    "{} {} {} {}",
                    network.id, network.name, network.driver, network.scope
                )
            }
            ResourceItem::Volume(volume) => {
                format!("{} {} {}", volume.name, volume.driver, volume.mountpoint)
            }
        }
        .to_lowercase()
    }
}

impl ImageView {
    fn display_tag(&self) -> String {
        self.tags
            .first()
            .cloned()
            .unwrap_or_else(|| "<none>".to_string())
    }
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::StartContainer => "start container",
            OperationKind::StopContainer => "stop container",
            OperationKind::RestartContainer => "restart container",
            OperationKind::RemoveContainer => "remove container",
            OperationKind::ExecShell => "exec shell",
            OperationKind::RemoveImage => "remove image",
        }
    }
}

fn exec_key_bytes(key: KeyEvent) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(ch) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let lower = ch.to_ascii_lowercase();
            if lower.is_ascii_lowercase() {
                Some(vec![(lower as u8) - b'a' + 1])
            } else {
                None
            }
        }
        KeyCode::Char(ch) => Some(ch.to_string().into_bytes()),
        KeyCode::Enter => Some(vec![b'\n']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::Esc => Some(vec![0x1b]),
        _ => None,
    }
}

fn connection_value(config: &DockerConfig) -> Value {
    match &config.connection {
        DockerConnection::Local => json!({
            "kind": "local",
            "endpoint_label": "local docker daemon",
            "timeout_seconds": config.timeout,
            "secret_material": "redacted"
        }),
        DockerConnection::Socket { path: _ } => json!({
            "kind": "socket",
            "endpoint_label": "custom unix socket",
            "timeout_seconds": config.timeout,
            "socket_path": "redacted",
            "secret_material": "redacted"
        }),
        DockerConnection::Http { url } => json!({
            "kind": "http",
            "endpoint_label": redacted_url(url),
            "timeout_seconds": config.timeout,
            "secret_material": "redacted"
        }),
        DockerConnection::Tls {
            url,
            ca_cert: _,
            cert: _,
            key: _,
        } => json!({
            "kind": "tls",
            "endpoint_label": redacted_url(url),
            "timeout_seconds": config.timeout,
            "ca_cert": "redacted",
            "client_cert": "redacted",
            "client_key": "redacted",
            "secret_material": "redacted"
        }),
    }
}

fn connection_labels(config: &DockerConfig) -> (String, String) {
    match &config.connection {
        DockerConnection::Local => ("local".to_string(), "local docker daemon".to_string()),
        DockerConnection::Socket { path: _ } => {
            ("socket".to_string(), "custom unix socket".to_string())
        }
        DockerConnection::Http { url } => ("http".to_string(), redacted_url(url)),
        DockerConnection::Tls { url, .. } => ("tls".to_string(), redacted_url(url)),
    }
}

fn redacted_url(url: &str) -> String {
    let without_fragment = url.split('#').next().unwrap_or(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    if let Some((scheme, rest)) = without_query.split_once("://")
        && let Some((_, host_path)) = rest.rsplit_once('@')
    {
        return format!("{scheme}://redacted@{host_path}");
    }
    without_query.to_string()
}

fn redacted_mountpoint(path: &str) -> String {
    if path.is_empty() {
        "-".to_string()
    } else {
        "redacted-host-path".to_string()
    }
}

fn container_sort(left: &ContainerView, right: &ContainerView) -> std::cmp::Ordering {
    container_rank(&left.state)
        .cmp(&container_rank(&right.state))
        .then_with(|| left.name.cmp(&right.name))
        .then_with(|| left.id.cmp(&right.id))
}

fn container_rank(state: &str) -> u8 {
    match state {
        "running" => 0,
        "created" | "restarting" => 1,
        "paused" => 2,
        "exited" | "dead" => 3,
        _ => 4,
    }
}

fn image_sort(left: &ImageView, right: &ImageView) -> std::cmp::Ordering {
    left.display_tag()
        .cmp(&right.display_tag())
        .then_with(|| left.id.cmp(&right.id))
}

fn empty_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "registry_token_value",
        "DOCKER_AUTH_CONFIG",
        "BEGIN PRIVATE KEY",
        "raw_plugin_config",
        "client_key_material",
        "super-secret-docker-token",
        "container_env_secret",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn docker_context_share_store() -> Result<AgentContextShareStore> {
    AgentContextShareStore::new(
        docker_agent_context_store_root()?,
        AgentContextSharePolicy::non_pty(),
    )
}

pub fn docker_agent_context_store_root() -> Result<std::path::PathBuf> {
    if let Some(path) = std::env::var_os(DOCKER_AGENT_CONTEXT_STORE_DIR_ENV)
        .or_else(|| std::env::var_os(LEGACY_DOCKER_ASSIST_STORE_DIR_ENV))
    {
        Ok(std::path::PathBuf::from(path))
    } else {
        Ok(AppConfig::config_dir()
            .map_err(|error| anyhow!(error.to_string()))?
            .join("docker-assist"))
    }
}

fn operation_target_label(action: &AgentOperation) -> String {
    match action {
        AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } => {
            let target = input_summary
                .get("target_label")
                .and_then(Value::as_str)
                .unwrap_or("docker-target");
            format!("capability:{capability_id}:{target}")
        }
        AgentOperation::Guidance { title, .. } => format!("guidance:{title}"),
        AgentOperation::RequestPermission { permission, .. } => {
            format!("permission:{permission:?}")
        }
        AgentOperation::ProposedCommand { target, .. } => format!("command:{target:?}"),
    }
}

fn operation_summary(action: &AgentOperation) -> Option<String> {
    match action {
        AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } => Some(format!(
            "{capability_id} {}",
            safe_error_summary(&input_summary.to_string())
        )),
        AgentOperation::ProposedCommand { command, .. } => Some(safe_error_summary(command)),
        _ => None,
    }
}

fn operation_capability_id(action: &AgentOperation) -> Option<String> {
    match action {
        AgentOperation::CapabilityCall { capability_id, .. } => Some(capability_id.clone()),
        AgentOperation::ProposedCommand {
            target: AgentOperationTarget::Capability { capability_id },
            ..
        } => Some(capability_id.clone()),
        _ => None,
    }
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
        Mode::Help => "help",
        Mode::Error => "error",
        Mode::Exec => "exec",
    }
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

struct StandaloneDockerTabManager {
    render_tx: mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneDockerTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow!("Docker TUI does not host plugin tabs; use plugin-owned CLI commands instead")
    }
}

impl TabManager for StandaloneDockerTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "Docker".to_string(),
            plugin_id: "docker".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow!("Docker TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow!("Docker TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_launch(readonly: bool) -> DockerTuiLaunch {
        DockerTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: DockerTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/docker_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    #[test]
    fn preflight_redacts_tls_material() {
        let launch = DockerTuiLaunch {
            profile_label: "tls-docker".to_string(),
            config: Some(DockerConfig {
                connection: DockerConnection::Tls {
                    url: "https://user:super-secret-docker-token@docker.example.test:2376?token=registry_token_value".to_string(),
                    ca_cert: "ca".to_string(),
                    cert: "cert".to_string(),
                    key: "BEGIN PRIVATE KEY client_key_material".to_string(),
                },
                timeout: 15,
            }),
            source: DockerTuiSource::Connection,
            fixture_path: None,
            purpose: "operations".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("registry_token_value"));
        assert!(!rendered.contains("super-secret-docker-token"));
        assert!(!rendered.contains("BEGIN PRIVATE KEY"));
        assert!(!rendered.contains("client_key_material"));
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_docker_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("docker_tui_fixture_evidence"));
        assert!(rendered.contains("destructive_double_confirmation"));
        assert!(rendered.contains("current_pty_rejected"));
        assert!(!rendered.contains("assist_handoff"));
        assert!(!rendered.contains("response_review"));
        assert_eq!(
            evidence["external_agent_interaction"]["broker_policy"],
            json!("non_pty")
        );
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
    }

    #[test]
    fn filter_matches_container_fields() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.filter = "api".to_string();
        let items = app.filtered_items();
        assert_eq!(items.len(), 1);
        assert!(items[0].safe_label().contains("api"));
    }

    #[test]
    fn destructive_plan_requires_second_confirmation() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.plan_container_action(OperationKind::StopContainer);
        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("press y again"));
        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed stop container"));
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = DockerTuiApp::new(fixture_launch(true)).unwrap();
        app.plan_container_action(OperationKind::StopContainer);
        app.confirm_plan();
        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
    }

    #[test]
    fn external_agent_operation_stages_existing_docker_confirmation_plan() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_container().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, docker_operation_request(&request_id, &target))
            .unwrap();

        app.sync_agent_operation();
        assert!(app.status.contains("agent operation ready"));
        assert!(app.primary_status().contains("[y stage, n deny]"));
        app.stage_agent_operation();
        assert!(app.operation_plan.is_some());
        assert!(app.operation_confirmation.is_some());
        let detail = app.context_share_store.detail(&request_id).unwrap();
        assert_eq!(detail.record.action_confirmations.len(), 1);
        assert!(!detail.record.action_confirmations[0].uses_current_pty);

        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed stop container"));
    }

    #[test]
    fn external_agent_operation_can_be_denied_without_staging() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_container().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, docker_operation_request(&request_id, &target))
            .unwrap();

        app.sync_agent_operation();
        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));

        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("denied"));
        let detail = app.context_share_store.detail(&request_id).unwrap();
        assert_eq!(
            detail.record.action_confirmations[0].status,
            "denied_by_user"
        );
    }

    #[test]
    fn external_multi_action_request_is_never_partially_staged() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store =
            temp_context_share_store_with_policy(AgentContextSharePolicy::current_pty_capable());
        app.share_agent_context();
        let request_id = app
            .context_share
            .as_ref()
            .unwrap_or_else(|| panic!("{}", app.status))
            .id
            .clone();
        let target = app.selected_container().unwrap();
        let mut request = docker_operation_request(&request_id, &target);
        request.actions.push(request.actions[0].clone());
        app.context_share_store
            .post_operation_request(&request_id, request)
            .unwrap();

        assert!(app.sync_agent_operation());
        assert!(app.operation_request.is_none());
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("exactly one operation"));
        assert!(
            app.context_share_store
                .detail(&request_id)
                .unwrap()
                .record
                .action_confirmations
                .is_empty()
        );
    }

    #[test]
    fn expired_agent_plan_is_cleared_before_service_dispatch() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_container().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, docker_operation_request(&request_id, &target))
            .unwrap();
        app.sync_agent_operation();
        app.stage_agent_operation();
        app.context_share.as_mut().unwrap().expires_at = Utc::now() - chrono::Duration::seconds(1);

        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.operation_confirmation.is_none());
        assert!(app.status.contains("expired or became stale"));
    }

    #[test]
    fn polling_clears_an_operation_decided_by_another_process() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_container().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, docker_operation_request(&request_id, &target))
            .unwrap();
        app.sync_agent_operation();
        let denial = app
            .operation_confirmation("denied_by_agent", "external task was cancelled")
            .unwrap();
        app.context_share_store
            .confirm_operation(&request_id, denial)
            .unwrap();
        app.last_operation_sync = Instant::now() - OPERATION_SYNC_INTERVAL;

        assert!(app.sync_agent_operation());
        assert!(app.operation_request.is_none());
        assert!(app.status.contains("decision synchronized"));
    }

    #[test]
    fn uppercase_a_is_not_an_operation_shortcut() {
        let mut app = DockerTuiApp::new(fixture_launch(false)).unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE));
        assert!(app.status.contains("a share context"));
        assert!(!app.status.contains("assist"));
        assert!(!app.status.contains("poll"));
    }

    fn temp_context_share_store() -> AgentContextShareStore {
        temp_context_share_store_with_policy(AgentContextSharePolicy::non_pty())
    }

    fn temp_context_share_store_with_policy(
        policy: AgentContextSharePolicy,
    ) -> AgentContextShareStore {
        AgentContextShareStore::new(
            std::env::temp_dir().join(format!(
                "voidb-docker-agent-context-test-{}-{:?}",
                Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                std::thread::current().id()
            )),
            policy,
        )
        .unwrap()
    }

    fn docker_operation_request(request_id: &str, target: &ContainerView) -> AgentOperationRequest {
        AgentOperationRequest {
            id: format!(
                "agent:operation:docker:{}",
                Utc::now().timestamp_nanos_opt().unwrap_or_default()
            ),
            request_id: request_id.to_string(),
            agent: AgentPrincipal {
                client_id: "agent".to_string(),
                task_id: "tw-90".to_string(),
                instance_id: Some("docker-agent-operation-test".to_string()),
            },
            created_at: Utc::now(),
            summary: "Stop the selected container after operator review.".to_string(),
            diagnosis: Some("Fixture container is the target for lifecycle review.".to_string()),
            actions: vec![AgentOperation::CapabilityCall {
                capability_id: "docker.container_action".to_string(),
                input_summary: json!({
                    "action": "stop",
                    "target_id": target.id.clone(),
                    "target_label": target.name.clone()
                }),
                rationale: "Use the existing Docker lifecycle plan and confirmation gate."
                    .to_string(),
                risk: AgentOperationRisk::Destructive,
                target: AgentOperationTarget::Capability {
                    capability_id: "docker.container_action".to_string(),
                },
            }],
            requested_permissions: vec![AssistPermission::ProposeCommands],
            redaction: RedactionStatus::Applied,
        }
    }
}
