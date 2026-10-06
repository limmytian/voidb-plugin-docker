//! Bounded Docker live workflows exposed through the generic agent broker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use voidb_core::{
    AGENT_LIVE_SESSION_PROTOCOL_VERSION, AgentLiveSessionAuditIdentity,
    AgentLiveSessionBufferOverflow, AgentLiveSessionBufferPolicy, AgentLiveSessionCallCancellation,
    AgentLiveSessionCancelBehavior, AgentLiveSessionCloseEffect, AgentLiveSessionContract,
    AgentLiveSessionControlPolicy, AgentLiveSessionCursor, AgentLiveSessionCursorKind,
    AgentLiveSessionEventBuffer, AgentLiveSessionEventKind, AgentLiveSessionKind,
    AgentLiveSessionOperations, AgentLiveSessionReadRequest, AgentLiveSessionReconnectMode,
    AgentLiveSessionReconnectPolicy, AgentLiveSessionResourceDescriptor,
    AgentLiveSessionResumeMode, AgentLiveSessionStartRequest, AgentSessionCallRequest,
    AgentSessionCallResult, AgentSessionOpenContext, CapabilityRiskLevel, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus, RedactionTarget, collect_redaction_targets,
    redact_text_with_targets,
};

use crate::config::DockerConfig;
use crate::service::agent_live::{
    DockerAgentService, DockerDaemonEvent, DockerStatsSnapshot, DockerTerminalControl,
    DockerTerminalOutput,
};

pub(crate) const LOGS_FOLLOW_CAPABILITY: &str = "docker.logs_follow";
pub(crate) const STATS_FOLLOW_CAPABILITY: &str = "docker.stats_follow";
pub(crate) const EVENTS_FOLLOW_CAPABILITY: &str = "docker.events_follow";
pub(crate) const EXEC_READ_CAPABILITY: &str = "docker.exec_read";
pub(crate) const EXEC_INPUT_CAPABILITY: &str = "docker.exec_input";
pub(crate) const EXEC_RESIZE_CAPABILITY: &str = "docker.exec_resize";
pub(crate) const EXEC_SIGNAL_CAPABILITY: &str = "docker.exec_signal";
pub(crate) const ATTACH_READ_CAPABILITY: &str = "docker.attach_read";
pub(crate) const ATTACH_INPUT_CAPABILITY: &str = "docker.attach_input";
pub(crate) const ATTACH_RESIZE_CAPABILITY: &str = "docker.attach_resize";

const EXEC_CAPABILITIES: &[&str] = &[
    EXEC_READ_CAPABILITY,
    EXEC_INPUT_CAPABILITY,
    EXEC_RESIZE_CAPABILITY,
    EXEC_SIGNAL_CAPABILITY,
];
const ATTACH_CAPABILITIES: &[&str] = &[
    ATTACH_READ_CAPABILITY,
    ATTACH_INPUT_CAPABILITY,
    ATTACH_RESIZE_CAPABILITY,
];
const DEFAULT_LOG_TAIL: usize = 100;
const MAX_LOG_TAIL: usize = 5_000;
const DEFAULT_COLS: u16 = 120;
const DEFAULT_ROWS: u16 = 40;
const MIN_COLS: u16 = 20;
const MAX_COLS: u16 = 500;
const MIN_ROWS: u16 = 5;
const MAX_ROWS: u16 = 200;
const MAX_TERMINAL_WRITE_BYTES: usize = 16 * 1024;
const MAX_TERMINAL_EVENT_TEXT_BYTES: usize = 48 * 1024;

pub struct DockerAgentSessionFactory {
    config: DockerConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl DockerAgentSessionFactory {
    pub fn new(config: DockerConfig) -> Self {
        let mut redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        if let crate::config::DockerConnection::Tls {
            ca_cert, cert, key, ..
        } = &config.connection
        {
            // These legacy field names are intentionally too generic for the
            // shared classifier. Give their TLS meaning explicitly so daemon
            // output cannot echo profile material into an agent-visible event.
            redaction_targets.extend(collect_redaction_targets(&json!({
                "client_certificate_authority": ca_cert,
                "client_certificate": cert,
                "private_key": key,
            })));
        }
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for DockerAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "docker"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        let family = DockerLiveFamily::from_binding(&context)?;
        let representative = family.capabilities()[0];
        let (purpose, contract) =
            docker_live_session_contract(representative).expect("Docker live family contract");
        if context.binding.purpose != purpose {
            return Err(session_error(
                PluginSessionErrorCode::BindingMismatch,
                "The Docker live-session purpose does not match its capability family.",
            ));
        }
        contract.validate(&context.binding.allowed_capabilities)?;
        contract.validate_start(&context.request.input)?;
        let start: AgentLiveSessionStartRequest =
            serde_json::from_value(context.request.input.clone()).map_err(|_| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Docker live-session start envelope is invalid.",
                )
            })?;
        let policy = start
            .buffer
            .clone()
            .unwrap_or_else(|| contract.buffer.clone());
        let buffer = AgentLiveSessionEventBuffer::new(contract.clone(), policy)?;
        let service = Arc::new(
            DockerAgentService::connect(&self.config)
                .await
                .map_err(|_| {
                    session_error(
                        PluginSessionErrorCode::OwnerUnavailable,
                        "The Docker daemon could not be reached or authenticated.",
                    )
                })?,
        );

        let mut terminal = None;
        let producer_buffer = buffer.clone();
        let targets = Arc::clone(&self.redaction_targets);
        let task = match family {
            DockerLiveFamily::Logs => {
                let source = Arc::clone(&service);
                tokio::spawn(async move {
                    run_logs(source, start, producer_buffer.clone(), targets).await;
                    producer_buffer.close_source().await;
                })
            }
            DockerLiveFamily::Stats => {
                let source = Arc::clone(&service);
                tokio::spawn(async move {
                    run_stats(source, start, producer_buffer.clone()).await;
                    producer_buffer.close_source().await;
                })
            }
            DockerLiveFamily::Events => {
                let source = Arc::clone(&service);
                tokio::spawn(async move {
                    run_events(source, start, producer_buffer.clone()).await;
                    producer_buffer.close_source().await;
                })
            }
            DockerLiveFamily::Exec => {
                let terminal_parts = open_exec(&service, &start).await?;
                terminal = Some(Arc::clone(&terminal_parts.control));
                tokio::spawn(async move {
                    run_terminal_output(terminal_parts.output, producer_buffer.clone(), targets)
                        .await;
                    producer_buffer.close_source().await;
                })
            }
            DockerLiveFamily::Attach => {
                let terminal_parts = open_attach(&service, &start).await?;
                terminal = Some(Arc::clone(&terminal_parts.control));
                tokio::spawn(async move {
                    run_terminal_output(terminal_parts.output, producer_buffer.clone(), targets)
                        .await;
                    producer_buffer.close_source().await;
                })
            }
        };

        Ok(Arc::new(DockerAgentLiveSession {
            family,
            buffer,
            terminal,
            cancellations: AgentLiveSessionCallCancellation::default(),
            task: Mutex::new(Some(task)),
            closed: AtomicBool::new(false),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DockerLiveFamily {
    Logs,
    Stats,
    Events,
    Exec,
    Attach,
}

impl DockerLiveFamily {
    fn from_binding(context: &AgentSessionOpenContext) -> Result<Self, PluginSessionError> {
        let actual = context
            .binding
            .allowed_capabilities
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for family in [
            Self::Logs,
            Self::Stats,
            Self::Events,
            Self::Exec,
            Self::Attach,
        ] {
            let expected = family
                .capabilities()
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if actual == expected {
                return Ok(family);
            }
        }
        Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "A Docker live session requires one complete, unmixed capability family.",
        ))
    }

    fn capabilities(self) -> &'static [&'static str] {
        match self {
            Self::Logs => &[LOGS_FOLLOW_CAPABILITY],
            Self::Stats => &[STATS_FOLLOW_CAPABILITY],
            Self::Events => &[EVENTS_FOLLOW_CAPABILITY],
            Self::Exec => EXEC_CAPABILITIES,
            Self::Attach => ATTACH_CAPABILITIES,
        }
    }

    fn read_capability(self) -> &'static str {
        self.capabilities()[0]
    }
}

struct DockerAgentLiveSession {
    family: DockerLiveFamily,
    buffer: AgentLiveSessionEventBuffer,
    terminal: Option<Arc<DockerTerminalControl>>,
    cancellations: AgentLiveSessionCallCancellation,
    task: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

#[async_trait]
impl PluginAgentSession for DockerAgentLiveSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if !self
            .family
            .capabilities()
            .contains(&request.capability.as_str())
        {
            return Err(session_error(
                PluginSessionErrorCode::PolicyDenied,
                "The Docker call is outside this live-session binding.",
            ));
        }
        match request.capability.as_str() {
            capability if capability == self.family.read_capability() => {
                self.read_events(request).await
            }
            EXEC_INPUT_CAPABILITY | ATTACH_INPUT_CAPABILITY => self.write_terminal(request).await,
            EXEC_RESIZE_CAPABILITY | ATTACH_RESIZE_CAPABILITY => {
                self.resize_terminal(request).await
            }
            EXEC_SIGNAL_CAPABILITY => self.signal_terminal(request).await,
            _ => Err(session_error(
                PluginSessionErrorCode::Unsupported,
                "The Docker live-session operation is unsupported.",
            )),
        }
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.closed.load(Ordering::Acquire) {
            PluginSessionHealth::Closed
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, call_id: &str) -> Result<(), PluginSessionError> {
        self.cancellations.cancel(call_id).await;
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancellations.close().await;
        let terminal_result = if let Some(terminal) = &self.terminal {
            terminal.close().await.map_err(|_| {
                session_error(
                    PluginSessionErrorCode::OwnerUnavailable,
                    "Docker terminal cleanup could not be verified.",
                )
            })
        } else {
            Ok(())
        };
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        self.buffer.close_source().await;
        terminal_result
    }
}

impl DockerAgentLiveSession {
    async fn read_events(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let mut read = if request.input.is_null() {
            AgentLiveSessionReadRequest::default()
        } else {
            serde_json::from_value(request.input.clone()).map_err(|_| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Docker live-session read request is invalid.",
                )
            })?
        };
        read.max_bytes = read.max_bytes.min(request.output_limit_bytes);
        let call_id = request.call_id.clone();
        let batch = self
            .cancellations
            .run(&call_id, self.buffer.read(&read))
            .await?;
        let output = serde_json::to_value(batch).map_err(|_| {
            session_error(
                PluginSessionErrorCode::RedactionFailed,
                "The Docker live-session batch could not be serialized.",
            )
        })?;
        AgentSessionCallResult::bounded(call_id, output, request.output_limit_bytes)
    }

    async fn write_terminal(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::Unsupported,
                "This Docker session has no terminal input.",
            )
        })?;
        let bytes = terminal_input(&request.input)?;
        let written = bytes.len();
        terminal.write(&bytes).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "Docker terminal input failed.",
            )
        })?;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "written_bytes": written }),
            request.output_limit_bytes,
        )
    }

    async fn resize_terminal(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::Unsupported,
                "This Docker session has no terminal resize control.",
            )
        })?;
        let cols = terminal_dimension(&request.input, "cols", None)?;
        let rows = terminal_dimension(&request.input, "rows", None)?;
        terminal.resize(cols, rows).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "Docker terminal resize failed.",
            )
        })?;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "cols": cols, "rows": rows }),
            request.output_limit_bytes,
        )
    }

    async fn signal_terminal(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::Unsupported,
                "This Docker session has no process signal control.",
            )
        })?;
        let signal = request
            .input
            .get("signal")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                session_error(PluginSessionErrorCode::PolicyDenied, "signal is required.")
            })?;
        terminal.signal_exec(signal).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "Docker exec signal failed.",
            )
        })?;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "signal": signal.to_ascii_uppercase() }),
            request.output_limit_bytes,
        )
    }
}

pub(crate) fn docker_live_session_contract(
    capability: &str,
) -> Option<(PluginSessionPurpose, AgentLiveSessionContract)> {
    let family = match capability {
        LOGS_FOLLOW_CAPABILITY => DockerLiveFamily::Logs,
        STATS_FOLLOW_CAPABILITY => DockerLiveFamily::Stats,
        EVENTS_FOLLOW_CAPABILITY => DockerLiveFamily::Events,
        EXEC_READ_CAPABILITY
        | EXEC_INPUT_CAPABILITY
        | EXEC_RESIZE_CAPABILITY
        | EXEC_SIGNAL_CAPABILITY => DockerLiveFamily::Exec,
        ATTACH_READ_CAPABILITY | ATTACH_INPUT_CAPABILITY | ATTACH_RESIZE_CAPABILITY => {
            DockerLiveFamily::Attach
        }
        _ => return None,
    };
    let (
        purpose,
        kind,
        resource_type,
        identity_schema,
        identity_fields,
        parameters,
        buffer,
        reconnect,
        close,
        start_risk,
    ) = match family {
        DockerLiveFamily::Logs => (
            PluginSessionPurpose::LogStream,
            AgentLiveSessionKind::Log,
            "docker_container",
            container_identity_schema(),
            vec!["/container_id".into()],
            json!({
                "type": "object",
                "properties": {
                    "tail": { "type": "integer", "minimum": 1, "maximum": MAX_LOG_TAIL, "default": DEFAULT_LOG_TAIL },
                    "timestamps": { "type": "boolean", "default": true }
                },
                "additionalProperties": false
            }),
            observation_buffer(AgentLiveSessionBufferOverflow::DropOldest),
            timestamp_reconnect_policy(),
            AgentLiveSessionCloseEffect::StopObservation,
            CapabilityRiskLevel::ReadOnly,
        ),
        DockerLiveFamily::Stats => (
            PluginSessionPurpose::WatchStream,
            AgentLiveSessionKind::Metrics,
            "docker_container",
            container_identity_schema(),
            vec!["/container_id".into()],
            empty_parameters_schema(),
            AgentLiveSessionBufferPolicy {
                max_events: 64,
                max_bytes: 512 * 1024,
                overflow: AgentLiveSessionBufferOverflow::Coalesce,
            },
            restart_reconnect_policy(),
            AgentLiveSessionCloseEffect::StopObservation,
            CapabilityRiskLevel::ReadOnly,
        ),
        DockerLiveFamily::Events => (
            PluginSessionPurpose::WatchStream,
            AgentLiveSessionKind::Events,
            "docker_daemon",
            json!({
                "type": "object",
                "required": ["scope"],
                "properties": { "scope": { "const": "daemon" } },
                "additionalProperties": false
            }),
            vec!["/scope".into()],
            json!({
                "type": "object",
                "properties": {
                    "filters": {
                        "type": "object",
                        "maxProperties": 16,
                        "additionalProperties": {
                            "type": "array",
                            "maxItems": 64,
                            "items": { "type": "string", "minLength": 1, "maxLength": 256 }
                        }
                    }
                },
                "additionalProperties": false
            }),
            observation_buffer(AgentLiveSessionBufferOverflow::DropOldest),
            timestamp_reconnect_policy(),
            AgentLiveSessionCloseEffect::StopObservation,
            CapabilityRiskLevel::ReadOnly,
        ),
        DockerLiveFamily::Exec => (
            PluginSessionPurpose::InteractiveTerminal,
            AgentLiveSessionKind::Exec,
            "docker_container",
            container_identity_schema(),
            vec!["/container_id".into()],
            terminal_parameters_schema(true),
            terminal_buffer(),
            AgentLiveSessionReconnectPolicy::default(),
            AgentLiveSessionCloseEffect::TerminateRemote,
            CapabilityRiskLevel::ExternalSideEffect,
        ),
        DockerLiveFamily::Attach => (
            PluginSessionPurpose::InteractiveTerminal,
            AgentLiveSessionKind::Attach,
            "docker_container",
            container_identity_schema(),
            vec!["/container_id".into()],
            terminal_parameters_schema(false),
            terminal_buffer(),
            AgentLiveSessionReconnectPolicy::default(),
            AgentLiveSessionCloseEffect::DetachRemote,
            CapabilityRiskLevel::ExternalSideEffect,
        ),
    };
    let capabilities = family.capabilities();
    Some((
        purpose,
        AgentLiveSessionContract {
            protocol_version: AGENT_LIVE_SESSION_PROTOCOL_VERSION,
            kind,
            resource: AgentLiveSessionResourceDescriptor {
                resource_type: resource_type.into(),
                identity_schema,
                identity_fields,
                audit_identity: AgentLiveSessionAuditIdentity::Fingerprint,
            },
            start_parameters_schema: parameters,
            event_schema: json!({ "type": "object", "maxProperties": 16 }),
            operations: AgentLiveSessionOperations {
                events: capabilities[0].into(),
                input: capabilities.get(1).map(|capability| (*capability).into()),
                resize: capabilities.get(2).map(|capability| (*capability).into()),
                signal: capabilities.get(3).map(|capability| (*capability).into()),
            },
            buffer,
            reconnect,
            delivery: Default::default(),
            control: AgentLiveSessionControlPolicy {
                cancel: AgentLiveSessionCancelBehavior::CallOnly,
                close,
            },
            start_risk,
        },
    ))
}

fn container_identity_schema() -> Value {
    json!({
        "type": "object",
        "required": ["container_id"],
        "properties": {
            "container_id": { "type": "string", "minLength": 1, "maxLength": 256 }
        },
        "additionalProperties": false
    })
}

fn empty_parameters_schema() -> Value {
    json!({ "type": "object", "additionalProperties": false })
}

fn terminal_parameters_schema(exec: bool) -> Value {
    let mut properties = serde_json::Map::from_iter([
        ("tty".into(), json!({ "type": "boolean", "default": true })),
        (
            "cols".into(),
            json!({ "type": "integer", "minimum": MIN_COLS, "maximum": MAX_COLS, "default": DEFAULT_COLS }),
        ),
        (
            "rows".into(),
            json!({ "type": "integer", "minimum": MIN_ROWS, "maximum": MAX_ROWS, "default": DEFAULT_ROWS }),
        ),
    ]);
    let required = if exec {
        properties.insert(
            "command".into(),
            json!({
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": { "type": "string", "minLength": 1, "maxLength": 4096 }
            }),
        );
        properties.insert(
            "user".into(),
            json!({ "type": "string", "minLength": 1, "maxLength": 256 }),
        );
        properties.insert(
            "working_dir".into(),
            json!({ "type": "string", "minLength": 1, "maxLength": 4096 }),
        );
        vec!["command"]
    } else {
        properties.insert(
            "logs".into(),
            json!({ "type": "boolean", "default": false }),
        );
        Vec::new()
    };
    json!({
        "type": "object",
        "required": required,
        "properties": properties,
        "additionalProperties": false
    })
}

fn observation_buffer(overflow: AgentLiveSessionBufferOverflow) -> AgentLiveSessionBufferPolicy {
    AgentLiveSessionBufferPolicy {
        max_events: 2_000,
        max_bytes: 2 * 1024 * 1024,
        overflow,
    }
}

fn terminal_buffer() -> AgentLiveSessionBufferPolicy {
    AgentLiveSessionBufferPolicy {
        max_events: 1_000,
        max_bytes: 1024 * 1024,
        overflow: AgentLiveSessionBufferOverflow::DropOldest,
    }
}

fn timestamp_reconnect_policy() -> AgentLiveSessionReconnectPolicy {
    AgentLiveSessionReconnectPolicy {
        mode: AgentLiveSessionReconnectMode::Transient,
        max_attempts: 8,
        initial_backoff_ms: 250,
        max_backoff_ms: 10_000,
        resume: AgentLiveSessionResumeMode::BestEffortCursor,
        cursor_kind: Some(AgentLiveSessionCursorKind::Timestamp),
    }
}

fn restart_reconnect_policy() -> AgentLiveSessionReconnectPolicy {
    AgentLiveSessionReconnectPolicy {
        mode: AgentLiveSessionReconnectMode::Transient,
        max_attempts: 8,
        initial_backoff_ms: 250,
        max_backoff_ms: 10_000,
        resume: AgentLiveSessionResumeMode::Restart,
        cursor_kind: None,
    }
}

async fn open_exec(
    service: &DockerAgentService,
    start: &AgentLiveSessionStartRequest,
) -> Result<crate::service::agent_live::DockerTerminalParts, PluginSessionError> {
    let container = required_string(&start.resource, "container_id")?;
    let command = required_string_array(&start.parameters, "command")?;
    let tty = optional_bool(&start.parameters, "tty", true)?;
    let cols = terminal_dimension(&start.parameters, "cols", Some(DEFAULT_COLS))?;
    let rows = terminal_dimension(&start.parameters, "rows", Some(DEFAULT_ROWS))?;
    let user = optional_string(&start.parameters, "user")?;
    let working_dir = optional_string(&start.parameters, "working_dir")?;
    service
        .open_exec(crate::service::agent_live::DockerExecSpec {
            container_id: container,
            command,
            tty,
            user,
            working_dir,
            cols,
            rows,
        })
        .await
        .map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "The controlled Docker exec session could not be started.",
            )
        })
}

async fn open_attach(
    service: &DockerAgentService,
    start: &AgentLiveSessionStartRequest,
) -> Result<crate::service::agent_live::DockerTerminalParts, PluginSessionError> {
    let container = required_string(&start.resource, "container_id")?;
    let tty = optional_bool(&start.parameters, "tty", true)?;
    let logs = optional_bool(&start.parameters, "logs", false)?;
    let cols = terminal_dimension(&start.parameters, "cols", Some(DEFAULT_COLS))?;
    let rows = terminal_dimension(&start.parameters, "rows", Some(DEFAULT_ROWS))?;
    service
        .open_attach(&container, logs, tty, cols, rows)
        .await
        .map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "The controlled Docker attach session could not be started.",
            )
        })
}

async fn run_logs(
    service: Arc<DockerAgentService>,
    start: AgentLiveSessionStartRequest,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    let Ok(container) = required_string(&start.resource, "container_id") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let tail = start
        .parameters
        .get("tail")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(DEFAULT_LOG_TAIL)
        .min(MAX_LOG_TAIL);
    let timestamps = start
        .parameters
        .get("timestamps")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let mut since = start
        .resume_from
        .as_ref()
        .and_then(|cursor| cursor.value.parse::<i64>().ok());
    let mut initial = true;
    loop {
        let mut stream = service.logs(
            &container,
            if initial { tail } else { 0 },
            since,
            timestamps,
        );
        initial = false;
        let mut retry = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    let observed = chrono::Utc::now();
                    let cursor_value = observed.timestamp().to_string();
                    for text in split_text(&chunk.text, MAX_TERMINAL_EVENT_TEXT_BYTES) {
                        let source_bytes = text.len();
                        let (text, redaction) = redact_text_with_targets(&text, &redaction_targets);
                        if buffer
                            .push(
                                observed,
                                AgentLiveSessionEventKind::Data,
                                json!({
                                    "stream": chunk.stream,
                                    "text": text,
                                    "source_bytes": source_bytes
                                }),
                                Some(AgentLiveSessionCursor {
                                    kind: AgentLiveSessionCursorKind::Timestamp,
                                    value: cursor_value.clone(),
                                    scope: None,
                                }),
                                redaction,
                                false,
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    since = Some(observed.timestamp());
                }
                Err(error) => {
                    retry = Some(error);
                    break;
                }
            }
        }
        drop(stream);
        if let Some(error) = retry {
            if !retry_after_error(&buffer, &error).await {
                return;
            }
        } else {
            push_terminal_end(&buffer, "source_closed").await;
            return;
        }
    }
}

async fn run_stats(
    service: Arc<DockerAgentService>,
    start: AgentLiveSessionStartRequest,
    buffer: AgentLiveSessionEventBuffer,
) {
    let Ok(container) = required_string(&start.resource, "container_id") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    loop {
        let mut stream = service.stats(&container);
        let mut retry = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(snapshot) => {
                    if push_stats(&buffer, snapshot).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    retry = Some(error);
                    break;
                }
            }
        }
        drop(stream);
        if let Some(error) = retry {
            if !retry_after_error(&buffer, &error).await {
                return;
            }
        } else {
            push_terminal_end(&buffer, "source_closed").await;
            return;
        }
    }
}

async fn push_stats(
    buffer: &AgentLiveSessionEventBuffer,
    snapshot: DockerStatsSnapshot,
) -> Result<(), PluginSessionError> {
    buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::Data,
            json!({
                "cpu_percent": snapshot.cpu_percent,
                "memory_usage": snapshot.memory_usage,
                "memory_limit": snapshot.memory_limit,
                "memory_percent": snapshot.memory_percent,
                "network_rx": snapshot.network_rx,
                "network_tx": snapshot.network_tx
            }),
            None,
            RedactionStatus::NotRequired,
            false,
        )
        .await
        .map(|_| ())
}

async fn run_events(
    service: Arc<DockerAgentService>,
    start: AgentLiveSessionStartRequest,
    buffer: AgentLiveSessionEventBuffer,
) {
    let filters = parse_filters(&start.parameters).unwrap_or_default();
    let mut since = start
        .resume_from
        .as_ref()
        .map(|cursor| cursor.value.clone());
    loop {
        let mut stream = service.events(since.clone(), filters.clone());
        let mut retry = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    since = event_timestamp_cursor(&event).or(since);
                    let cursor = since.clone().map(|value| AgentLiveSessionCursor {
                        kind: AgentLiveSessionCursorKind::Timestamp,
                        value,
                        scope: None,
                    });
                    if buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::Data,
                            json!({
                                "type": event.event_type,
                                "action": event.action,
                                "actor_id": event.actor_id,
                                "attribute_keys": event.attribute_keys,
                                "scope": event.scope,
                                "time": event.time,
                                "time_nano": event.time_nano
                            }),
                            cursor,
                            RedactionStatus::NotRequired,
                            false,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    retry = Some(error);
                    break;
                }
            }
        }
        drop(stream);
        if let Some(error) = retry {
            if !retry_after_error(&buffer, &error).await {
                return;
            }
        } else {
            push_terminal_end(&buffer, "source_closed").await;
            return;
        }
    }
}

async fn run_terminal_output(
    mut output: DockerTerminalOutput,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    while let Some(item) = output.next().await {
        match item {
            Ok(chunk) => {
                let text = String::from_utf8_lossy(&chunk.bytes);
                for text in split_text(&text, MAX_TERMINAL_EVENT_TEXT_BYTES) {
                    let source_bytes = text.len();
                    let (text, redaction) = redact_text_with_targets(&text, &redaction_targets);
                    if buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::Data,
                            json!({
                                "stream": chunk.stream,
                                "text": text,
                                "source_bytes": source_bytes
                            }),
                            None,
                            redaction,
                            false,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                push_terminal_error(&buffer, target_error_class(&error)).await;
                return;
            }
        }
    }
    push_terminal_end(&buffer, "process_exited").await;
}

async fn retry_after_error(buffer: &AgentLiveSessionEventBuffer, error: &anyhow::Error) -> bool {
    let class = target_error_class(error);
    if matches!(class, "authentication" | "authorization" | "not_found") {
        push_terminal_error(buffer, class).await;
        return false;
    }
    let attempt = match buffer.record_reconnect().await {
        Ok(attempt) => attempt,
        Err(_) => {
            push_terminal_error(buffer, "retry_exhausted").await;
            return false;
        }
    };
    if buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::Warning,
            json!({ "state": "reconnecting", "error_class": class, "attempt": attempt }),
            None,
            RedactionStatus::NotRequired,
            false,
        )
        .await
        .is_err()
    {
        return false;
    }
    let backoff = 250u64
        .saturating_mul(1u64 << attempt.saturating_sub(1).min(5))
        .min(10_000);
    tokio::time::sleep(Duration::from_millis(backoff)).await;
    true
}

async fn push_terminal_error(buffer: &AgentLiveSessionEventBuffer, class: &str) {
    let _ = buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::Error,
            json!({ "state": "failed", "error_class": class }),
            None,
            RedactionStatus::NotRequired,
            true,
        )
        .await;
}

async fn push_terminal_end(buffer: &AgentLiveSessionEventBuffer, state: &str) {
    let _ = buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::End,
            json!({ "state": state }),
            None,
            RedactionStatus::NotRequired,
            true,
        )
        .await;
}

fn target_error_class(error: &anyhow::Error) -> &'static str {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("401") || message.contains("unauthorized") {
        "authentication"
    } else if message.contains("403")
        || message.contains("forbidden")
        || message.contains("permission denied")
    {
        "authorization"
    } else if message.contains("404") || message.contains("not found") {
        "not_found"
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout"
    } else {
        "transport"
    }
}

fn event_timestamp_cursor(event: &DockerDaemonEvent) -> Option<String> {
    event
        .time_nano
        .filter(|value| *value > 0)
        .map(|value| format!("{}.{:09}", value / 1_000_000_000, value % 1_000_000_000))
        .or_else(|| {
            event
                .time
                .filter(|value| *value > 0)
                .map(|value| value.to_string())
        })
}

fn parse_filters(parameters: &Value) -> Result<HashMap<String, Vec<String>>, PluginSessionError> {
    parameters
        .get("filters")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| {
            session_error(
                PluginSessionErrorCode::PolicyDenied,
                "Docker event filters must map names to string arrays.",
            )
        })
        .map(Option::unwrap_or_default)
}

fn terminal_input(input: &Value) -> Result<Vec<u8>, PluginSessionError> {
    let text = input.get("text").and_then(Value::as_str).unwrap_or("");
    if text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "Docker terminal text contains unsupported control characters.",
        ));
    }
    let enter = input.get("enter").and_then(Value::as_bool).unwrap_or(false);
    let mut bytes = text.as_bytes().to_vec();
    if enter {
        bytes.push(b'\r');
    }
    if bytes.is_empty() || bytes.len() > MAX_TERMINAL_WRITE_BYTES {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Docker terminal input must contain 1 to {MAX_TERMINAL_WRITE_BYTES} bytes."),
        ));
    }
    Ok(bytes)
}

fn required_string(value: &Value, key: &str) -> Result<String, PluginSessionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::PolicyDenied,
                format!("Docker live-session field '{key}' is required."),
            )
        })
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>, PluginSessionError> {
    value
        .get(key)
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    session_error(
                        PluginSessionErrorCode::PolicyDenied,
                        format!("Docker live-session field '{key}' must be a non-empty string."),
                    )
                })
        })
        .transpose()
}

fn required_string_array(value: &Value, key: &str) -> Result<Vec<String>, PluginSessionError> {
    let values = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Docker live-session field '{key}' must be a string array."),
        )
    })?;
    if values.is_empty() || values.len() > 64 {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "Docker exec command must contain 1 to 64 arguments.",
        ));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 4096)
                .map(str::to_string)
                .ok_or_else(|| {
                    session_error(
                        PluginSessionErrorCode::PolicyDenied,
                        "Docker exec command arguments must be non-empty bounded strings.",
                    )
                })
        })
        .collect()
}

fn optional_bool(value: &Value, key: &str, default: bool) -> Result<bool, PluginSessionError> {
    value
        .get(key)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    format!("Docker live-session field '{key}' must be a boolean."),
                )
            })
        })
        .unwrap_or(Ok(default))
}

fn terminal_dimension(
    value: &Value,
    key: &str,
    default: Option<u16>,
) -> Result<u16, PluginSessionError> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .or(default.map(u64::from));
    let raw = raw.ok_or_else(|| {
        session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Docker terminal field '{key}' is required."),
        )
    })?;
    let (minimum, maximum) = if key == "cols" {
        (MIN_COLS, MAX_COLS)
    } else {
        (MIN_ROWS, MAX_ROWS)
    };
    if raw < u64::from(minimum) || raw > u64::from(maximum) {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Docker terminal field '{key}' must be between {minimum} and {maximum}."),
        ));
    }
    Ok(raw as u16)
}

fn split_text(text: &str, max_bytes: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if !current.is_empty() && current.len().saturating_add(character.len_utf8()) > max_bytes {
            chunks.push(std::mem::take(&mut current));
        }
        current.push(character);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn session_error(code: PluginSessionErrorCode, message: impl Into<String>) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_contracts_bind_complete_families_and_risk() {
        for capability in [
            LOGS_FOLLOW_CAPABILITY,
            STATS_FOLLOW_CAPABILITY,
            EVENTS_FOLLOW_CAPABILITY,
            EXEC_READ_CAPABILITY,
            ATTACH_READ_CAPABILITY,
        ] {
            let (_, contract) = docker_live_session_contract(capability).expect("live contract");
            let family = match capability {
                EXEC_READ_CAPABILITY => EXEC_CAPABILITIES,
                ATTACH_READ_CAPABILITY => ATTACH_CAPABILITIES,
                _ => &[capability],
            };
            contract
                .validate(
                    &family
                        .iter()
                        .map(|value| (*value).into())
                        .collect::<Vec<_>>(),
                )
                .expect("valid contract");
        }
        let (_, exec) = docker_live_session_contract(EXEC_READ_CAPABILITY).unwrap();
        assert_eq!(exec.start_risk, CapabilityRiskLevel::ExternalSideEffect);
        assert_eq!(
            exec.control.close,
            AgentLiveSessionCloseEffect::TerminateRemote
        );
        let (_, logs) = docker_live_session_contract(LOGS_FOLLOW_CAPABILITY).unwrap();
        assert_eq!(logs.start_risk, CapabilityRiskLevel::ReadOnly);
    }

    #[test]
    fn terminal_input_is_bounded_and_rejects_raw_control_bytes() {
        assert_eq!(
            terminal_input(&json!({ "text": "echo ok", "enter": true })).unwrap(),
            b"echo ok\r"
        );
        assert!(terminal_input(&json!({ "text": "secret\u{0000}" })).is_err());
        assert!(terminal_input(&json!({ "text": "" })).is_err());
    }

    #[test]
    fn event_cursor_prefers_nanosecond_precision() {
        let event = DockerDaemonEvent {
            event_type: None,
            action: None,
            actor_id: None,
            attribute_keys: Vec::new(),
            scope: None,
            time: Some(12),
            time_nano: Some(12_345_000_006),
        };
        assert_eq!(
            event_timestamp_cursor(&event).as_deref(),
            Some("12.345000006")
        );
    }

    #[test]
    fn factory_redaction_targets_withhold_profile_secrets() {
        let secret = "docker-conformance-private-key";
        let factory = DockerAgentSessionFactory::new(DockerConfig {
            connection: crate::config::DockerConnection::Tls {
                url: "https://docker.example.test".into(),
                ca_cert: "fixture-ca".into(),
                cert: "fixture-cert".into(),
                key: secret.into(),
            },
            timeout: 1,
        });
        let (redacted, status) = redact_text_with_targets(
            &format!("target returned {secret}"),
            factory.redaction_targets.as_ref(),
        );
        assert_eq!(status, RedactionStatus::Applied);
        assert!(!redacted.contains(secret));
    }
}
