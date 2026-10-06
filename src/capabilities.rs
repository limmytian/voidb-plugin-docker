#![allow(clippy::result_large_err)]

use serde_json::{Value, json};
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, Pagination, RedactionStatus, TargetSystemFailure,
};

use crate::agent_session::{
    ATTACH_INPUT_CAPABILITY, ATTACH_READ_CAPABILITY, ATTACH_RESIZE_CAPABILITY,
    EVENTS_FOLLOW_CAPABILITY, EXEC_INPUT_CAPABILITY, EXEC_READ_CAPABILITY, EXEC_RESIZE_CAPABILITY,
    EXEC_SIGNAL_CAPABILITY, LOGS_FOLLOW_CAPABILITY, STATS_FOLLOW_CAPABILITY,
    docker_live_session_contract,
};
use crate::config::{DockerConfig, DockerConnection};
use crate::docker_ops;
use crate::types::{ContainerInfo, ImageInfo, NetworkInfo, VolumeInfo};

const PLUGIN_ID: &str = "docker";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 500;
const DEFAULT_LOG_TAIL: usize = 100;
const MAX_LOG_TAIL: usize = 5_000;
const DEFAULT_TEXT_LIMIT_BYTES: usize = 64 * 1024;
const MAX_TEXT_LIMIT_BYTES: usize = 1024 * 1024;

pub fn docker_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "diagnostics",
            "Return agent-safe Docker profile diagnostics without opening a daemon connection.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": ["connection_type", "timeout_secs", "network_checked"],
                "properties": {
                    "connection_type": { "type": "string" },
                    "timeout_secs": { "type": "integer", "minimum": 0 },
                    "network_checked": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "docker.diagnostics"],
            false,
            false,
            false,
        ),
        capability(
            "list_containers",
            "List Docker containers with bounded, cursor-based output.",
            json!({
                "type": "object",
                "properties": {
                    "all": { "type": "boolean", "default": true }
                },
                "additionalProperties": false
            }),
            docker_list_schema("containers", container_schema()),
            vec!["connection.read", "docker.containers.list"],
            false,
            false,
            false,
        ),
        capability(
            "list_images",
            "List Docker images with bounded, cursor-based output.",
            empty_input_schema(),
            docker_list_schema("images", image_schema()),
            vec!["connection.read", "docker.images.list"],
            false,
            false,
            false,
        ),
        capability(
            "list_networks",
            "List Docker networks with bounded, cursor-based output.",
            empty_input_schema(),
            docker_list_schema("networks", network_schema()),
            vec!["connection.read", "docker.networks.list"],
            false,
            false,
            false,
        ),
        capability(
            "list_volumes",
            "List Docker volumes with bounded, cursor-based output.",
            empty_input_schema(),
            docker_list_schema("volumes", volume_schema()),
            vec!["connection.read", "docker.volumes.list"],
            false,
            false,
            false,
        ),
        capability(
            "inspect_container",
            "Inspect a Docker container and return a redacted summary.",
            json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["container", "raw_omitted"],
                "properties": {
                    "container": {
                        "type": "object",
                        "required": [
                            "id",
                            "name",
                            "image",
                            "status",
                            "running",
                            "exit_code",
                            "env_count",
                            "mount_count",
                            "network_count"
                        ],
                        "properties": {
                            "id": { "type": "string" },
                            "name": { "type": "string" },
                            "image": { "type": ["string", "null"] },
                            "status": { "type": ["string", "null"] },
                            "running": { "type": ["boolean", "null"] },
                            "exit_code": { "type": ["integer", "null"] },
                            "env_count": { "type": "integer", "minimum": 0 },
                            "mount_count": { "type": "integer", "minimum": 0 },
                            "network_count": { "type": "integer", "minimum": 0 }
                        },
                        "additionalProperties": false
                    },
                    "raw_omitted": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "docker.containers.inspect"],
            false,
            false,
            false,
        ),
        capability(
            "logs",
            "Read bounded Docker container logs without following the stream.",
            json!({
                "type": "object",
                "required": ["container_id"],
                "properties": {
                    "container_id": { "type": "string", "minLength": 1 },
                    "tail": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LOG_TAIL,
                        "default": DEFAULT_LOG_TAIL
                    },
                    "max_bytes": text_limit_schema()
                },
                "additionalProperties": false
            }),
            text_output_schema("container_id"),
            vec!["connection.read", "docker.containers.logs"],
            false,
            false,
            false,
        ),
        live_capability(
            LOGS_FOLLOW_CAPABILITY,
            "Follow Docker container logs with bounded retention and best-effort timestamp resume.",
            vec!["connection.read", "docker.containers.logs"],
        ),
        live_capability(
            STATS_FOLLOW_CAPABILITY,
            "Follow coalesced Docker container resource statistics.",
            vec!["connection.read", "docker.containers.stats"],
        ),
        live_capability(
            EVENTS_FOLLOW_CAPABILITY,
            "Follow filtered Docker daemon events without exposing attribute values.",
            vec!["connection.read", "docker.events"],
        ),
        live_capability(
            EXEC_READ_CAPABILITY,
            "Read output from one explicitly configured Docker exec process.",
            vec!["connection.read", "docker.containers.exec"],
        ),
        live_capability(
            EXEC_INPUT_CAPABILITY,
            "Write bounded text to one controlled Docker exec process.",
            vec!["connection.write", "docker.containers.exec"],
        ),
        live_capability(
            EXEC_RESIZE_CAPABILITY,
            "Resize one controlled Docker exec TTY.",
            vec!["connection.write", "docker.containers.exec"],
        ),
        live_capability(
            EXEC_SIGNAL_CAPABILITY,
            "Signal one controlled Docker exec process by its daemon-reported PID.",
            vec!["connection.write", "docker.containers.exec"],
        ),
        live_capability(
            ATTACH_READ_CAPABILITY,
            "Read bounded output from an explicit Docker container attach.",
            vec!["connection.read", "docker.containers.attach"],
        ),
        live_capability(
            ATTACH_INPUT_CAPABILITY,
            "Write bounded text to an explicit Docker container attach.",
            vec!["connection.write", "docker.containers.attach"],
        ),
        live_capability(
            ATTACH_RESIZE_CAPABILITY,
            "Resize an explicit Docker container attach TTY.",
            vec!["connection.write", "docker.containers.attach"],
        ),
        capability(
            "container_action",
            "Start, stop, restart, or remove one Docker container.",
            json!({
                "type": "object",
                "required": ["id", "action"],
                "properties": {
                    "id": { "type": "string", "minLength": 1 },
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "restart", "remove"]
                    }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "docker.containers.lifecycle"],
            true,
            false,
            true,
        ),
    ]
}

pub async fn invoke_docker_capability(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match Docker.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "diagnostics" => Ok(diagnostics_result(config, invocation.id)),
        "list_containers" => invoke_list_containers(config, invocation).await,
        "list_images" => invoke_list_images(config, invocation).await,
        "list_networks" => invoke_list_networks(config, invocation).await,
        "list_volumes" => invoke_list_volumes(config, invocation).await,
        "inspect_container" => invoke_inspect_container(config, invocation).await,
        "logs" => invoke_logs(config, invocation).await,
        "logs_follow" | "stats_follow" | "events_follow" | "exec_read" | "exec_input"
        | "exec_resize" | "exec_signal" | "attach_read" | "attach_input" | "attach_resize" => {
            Err(unavailable_error(
                "unavailable.session_required",
                "This Docker live workflow requires a persistent agent session.",
                json!({ "capability_id": invocation.capability_id }),
            ))
        }
        "container_action" => invoke_container_action(config, invocation).await,
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "Docker capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn live_capability(
    qualified_id: &str,
    description: &str,
    permissions: Vec<&str>,
) -> CapabilityDefinition {
    let id = qualified_id
        .strip_prefix("docker.")
        .expect("Docker live capability ID");
    let (purpose, contract) =
        docker_live_session_contract(qualified_id).expect("Docker live contract");
    let handoff_capabilities = contract
        .operations
        .capabilities()
        .cloned()
        .collect::<Vec<_>>();
    let risk = match qualified_id {
        EXEC_SIGNAL_CAPABILITY => CapabilityRiskLevel::Destructive,
        EXEC_INPUT_CAPABILITY
        | EXEC_RESIZE_CAPABILITY
        | ATTACH_INPUT_CAPABILITY
        | ATTACH_RESIZE_CAPABILITY => CapabilityRiskLevel::ExternalSideEffect,
        _ => CapabilityRiskLevel::ReadOnly,
    };
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema: docker_live_call_schema(qualified_id),
        output_schema: docker_live_output_schema(qualified_id),
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: docker_live_authorization(qualified_id, purpose.clone()),
        risk,
        destructive: risk == CapabilityRiskLevel::Destructive,
        streaming: qualified_id.ends_with("_read") || qualified_id.ends_with("_follow"),
        execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
        session_handoff: Some(
            voidb_core::CapabilitySessionHandoff::new(purpose, handoff_capabilities)
                .with_live_session(contract),
        ),
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run: false,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn docker_live_authorization(
    capability: &str,
    purpose: voidb_core::PluginSessionPurpose,
) -> voidb_core::CapabilityAuthorizationMetadata {
    let mut fields = Vec::new();
    if capability != EVENTS_FOLLOW_CAPABILITY {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/resource/container_id",
                "Container",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
        );
    }
    if matches!(
        capability,
        EXEC_READ_CAPABILITY
            | EXEC_INPUT_CAPABILITY
            | EXEC_RESIZE_CAPABILITY
            | EXEC_SIGNAL_CAPABILITY
    ) {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/parameters/command",
                "Exec command",
                voidb_core::CapabilityApprovalValueType::CommandArgv,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        );
    }
    if capability == EVENTS_FOLLOW_CAPABILITY {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/parameters/filters",
                "Event filters",
                voidb_core::CapabilityApprovalValueType::Json,
            )
            .with_constraint(voidb_core::CapabilityConstraintKind::Subset),
        );
    }
    let mut metadata = voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![purpose])
        .with_note(
            "Docker container, command, and daemon filter scope are revalidated at session open.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields));
    if matches!(
        capability,
        EXEC_READ_CAPABILITY
            | EXEC_INPUT_CAPABILITY
            | EXEC_RESIZE_CAPABILITY
            | EXEC_SIGNAL_CAPABILITY
            | ATTACH_READ_CAPABILITY
            | ATTACH_INPUT_CAPABILITY
            | ATTACH_RESIZE_CAPABILITY
    ) {
        metadata = metadata.with_interactive_execute();
    }
    metadata
}

fn docker_live_call_schema(capability: &str) -> Value {
    match capability {
        EXEC_INPUT_CAPABILITY | ATTACH_INPUT_CAPABILITY => json!({
            "type": "object",
            "properties": {
                "text": { "type": "string", "maxLength": 16384 },
                "enter": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
        EXEC_RESIZE_CAPABILITY | ATTACH_RESIZE_CAPABILITY => json!({
            "type": "object",
            "required": ["cols", "rows"],
            "properties": {
                "cols": { "type": "integer", "minimum": 20, "maximum": 500 },
                "rows": { "type": "integer", "minimum": 5, "maximum": 200 }
            },
            "additionalProperties": false
        }),
        EXEC_SIGNAL_CAPABILITY => json!({
            "type": "object",
            "required": ["signal"],
            "properties": { "signal": { "enum": ["INT", "TERM", "KILL"] } },
            "additionalProperties": false
        }),
        _ => live_read_schema(),
    }
}

fn docker_live_output_schema(capability: &str) -> Value {
    match capability {
        EXEC_INPUT_CAPABILITY | ATTACH_INPUT_CAPABILITY => json!({
            "type": "object",
            "required": ["written_bytes"],
            "properties": { "written_bytes": { "type": "integer", "minimum": 1 } },
            "additionalProperties": false
        }),
        EXEC_RESIZE_CAPABILITY | ATTACH_RESIZE_CAPABILITY => json!({
            "type": "object",
            "required": ["cols", "rows"],
            "properties": {
                "cols": { "type": "integer" },
                "rows": { "type": "integer" }
            },
            "additionalProperties": false
        }),
        EXEC_SIGNAL_CAPABILITY => json!({
            "type": "object",
            "required": ["signal"],
            "properties": { "signal": { "type": "string" } },
            "additionalProperties": false
        }),
        _ => live_batch_schema(),
    }
}

fn live_read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after_sequence": { "type": "integer", "minimum": 0 },
            "max_events": { "type": "integer", "minimum": 1, "maximum": 1000 },
            "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 }
        },
        "additionalProperties": false
    })
}

fn live_batch_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "protocol_version",
            "events",
            "next_sequence",
            "source_closed",
            "dropped_events",
            "dropped_bytes",
            "coalesced_events",
            "reconnect_attempts"
        ],
        "properties": {
            "protocol_version": { "type": "integer", "const": 1 },
            "events": { "type": "array", "maxItems": 1000 },
            "next_sequence": { "type": "integer", "minimum": 1 },
            "resume_cursor": { "type": "object" },
            "source_closed": { "type": "boolean" },
            "dropped_events": { "type": "integer", "minimum": 0 },
            "dropped_bytes": { "type": "integer", "minimum": 0 },
            "coalesced_events": { "type": "integer", "minimum": 0 },
            "reconnect_attempts": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    streaming: bool,
    supports_dry_run: bool,
) -> CapabilityDefinition {
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: docker_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming,
        execution_mode: voidb_core::CapabilityExecutionMode::Stateless,
        session_handoff: None,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn docker_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let schema = match id {
        "inspect_container" => Some(vec![
            voidb_core::CapabilityApprovalField::new(
                "/id",
                "Container",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
        ]),
        "logs" => Some(vec![
            voidb_core::CapabilityApprovalField::new(
                "/container_id",
                "Container",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
        ]),
        "container_action" => Some(vec![
            voidb_core::CapabilityApprovalField::new(
                "/id",
                "Container",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/action",
                "Container action",
                voidb_core::CapabilityApprovalValueType::String,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        ]),
        _ => None,
    };
    let metadata = voidb_core::CapabilityAuthorizationMetadata::declared();
    match schema {
        Some(fields) => metadata
            .with_note(
                "Container identity and action are revalidated; exec/attach remain deferred.",
            )
            .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields)),
        None => metadata,
    }
}

fn diagnostics_result(config: &DockerConfig, invocation_id: String) -> CapabilityInvocationResult {
    let output = json!({
        "connection_type": docker_connection_type(config),
        "timeout_secs": config.timeout,
        "network_checked": false,
    });
    result(invocation_id, output.clone(), output, None)
}

async fn invoke_list_containers(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let all = optional_bool(&invocation.input, "all")?.unwrap_or(true);
    let page = page_request(invocation.controls.page.as_ref())?;
    let docker = docker_client(config)?;
    let mut containers = docker_ops::list_containers(&docker, all)
        .await
        .map_err(|error| target_error("docker.list_containers_failed", error.to_string()))?;
    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    let output_items = containers.iter().map(container_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "containers",
        output_items,
        page,
        json!({ "all": all }),
    ))
}

async fn invoke_list_images(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let docker = docker_client(config)?;
    let mut images = docker_ops::list_images(&docker)
        .await
        .map_err(|error| target_error("docker.list_images_failed", error.to_string()))?;
    images.sort_by(|left, right| left.tags.cmp(&right.tags).then(left.id.cmp(&right.id)));

    let output_items = images.iter().map(image_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "images",
        output_items,
        page,
        Value::Null,
    ))
}

async fn invoke_list_networks(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let docker = docker_client(config)?;
    let mut networks = docker_ops::list_networks(&docker)
        .await
        .map_err(|error| target_error("docker.list_networks_failed", error.to_string()))?;
    networks.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    let output_items = networks.iter().map(network_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "networks",
        output_items,
        page,
        Value::Null,
    ))
}

async fn invoke_list_volumes(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let docker = docker_client(config)?;
    let mut volumes = docker_ops::list_volumes(&docker)
        .await
        .map_err(|error| target_error("docker.list_volumes_failed", error.to_string()))?;
    volumes.sort_by(|left, right| left.name.cmp(&right.name));

    let output_items = volumes.iter().map(volume_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "volumes",
        output_items,
        page,
        Value::Null,
    ))
}

async fn invoke_inspect_container(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let id = required_string(&invocation.input, "id")?;
    let docker = docker_client(config)?;
    let inspect = docker_ops::inspect_container(&docker, &id)
        .await
        .map_err(|error| target_error("docker.inspect_container_failed", error.to_string()))?;
    let inspect_json = serde_json::to_value(inspect).map_err(|error| {
        capability_error(
            CapabilityErrorCategory::Internal,
            "internal.docker_inspect_encode_failed",
            "Docker inspect response could not be encoded.",
            json!({ "message": error.to_string() }),
            None,
            false,
        )
    })?;
    let summary = inspect_summary(&inspect_json);
    let output = json!({
        "container": summary,
        "raw_omitted": true,
    });
    let summary = output.clone();
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_logs(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let container_id = required_string(&invocation.input, "container_id")?;
    let tail = requested_usize(&invocation.input, "tail", DEFAULT_LOG_TAIL, 1, MAX_LOG_TAIL)?;
    let max_bytes = requested_usize(
        &invocation.input,
        "max_bytes",
        DEFAULT_TEXT_LIMIT_BYTES,
        1,
        MAX_TEXT_LIMIT_BYTES,
    )?;
    let docker = docker_client(config)?;
    let logs = docker_ops::read_logs(&docker, &container_id, tail, max_bytes)
        .await
        .map_err(|error| target_error("docker.logs_failed", error.to_string()))?;
    let output = json!({
        "container_id": container_id,
        "text": logs.text,
        "bytes_returned": logs.text.len(),
        "source_bytes": logs.source_bytes,
        "truncated": logs.truncated,
        "tail": tail,
        "byte_limit": max_bytes,
    });
    let summary = json!({
        "container_id": output["container_id"],
        "bytes_returned": output["bytes_returned"],
        "source_bytes": output["source_bytes"],
        "truncated": output["truncated"],
        "tail": tail,
    });
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_container_action(
    config: &DockerConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let id = required_string(&invocation.input, "id")?;
    let action = required_string(&invocation.input, "action")?;
    validate_container_action(&action)?;

    if invocation.controls.dry_run {
        return Ok(dry_run_result(
            invocation.id,
            "container_action",
            json!({ "id": id, "action": action }),
        ));
    }

    let docker = docker_client(config)?;
    match action.as_str() {
        "start" => docker_ops::start_container(&docker, &id).await,
        "stop" => docker_ops::stop_container(&docker, &id).await,
        "restart" => docker_ops::restart_container(&docker, &id).await,
        "remove" => docker_ops::remove_container(&docker, &id).await,
        _ => unreachable!("validated container action"),
    }
    .map_err(|error| target_error("docker.container_action_failed", error.to_string()))?;

    Ok(mutation_result(
        invocation.id,
        "container_action",
        json!({ "id": id, "action": action }),
    ))
}

fn docker_client(config: &DockerConfig) -> Result<bollard::Docker, CapabilityError> {
    docker_ops::create_client(config)
        .map_err(|error| target_error("docker.connect_failed", error.to_string()))
}

fn docker_connection_type(config: &DockerConfig) -> &'static str {
    match config.connection {
        DockerConnection::Local => "local",
        DockerConnection::Socket { .. } => "socket",
        DockerConnection::Http { .. } => "http",
        DockerConnection::Tls { .. } => "tls",
    }
}

fn validate_container_action(action: &str) -> Result<(), CapabilityError> {
    match action {
        "start" | "stop" | "restart" | "remove" => Ok(()),
        other => Err(validation_error(
            "validation.docker_container_action_invalid",
            "Docker container action is not supported.",
            json!({
                "action": other,
                "supported_actions": ["start", "stop", "restart", "remove"],
            }),
        )),
    }
}

fn paged_result(
    invocation_id: String,
    item_key: &str,
    items: Vec<Value>,
    page: PageRequest,
    metadata: Value,
) -> CapabilityInvocationResult {
    let source_count = items.len();
    let end = page.offset.saturating_add(page.limit).min(source_count);
    let page_items = if page.offset >= source_count {
        Vec::new()
    } else {
        items[page.offset..end].to_vec()
    };
    let next_cursor = (end < source_count).then(|| end.to_string());
    let item_count = page_items.len();
    let truncated = next_cursor.is_some();
    let output = json!({
        item_key: page_items,
        "item_count": item_count,
        "source_item_count": source_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "metadata": metadata,
    });
    let summary = json!({
        "item_key": item_key,
        "item_count": item_count,
        "source_item_count": source_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    result(invocation_id, output, summary, output_page)
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": true,
        "would_execute": true,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": true }),
        None,
    )
}

fn mutation_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": false,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": false }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn container_json(container: &ContainerInfo) -> Value {
    json!({
        "id": container.id,
        "name": container.name,
        "image": container.image,
        "state": container.state,
        "status": container.status,
        "ports": container.ports,
        "created": container.created,
    })
}

fn image_json(image: &ImageInfo) -> Value {
    json!({
        "id": image.id,
        "tags": image.tags,
        "size": image.size,
        "created": image.created,
    })
}

fn network_json(network: &NetworkInfo) -> Value {
    json!({
        "id": network.id,
        "name": network.name,
        "driver": network.driver,
        "scope": network.scope,
    })
}

fn volume_json(volume: &VolumeInfo) -> Value {
    json!({
        "name": volume.name,
        "driver": volume.driver,
        "mountpoint_present": !volume.mountpoint.is_empty(),
    })
}

fn inspect_summary(value: &Value) -> Value {
    let config = object_field(value, &["Config", "config"]).unwrap_or(&Value::Null);
    let state = object_field(value, &["State", "state"]).unwrap_or(&Value::Null);
    let network_settings =
        object_field(value, &["NetworkSettings", "network_settings"]).unwrap_or(&Value::Null);
    let env_count = object_field(config, &["Env", "env"])
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let mount_count = object_field(value, &["Mounts", "mounts"])
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let network_count = object_field(network_settings, &["Networks", "networks"])
        .and_then(Value::as_object)
        .map_or(0, serde_json::Map::len);

    json!({
        "id": string_field(value, &["Id", "ID", "id"]).unwrap_or_default(),
        "name": string_field(value, &["Name", "name"])
            .map(|name| name.trim_start_matches('/').to_string())
            .unwrap_or_default(),
        "image": string_field(config, &["Image", "image"]),
        "status": string_field(state, &["Status", "status"]),
        "running": object_field(state, &["Running", "running"]).and_then(Value::as_bool),
        "exit_code": object_field(state, &["ExitCode", "exit_code"]).and_then(Value::as_i64),
        "env_count": env_count,
        "mount_count": mount_count,
        "network_count": network_count,
    })
}

fn object_field<'a>(value: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| value.get(*name))
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    object_field(value, names)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn empty_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false
    })
}

fn docker_list_schema(item_key: &str, item_schema: Value) -> Value {
    json!({
        "type": "object",
        "required": [
            item_key,
            "item_count",
            "source_item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "metadata"
        ],
        "properties": {
            item_key: { "type": "array", "items": item_schema },
            "item_count": { "type": "integer", "minimum": 0 },
            "source_item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "metadata": { "type": ["object", "null"] }
        },
        "additionalProperties": false
    })
}

fn container_schema() -> Value {
    json!({
        "type": "object",
        "required": ["id", "name", "image", "state", "status", "ports", "created"],
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" },
            "image": { "type": "string" },
            "state": { "type": "string" },
            "status": { "type": "string" },
            "ports": { "type": "string" },
            "created": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn image_schema() -> Value {
    json!({
        "type": "object",
        "required": ["id", "tags", "size", "created"],
        "properties": {
            "id": { "type": "string" },
            "tags": { "type": "array", "items": { "type": "string" } },
            "size": { "type": "integer", "minimum": 0 },
            "created": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn network_schema() -> Value {
    json!({
        "type": "object",
        "required": ["id", "name", "driver", "scope"],
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" },
            "driver": { "type": "string" },
            "scope": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn volume_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "driver", "mountpoint_present"],
        "properties": {
            "name": { "type": "string" },
            "driver": { "type": "string" },
            "mountpoint_present": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn text_limit_schema() -> Value {
    json!({
        "type": "integer",
        "minimum": 1,
        "maximum": MAX_TEXT_LIMIT_BYTES,
        "default": DEFAULT_TEXT_LIMIT_BYTES
    })
}

fn text_output_schema(target_field: &str) -> Value {
    json!({
        "type": "object",
        "required": [
            target_field,
            "text",
            "bytes_returned",
            "source_bytes",
            "truncated",
            "tail",
            "byte_limit"
        ],
        "properties": {
            target_field: { "type": "string" },
            "text": { "type": "string" },
            "bytes_returned": { "type": "integer", "minimum": 0 },
            "source_bytes": { "type": "integer", "minimum": 0 },
            "truncated": { "type": "boolean" },
            "tail": { "type": "integer", "minimum": 1, "maximum": MAX_LOG_TAIL },
            "byte_limit": { "type": "integer", "minimum": 1, "maximum": MAX_TEXT_LIMIT_BYTES }
        },
        "additionalProperties": false
    })
}

fn mutation_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "operation", "dry_run", "destructive", "details"],
        "properties": {
            "ok": { "type": "boolean" },
            "operation": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "would_execute": { "type": "boolean" },
            "destructive": { "type": "boolean" },
            "details": { "type": "object" }
        },
        "additionalProperties": false
    })
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    optional_string(input, field)?.ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )
    })
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "String input field cannot be empty.",
                    json!({ "field": field }),
                )
            }),
        None => Ok(None),
    }
}

fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_boolean() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a boolean.",
            json!({ "field": field }),
        )),
        Some(value) => Ok(value.as_bool()),
        None => Ok(None),
    }
}

fn requested_usize(
    input: &Value,
    field: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, CapabilityError> {
    let Some(value) = input.get(field) else {
        return Ok(default);
    };
    let Some(raw) = value.as_u64() else {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be an integer.",
            json!({ "field": field }),
        ));
    };
    if raw < minimum as u64 || raw > maximum as u64 {
        return Err(validation_error(
            "validation.input_field_out_of_range",
            "Input field is outside the supported range.",
            json!({ "field": field, "minimum": minimum, "maximum": maximum }),
        ));
    }
    Ok(raw as usize)
}

#[derive(Debug)]
struct PageRequest {
    limit: usize,
    offset: usize,
    cursor: Option<String>,
}

fn page_request(page: Option<&Pagination>) -> Result<PageRequest, CapabilityError> {
    let Some(page) = page else {
        return Ok(PageRequest {
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
            cursor: None,
        });
    };
    let limit = (page.limit as usize).clamp(1, MAX_PAGE_LIMIT);
    let offset = match &page.cursor {
        Some(cursor) => cursor.parse::<usize>().map_err(|_| {
            validation_error(
                "validation.cursor_invalid",
                "Docker list cursor must be a numeric offset.",
                json!({ "cursor": cursor }),
            )
        })?,
        None => 0,
    };
    Ok(PageRequest {
        limit,
        offset,
        cursor: page.cursor.clone(),
    })
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(code: &str, message: String) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::TargetSystem,
        code,
        "Docker target operation failed.",
        json!({ "message": message }),
        Some(TargetSystemFailure {
            system: Some("docker".into()),
            code: Some(code.into()),
            message: Some(message),
        }),
        false,
    )
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction: RedactionStatus::NotRequired,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use voidb_core::{
        ActorRef, ActorType, ConnectionInstancePurpose, ConnectionProfileRef, InstanceReusePolicy,
        InvocationConnectionTarget, InvocationControls,
    };

    use super::*;

    #[test]
    fn catalog_marks_lifecycle_actions_as_destructive_dry_run() {
        let capabilities = docker_capabilities();
        let action = capabilities
            .iter()
            .find(|capability| capability.id == "container_action")
            .expect("container_action capability");
        assert!(action.destructive);
        assert!(action.supports_dry_run);
        assert_eq!(action.effective_risk(), CapabilityRiskLevel::Destructive);

        let logs = capabilities
            .iter()
            .find(|capability| capability.id == "logs")
            .expect("logs capability");
        assert!(!logs.destructive);
        assert!(!logs.supports_dry_run);
    }

    #[tokio::test]
    async fn lifecycle_dry_run_does_not_open_docker_connection() {
        let config = DockerConfig {
            connection: DockerConnection::Socket {
                path: "/no/such/docker.sock".into(),
            },
            timeout: 1,
        };
        let mut invocation = invocation(
            "container_action",
            json!({ "id": "abc123", "action": "restart" }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_docker_capability(&config, invocation)
            .await
            .expect("dry-run");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["details"]["action"], "restart");
    }

    #[tokio::test]
    async fn diagnostics_do_not_open_docker_connection() {
        let config = DockerConfig {
            connection: DockerConnection::Socket {
                path: "/no/such/docker.sock".into(),
            },
            timeout: 7,
        };

        let result = invoke_docker_capability(&config, invocation("diagnostics", json!({})))
            .await
            .expect("diagnostics");

        assert_eq!(result.output["connection_type"], "socket");
        assert_eq!(result.output["timeout_secs"], 7);
        assert_eq!(result.output["network_checked"], false);
    }

    #[tokio::test]
    async fn rejects_wrong_plugin_id() {
        let config = DockerConfig::default();
        let mut invocation = invocation("diagnostics", json!({}));
        invocation.plugin_id = "kubernetes".into();

        let error = invoke_docker_capability(&config, invocation)
            .await
            .expect_err("plugin mismatch");

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.plugin_mismatch");
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: format!("invoke-{}", capability_id),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::FromProfile {
                profile: ConnectionProfileRef::Name("docker".into()),
                purpose: ConnectionInstancePurpose::CapabilityInvocation,
                reuse: InstanceReusePolicy::Allow,
                options: Value::Null,
            },
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "test-agent".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }
}
