---
name: voidb-plugin-docker
description: Guide for the VoidB Docker plugin. Use when modifying crates/plugins/voidb-plugin-docker, docker.* capabilities, Docker service commands/events, Docker CLI standalone TUI launch, Docker connection config, diagnostics, logs, or container lifecycle actions.
---

# VoidB Docker Plugin

## Start Here

Primary crate: `crates/plugins/voidb-plugin-docker`.

Inspect:

- `src/config.rs` for `DockerConfig` and socket/TCP connection modes.
- `src/docker_ops.rs` for low-level Docker operations.
- `src/service/` for `DockerService`, commands, and events.
- `src/capabilities.rs` for `docker.*` metadata and invocation.
- `src/cli_plugin.rs` for `voidb-cli docker tui`.
- `src/tui.rs` for standalone Docker operations TUI.
- `docs/cloud-operations-promotion-report.md` and `docs/live-operations-tui-safety.md` for operational safety context.

## Boundaries

- Keep `bollard` client usage inside the plugin crate.
- Treat container lifecycle actions as destructive or operationally sensitive.
- Keep diagnostics and target errors redacted and deterministic when Docker is unavailable.
- Do not make shell/core depend on Docker client types.
- Prefer service commands/events for TUI flows; avoid blocking Docker calls in rendering paths.

## CLI And Capabilities

- CLI commands: `tui`.
- Capabilities: `docker.diagnostics`, `docker.list_containers`, `docker.list_images`, `docker.list_networks`, `docker.list_volumes`, `docker.inspect_container`, `docker.logs`, `docker.container_action`.

## Validation

- Focused gate: `cargo test -p voidb-plugin-docker`.
- Add `cargo test -p voidb-cli invoke` for capability or generic invoke changes.
- Docker lifecycle helper when fixture infrastructure changes: `scripts/local-fixture-smoke.sh run --fixture probe --report target/tmp/local-fixture-probe-evidence.md`.
- Always run `git diff --check`.
