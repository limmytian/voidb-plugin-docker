# VoidB Docker Plugin (`voidb-plugin-docker`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Docker containers, images, volumes, and networks.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `tui`: Standalone full-screen interactive terminal interface.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Rich container and resource inspection commands (`ps`, `images`, `volumes`, `networks`).
- **Capability Surface**:
  - `diagnostics`: Return agent-safe profile diagnostics without connecting to Docker daemon.
  - `list_containers`: Cursor-based container listing (running or all).
  - `list_images`: List container images.
  - `list_networks`: List Docker networks.
  - `list_volumes`: List Docker volumes.
  - `inspect_container`: Bounded summary of container configuration and runtime state.
  - `logs`: Read bounded container logs without streaming.
  - `logs_follow`: Real-time streaming log follow with bounded memory buffer.
  - `stats_follow`: Stream coalesced container CPU, memory, and IO statistics.
  - `events_follow`: Stream daemon event notifications.
  - `exec_read`, `exec_input`, `exec_resize`, `exec_signal`: Interactive, controlled execution inside containers.
  - `attach_read`, `attach_input`, `attach_resize`: Interactive attach to running container stdin/stdout.
  - `container_action`: Container lifecycle management (start, stop, restart, remove) with dry-run support.

## Usage & Operational Modes

When installed, `voidb-cli` automatically discovers this plugin and provides seamless transparent dispatch:

### 1. Human Operations (CLI & TUI)

```bash
# Connectivity test
voidb-cli docker test --profile local-docker

# Launch standalone interactive fullscreen TUI
voidb-cli docker tui --profile local-docker

# Inspect containers, images, volumes, networks
voidb-cli docker ps --profile local-docker
voidb-cli docker ps -a --profile local-docker
voidb-cli docker images --profile local-docker
voidb-cli docker volumes --profile local-docker
voidb-cli docker networks --profile local-docker
```

### 2. Machine Communication (AI Agents & JSON-RPC)

```bash
# Run the stdio-jsonrpc server directly or via voidb-cli
voidb-cli docker serve
# Or directly run the binary:
./bin/voidb-plugin-docker serve
```

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory (`~/Library/Application Support/voidb/plugins/docker` on macOS, or `~/.local/share/voidb/plugins/docker` on Linux):

```bash
mkdir -p "$HOME/Library/Application Support/voidb/plugins/docker"
cp -r plugin.toml bin schemas "$HOME/Library/Application Support/voidb/plugins/docker/"
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli docker --help
```

### Development & Build

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-docker bin/
```

## Protocol Specifications

Complies with the [VoidB Process Plugin Protocol](https://github.com/limmytian/voidb/blob/main/docs/quickstart-process-plugin.md) specification (v1.0).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
