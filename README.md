# VoidB Docker Plugin (`voidb-plugin-docker`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Docker containers, images, volumes, and networks.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
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
- **Dual Mode**: Can run as a JSON-RPC worker server (`voidb-plugin-docker serve`) or standalone interactive TUI.

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory:

```bash
mkdir -p ~/.config/voidb/plugins/docker
cp -r plugin.toml bin schemas ~/.config/voidb/plugins/docker/
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli plugin describe docker
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
