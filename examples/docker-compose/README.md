# Docker Compose Example

Run Flo with Docker Compose using the pre-built image from GitHub Container Registry.

## Quick Start

```bash
docker compose up -d
```

## Ports

| Port | Service |
|------|---------|
| 9000 | Wire protocol (clients, SDKs, CLI) |
| 9001 | Prometheus metrics |
| 9002 | Dashboard + REST API |

## Configuration

Edit `flo.toml` to customize. Changes require a restart:

```bash
docker compose restart
```

## Data

Data is stored in the `flo-data` Docker volume. To inspect:

```bash
docker volume inspect flo-data
```

## Logs

```bash
docker compose logs -f flo
```
