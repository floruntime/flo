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

A server refuses a volume written by an older Flo and exits with code 78;
`restart: unless-stopped` then restarts it in a loop. There is no migration:
stop the service and empty the volume, then start it again:

```bash
docker compose stop flo
docker compose run --rm flo server wipe -c /etc/flo/flo.toml --yes
docker compose start flo
```

## Logs

```bash
docker compose logs -f flo
```
