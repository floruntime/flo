# Terraform EC2 Example

Provisions a single Flo server on EC2 with a dedicated EBS data volume.

## What You Get

- EC2 instance (default: `c6i.xlarge` — 4 vCPU, 8 GiB RAM)
- Separate gp3 EBS volume for data at `/var/lib/flo`
- Flo installed via the official installer, running as a systemd service
- Security group with ports 9000 (wire), 9001 (metrics), 9002 (dashboard), 22 (SSH)

## Quick Start

```bash
terraform init
terraform apply -var="key_name=my-key" -var="region=us-east-1"
```

After apply, Terraform outputs the connection details:

```
wire_endpoint = "54.xx.xx.xx:9000"
dashboard_url = "http://54.xx.xx.xx:9002"
ssh_command   = "ssh -i ~/.ssh/my-key.pem ubuntu@54.xx.xx.xx"
```

## Connect

```bash
# Point your CLI at the remote server
flo kv set hello world --endpoint 54.xx.xx.xx
flo kv get hello --endpoint 54.xx.xx.xx

# Or from SDKs
# Python: client = Flo(host="54.xx.xx.xx", port=9000)
# Node:   const client = new FloClient({ host: "54.xx.xx.xx" })
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `region` | `us-east-1` | AWS region |
| `instance_type` | `c6i.xlarge` | EC2 instance type |
| `key_name` | — | EC2 key pair name **(required)** |
| `allowed_cidr` | `0.0.0.0/0` | CIDR for inbound access |
| `flo_version` | latest | Flo release tag (e.g. `v0.1.0`) |
| `shard_count` | `0` (auto) | Number of shards |
| `data_volume_size_gb` | `50` | Data volume size in GiB |
| `data_volume_type` | `gp3` | EBS volume type |
| `data_volume_iops` | `3000` | gp3 provisioned IOPS |
| `data_volume_throughput` | `250` | gp3 throughput (MiB/s) |

## Instance Type Guide

| Workload | Instance | vCPU | RAM | Notes |
|----------|----------|------|-----|-------|
| Dev / testing | `t3.medium` | 2 | 4 GiB | Burstable, cheapest |
| Small production | `c6i.xlarge` | 4 | 8 GiB | 2 shards + acceptor + dashboard |
| Medium production | `c6i.2xlarge` | 8 | 16 GiB | 6 shards |
| Heavy / streaming | `c7i.4xlarge` | 16 | 32 GiB | 14 shards, latest gen |
| I/O intensive | `i3en.xlarge` | 4 | 32 GiB | NVMe instance store (ephemeral) |

Use `c*` (compute-optimised) for Flo's thread-per-shard model. `i3en` for maximum I/O but data is ephemeral — only with Raft replication enabled.

## Storage

The data volume is a separate EBS volume mounted at `/var/lib/flo`. This means:

- **Survives instance replacement** — stop, change instance type, start
- **Snapshotable** — `aws ec2 create-snapshot --volume-id vol-xxx`
- **Resizable** — modify volume online, then `resize2fs`
- **gp3 baseline**: 3000 IOPS + 125 MiB/s free, scales to 16K IOPS / 1000 MiB/s

## Troubleshooting

```bash
# SSH in
ssh -i ~/.ssh/my-key.pem ubuntu@<ip>

# Check service
sudo systemctl status flo
sudo journalctl -u flo -f

# Check setup log
sudo cat /var/log/flo-user-data.log

# Check data volume
df -h /var/lib/flo
```
