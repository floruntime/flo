#!/bin/bash
set -euo pipefail

# Flo EC2 User Data — installs Flo, mounts data volume, starts as systemd service.
# Templated by Terraform (variables: flo_version, data_device, data_mount, shard_count).

FLO_VERSION="${flo_version}"
DATA_DEVICE="${data_device}"
DATA_MOUNT="${data_mount}"
SHARD_COUNT="${shard_count}"

exec > /var/log/flo-user-data.log 2>&1
echo "=== Flo setup started at $(date -u) ==="

# --- 1. Format and mount data volume ---

echo "Waiting for data volume $DATA_DEVICE..."
while [ ! -b "$DATA_DEVICE" ]; do sleep 1; done

# Only format if no filesystem exists
if ! blkid "$DATA_DEVICE" >/dev/null 2>&1; then
  echo "Formatting $DATA_DEVICE as ext4..."
  mkfs.ext4 -m 1 -L flo-data "$DATA_DEVICE"
fi

mkdir -p "$DATA_MOUNT"
mount -o noatime,discard "$DATA_DEVICE" "$DATA_MOUNT"

# Persist in fstab
if ! grep -q "$DATA_MOUNT" /etc/fstab; then
  echo "LABEL=flo-data $DATA_MOUNT ext4 noatime,discard 0 2" >> /etc/fstab
fi

# --- 2. Install Flo ---

echo "Installing Flo..."
if [ -n "$FLO_VERSION" ]; then
  curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh -s -- --version "$FLO_VERSION"
else
  curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh
fi

# Verify
flo --version || /usr/local/bin/flo --version

# --- 3. Create system user ---

if ! id flo >/dev/null 2>&1; then
  useradd --system --no-create-home --shell /usr/sbin/nologin flo
fi
chown -R flo:flo "$DATA_MOUNT"

# --- 4. Write config ---

mkdir -p /etc/flo
cat > /etc/flo/flo.toml <<'TOML'
[server]
port = 9000
bind = "0.0.0.0"
data_dir = "${data_mount}"
shards = ${shard_count}

[storage]
durability = "async_flush"
hot_buffer_capacity = 67108864

[logging]
level = "info"

[metrics]
enabled = true

[dashboard]
enabled = true
bind = "0.0.0.0"
TOML

# --- 5. Create systemd service ---

cat > /etc/systemd/system/flo.service <<'UNIT'
[Unit]
Description=Flo Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=flo
Group=flo
ExecStart=/usr/local/bin/flo server start -c /etc/flo/flo.toml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable flo
systemctl start flo

# --- 6. Verify ---

echo "Waiting for Flo to start..."
for i in $(seq 1 30); do
  if nc -z localhost 9000 2>/dev/null; then
    echo "Flo is up on port 9000"
    break
  fi
  sleep 1
done

echo ""
echo "=== Flo setup completed at $(date -u) ==="
echo "  Wire protocol: 0.0.0.0:9000"
echo "  Dashboard:     http://0.0.0.0:9002"
echo "  Data dir:      $DATA_MOUNT"
echo "  Config:        /etc/flo/flo.toml"
echo "  Logs:          journalctl -u flo -f"
