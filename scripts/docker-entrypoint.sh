#!/bin/sh
# Entrypoint for the flo container image.
#
# A freshly-provisioned volume (DO Block Storage, EBS, k8s PV without fsGroup,
# `docker -v new-volume:...`, Nomad host_volume, etc.) masks the image's
# pre-chowned /data/flo with a root-owned ext4/xfs inode. Without this fixup,
# flo starts as uid 1000 and trips error.AccessDenied on its first write.
# Pattern lifted from the official postgres/redis/mongo images.
set -e

DATA_DIR="${FLO_DATA_DIR:-/data/flo}"

if [ -d "$DATA_DIR" ] && [ "$(stat -c %u "$DATA_DIR")" != "1000" ]; then
    chown -R flo:flo "$DATA_DIR"
fi

if [ "$(id -u)" = "0" ]; then
    exec su-exec flo:flo /usr/local/bin/flo "$@"
fi

exec /usr/local/bin/flo "$@"
