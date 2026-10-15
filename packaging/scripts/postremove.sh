#!/bin/sh
# deb: "remove", "purge" or "upgrade"; rpm: 0 on removal, 1 on upgrade.
# The data in /var/lib/flo and the flo user are always kept.
set -e
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
if [ "$1" = purge ]; then
    rm -rf /etc/flo
fi
