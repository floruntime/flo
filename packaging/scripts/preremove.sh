#!/bin/sh
# deb: "remove" or "upgrade"; rpm: 0 on removal, 1 on upgrade.
set -e
case "$1" in
    remove|0)
        if [ -d /run/systemd/system ]; then
            systemctl disable --now flo.service >/dev/null 2>&1 || true
        fi
        ;;
esac
