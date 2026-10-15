#!/bin/sh
# deb: "configure" [old-version]; rpm: 1 on install, 2 on upgrade.
set -e
chown flo:flo /var/lib/flo
chmod 0750 /var/lib/flo

upgrade=no
case "$1" in
    configure) [ -n "$2" ] && upgrade=yes ;;
    2) upgrade=yes ;;
esac

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    # A running service picks up the new binary; a stopped one stays stopped.
    [ "$upgrade" = yes ] && systemctl try-restart flo.service >/dev/null 2>&1 || true
fi

if [ "$upgrade" = no ]; then
    cat <<'MSG'
flo is installed but not running. Its config is /etc/flo/flo.toml (the client
port listens on 127.0.0.1 only) and its data goes in /var/lib/flo.
Start it now and at every boot with:

    sudo systemctl enable --now flo
MSG
fi
