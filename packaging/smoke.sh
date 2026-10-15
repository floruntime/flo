#!/bin/sh
# Install, run, upgrade over itself and remove a flo package, checking what
# each step leaves. Run as root: packaging/smoke.sh <package.deb|package.rpm>
# Uses systemd when it is running; otherwise starts the server as `flo`
# with the packaged config, as the unit would.
set -eu
pkg=$1
case "$pkg" in
    *.deb) install() { dpkg -i "$pkg"; }; remove() { dpkg --remove flo; }; purge() { dpkg --purge flo; } ;;
    *.rpm) install() { rpm -Uvh --replacepkgs "$pkg"; }; remove() { rpm -e flo; }; purge() { :; } ;;
    *) echo "not a .deb or .rpm: $pkg" >&2; exit 2 ;;
esac
fail() { echo "FAIL: $*" >&2; exit 1; }
systemd=no
[ -d /run/systemd/system ] && systemd=yes

start() {
    if [ $systemd = yes ]; then
        systemctl enable --now flo
    else
        su -s /bin/sh flo -c '/usr/bin/flo server start --config /etc/flo/flo.toml >/tmp/flo-smoke.log 2>&1 & echo $! >/tmp/flo-smoke.pid'
    fi
    up
}
# Wait until the server answers.
up() {
    for _ in $(seq 1 50); do
        flo kv set __probe 1 -e 127.0.0.1:9000 >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    [ -f /tmp/flo-smoke.log ] && cat /tmp/flo-smoke.log
    [ $systemd = yes ] && journalctl -u flo --no-pager | tail -20
    fail "the server did not start"
}
stop() {
    if [ $systemd = yes ]; then
        systemctl stop flo
        return
    fi
    pid=$(cat /tmp/flo-smoke.pid)
    kill "$pid"
    for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.2
    done
    fail "the server did not stop"
}

echo "== install"
install
getent passwd flo >/dev/null || fail "no flo user"
[ "$(stat -c %U:%G /var/lib/flo)" = flo:flo ] || fail "/var/lib/flo is not owned by flo"
[ "$(stat -c %a /var/lib/flo)" = 750 ] || fail "/var/lib/flo is not 0750"
grep -q '^bind = "127.0.0.1"' /etc/flo/flo.toml || fail "the config doesn't bind 127.0.0.1"
[ "$(stat -c %U:%G:%a /etc/flo/flo.toml)" = root:flo:640 ] || fail "/etc/flo/flo.toml is not root:flo 0640"
if [ $systemd = yes ]; then
    [ "$(systemctl is-enabled flo || true)" = disabled ] || fail "the service was enabled on install"
    [ "$(systemctl is-active flo || true)" != active ] || fail "the service was started on install"
    [ "$(systemctl show flo -p StartLimitBurst --value)" = 10 ] || fail "the unit's start limit isn't 10"
fi
command -v systemd-analyze >/dev/null || fail "systemd-analyze is missing: can't verify the unit"
systemd-analyze verify /usr/lib/systemd/system/flo.service

echo "== run"
start
flo kv set smoke kept -e 127.0.0.1:9000
[ "$(flo kv get smoke -e 127.0.0.1:9000 -o raw)" = kept ] || fail "kv get"

echo "== upgrade over itself"
echo "# edited" >> /etc/flo/flo.toml
[ $systemd = yes ] && pid=$(systemctl show flo -p MainPID --value)
install
grep -q '^# edited' /etc/flo/flo.toml || fail "the upgrade replaced the edited config"
if [ $systemd = yes ]; then
    # The upgrade restarted the running service, which stays enabled.
    [ "$(systemctl is-active flo)" = active ] || fail "the service is not running after the upgrade"
    [ "$(systemctl show flo -p MainPID --value)" != "$pid" ] || fail "the upgrade didn't restart the service"
    [ "$(systemctl is-enabled flo)" = enabled ] || fail "the upgrade disabled the service"
    up
else
    stop
    start
fi
[ "$(flo kv get smoke -e 127.0.0.1:9000 -o raw)" = kept ] || fail "the data did not survive the upgrade"

echo "== remove"
# With systemd, removal itself must stop and disable the running service.
[ $systemd = yes ] || stop
remove
if [ $systemd = yes ]; then
    [ "$(systemctl is-active flo || true)" != active ] || fail "removal left the service running"
    [ "$(systemctl is-enabled flo 2>/dev/null || true)" != enabled ] || fail "removal left the service enabled"
fi
[ ! -e /usr/bin/flo ] || fail "/usr/bin/flo is still there"
[ -d /var/lib/flo ] && [ -n "$(ls -A /var/lib/flo)" ] || fail "the data was removed"
getent passwd flo >/dev/null || fail "the flo user was removed"
purge
[ -d /var/lib/flo ] || fail "purge removed the data"
case "$pkg" in *.deb) [ ! -e /etc/flo ] || fail "purge left /etc/flo" ;; esac
echo "OK"
