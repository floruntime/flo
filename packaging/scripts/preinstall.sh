#!/bin/sh
# The flo user the service runs as; kept on removal, with its data.
set -e
if ! getent group flo >/dev/null; then
    groupadd --system flo
fi
if ! getent passwd flo >/dev/null; then
    useradd --system --gid flo --home-dir /var/lib/flo --no-create-home \
        --shell /usr/sbin/nologin --comment "Flo server" flo
fi
