#!/bin/sh
set -e

# The binary needs hardware CRC32C: x86-64-v2, or ARMv8 with CRC. Below that
# it stops with an illegal instruction, so refuse here, before anything is
# unpacked; on an upgrade the installed version stays. The same check, with
# the same flag lists, is in scripts/install.sh (CI compares them). Only this
# arch's line is read: under emulation cpuinfo describes the host.
case "$(uname -m)" in
    x86_64)  CPU_KEY="flags";    NEEDED="cx16 lahf_lm pni popcnt sse4_1 sse4_2 ssse3" ;;
    aarch64) CPU_KEY="Features"; NEEDED="crc32" ;;
    *)       CPU_KEY="" ;;
esac
if [ -n "$CPU_KEY" ]; then
    CPU_FLAGS=$(grep -m1 -E "^${CPU_KEY}[[:space:]]*:" /proc/cpuinfo 2>/dev/null || true)
    if [ -z "$CPU_FLAGS" ]; then
        echo "Warning: could not read this CPU's $CPU_KEY from /proc/cpuinfo; flo needs an x86-64-v2 CPU or ARMv8 with CRC." >&2
    else
        MISSING=""
        for f in $NEEDED; do
            case " ${CPU_FLAGS#*:} " in
                *" $f "*) ;;
                *) MISSING="$MISSING $f" ;;
            esac
        done
        if [ -n "$MISSING" ]; then
            echo "Error: flo needs an x86-64-v2 CPU or ARMv8 with CRC; this CPU lacks:$MISSING." >&2
            exit 1
        fi
    fi
fi

# The flo user the service runs as; kept on removal, with its data.
if ! getent group flo >/dev/null; then
    groupadd --system flo
fi
if ! getent passwd flo >/dev/null; then
    useradd --system --gid flo --home-dir /var/lib/flo --no-create-home \
        --shell /usr/sbin/nologin --comment "Flo server" flo
fi
