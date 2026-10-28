#!/bin/sh
# The CPU floor (x86-64-v2, or ARMv8 with CRC) is checked in three places:
# scripts/install.sh, packaging/scripts/preinstall.sh and the Homebrew
# formula. Fail unless every copy names the same flags for each arch.
#
#   scripts/check-cpu-floor.sh [path/to/Formula/flo.rb]
set -eu
cd "$(dirname "$0")/.."

# "x86_64 <flags>" and "aarch64 <flags>" from a shell copy.
from_shell() {
  sed -n -E 's/^ *(x86_64|aarch64)\) *CPU_KEY="[A-Za-z]*"; *NEEDED="([^"]*)".*/\1 \2/p' "$1"
}
# The same from the formula: each list sits beside the flag that marks its
# arch's cpuinfo line ("fpu" on x86, "fp" on ARM).
from_formula() {
  sed -n -E 's/.*\[%w\[([^]]*)\], "fpu"\].*/x86_64 \1/p; s/.*\[%w\[([^]]*)\], "fp"\].*/aarch64 \1/p' "$1" | sort
}

want=$(from_shell scripts/install.sh | sort)
if [ "$(printf '%s\n' "$want" | grep -c .)" -ne 2 ]; then
  echo "check-cpu-floor: expected an x86_64 and an aarch64 list in scripts/install.sh, found:"
  printf '%s\n' "$want"
  exit 1
fi
status=0
check() {
  if [ "$2" != "$want" ]; then
    printf 'check-cpu-floor: %s differs from scripts/install.sh\n  install.sh:\n%s\n  %s:\n%s\n' "$1" "$want" "$1" "$2"
    status=1
  fi
}
check packaging/scripts/preinstall.sh "$(from_shell packaging/scripts/preinstall.sh | sort)"
if [ $# -gt 0 ]; then check "$1" "$(from_formula "$1")"; fi
[ $status = 0 ] && echo "check-cpu-floor: the lists match ($(printf '%s' "$want" | tr '\n' ';'))"
exit $status
