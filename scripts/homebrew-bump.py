#!/usr/bin/env python3
"""Point the Homebrew formula at a release: its version, asset URLs and sha256s.

Usage: homebrew-bump.py <Formula/flo.rb> <tag, e.g. v0.1.0-dev.12> <dir with flo-*.tar.gz.sha256>
"""
import pathlib
import re
import sys

formula_path, tag, sums_dir = sys.argv[1:4]
formula = pathlib.Path(formula_path).read_text()

formula, n = re.subn(r'version "[^"]*"', f'version "{tag.removeprefix("v")}"', formula)
if n != 1:
    sys.exit(f"expected one version line in {formula_path}, found {n}")

for asset in ("aarch64-macos", "x86_64-macos", "aarch64-linux", "x86_64-linux"):
    name = f"flo-{asset}.tar.gz"
    digest = (pathlib.Path(sums_dir) / f"{name}.sha256").read_text().split()[0]
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        sys.exit(f"{name}.sha256 does not hold a sha256: {digest!r}")
    # Each asset's url line, then its sha256 line.
    pattern = re.compile(r'(url "https://github\.com/floruntime/flo/releases/download/)[^/]+(/' + re.escape(name) + r'"\s*\n\s*sha256 ")[0-9a-f]{64}"')
    formula, n = pattern.subn(lambda m: f'{m.group(1)}{tag}{m.group(2)}{digest}"', formula)
    if n != 1:
        sys.exit(f"expected one url and sha256 for {name} in {formula_path}, found {n}")

pathlib.Path(formula_path).write_text(formula)
