#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Generate checked-in Grafana JSON artifacts from the Python dashboard source."""

from __future__ import annotations

import hashlib
import json
import runpy
import subprocess
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from metrics.panel_ids import (
    PANEL_ID_REGISTRY_FILE,
    PanelIdRegistry,
    build_panel_id_resolver,
    load_panel_id_registry,
    seed_panel_id_registry,
    sync_panel_ids,
    write_panel_id_registry,
)

MIN_PYTHON = (3, 12)
BASE_DASHBOARD_SOURCE = "metrics/dashboard.py"
BASE_DASHBOARD_JSON = "metrics/grafana/ticdc_new_arch.json"
DEFAULT_DASHBOARD_FILES = [
    BASE_DASHBOARD_JSON,
    "metrics/grafana/ticdc_new_arch_next_gen.json",
    "metrics/grafana/ticdc_new_arch_with_keyspace_name.json",
]


def discover_repo_root():
    """Resolve the repository root from this script location."""

    return Path(__file__).absolute().parents[1]


def require_python_version():
    """Fail early with a clear message when the runtime is too old."""

    if sys.version_info < MIN_PYTHON:
        raise SystemExit(f"python3 >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]} is required")


def load_dashboard_module(repo_root):
    """Load `metrics/dashboard.py` with repo root on `sys.path`.

    This keeps generation simple: the dashboard source stays as ordinary repo
    code and does not need packaging or installation first.
    """

    dashboard_source = repo_root / BASE_DASHBOARD_SOURCE
    old_sys_path = list(sys.path)
    try:
        sys.path.insert(0, str(repo_root))
        globals_dict = runpy.run_path(str(dashboard_source))
    finally:
        sys.path[:] = old_sys_path

    return globals_dict


def load_dashboard_builder(repo_root):
    """Load the default `build_dashboard()` entrypoint for callers and tests."""

    dashboard_module = load_dashboard_module(repo_root)
    build_dashboard = dashboard_module.get("build_dashboard")
    if build_dashboard is None:
        raise SystemExit("missing build_dashboard() in metrics/dashboard.py")
    return build_dashboard


def write_json(path, payload):
    """Write deterministic JSON with a trailing newline for stable diffs."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=False)
        f.write("\n")


def make_checksum_line(repo_root, path):
    """Build one checksum line using a repo-relative artifact path."""

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    relative = path.resolve().relative_to(repo_root.resolve()).as_posix()
    return f"{digest}  {relative}"


def write_checksum_file(repo_root, dashboard_path):
    checksum_path = dashboard_path.parent / f"{dashboard_path.name}.sha256"
    checksum_path.write_text(
        make_checksum_line(repo_root, dashboard_path) + "\n",
        encoding="utf-8",
    )
    return checksum_path


def run_next_gen_generator(repo_root):
    """Delegate next-gen artifact derivation to the existing shell pipeline."""

    subprocess.run(
        ["bash", str(repo_root / "scripts/generate-next-gen-metrics.sh")],
        cwd=repo_root,
        check=True,
    )


def build_stable_panel_id_registry(
    spec,
    *,
    registry_path: Path,
    base_dashboard_path: Path,
) -> PanelIdRegistry:
    if registry_path.exists():
        registry = load_panel_id_registry(registry_path)
    elif base_dashboard_path.exists():
        registry = seed_panel_id_registry(
            spec,
            json.loads(base_dashboard_path.read_text(encoding="utf-8")),
        )
    else:
        registry = PanelIdRegistry.empty()
    return sync_panel_ids(spec, registry)


def main():
    """Generate the base dashboard first, then refresh all adjacent checksums."""

    require_python_version()
    repo_root = discover_repo_root()
    dashboard_module = load_dashboard_module(repo_root)
    build_dashboard_spec = dashboard_module.get("build_dashboard_spec")
    build_dashboard_with_panel_ids = dashboard_module.get("build_dashboard_with_panel_ids")
    if build_dashboard_spec is None:
        raise SystemExit("missing build_dashboard_spec() in metrics/dashboard.py")
    if build_dashboard_with_panel_ids is None:
        raise SystemExit("missing build_dashboard_with_panel_ids() in metrics/dashboard.py")

    base_dashboard_path = repo_root / BASE_DASHBOARD_JSON
    panel_id_registry_path = repo_root / PANEL_ID_REGISTRY_FILE
    spec = build_dashboard_spec()
    panel_id_registry = build_stable_panel_id_registry(
        spec,
        registry_path=panel_id_registry_path,
        base_dashboard_path=base_dashboard_path,
    )
    dashboard = build_dashboard_with_panel_ids(
        spec,
        build_panel_id_resolver(panel_id_registry),
    )
    if not isinstance(dashboard, dict):
        raise SystemExit("build_dashboard_with_panel_ids() must return a dict")

    write_json(base_dashboard_path, dashboard)
    write_panel_id_registry(panel_id_registry_path, panel_id_registry)
    run_next_gen_generator(repo_root)

    for relative_path in DEFAULT_DASHBOARD_FILES:
        write_checksum_file(repo_root, repo_root / relative_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
