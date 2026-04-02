#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

DEFAULT_DASHBOARD_FILES = [
    "metrics/grafana/ticdc_new_arch.json",
    "metrics/grafana/ticdc_new_arch_next_gen.json",
    "metrics/grafana/ticdc_new_arch_with_keyspace_name.json",
]
DEFAULT_CHECKSUM_FILES = [
    "metrics/grafana/ticdc_new_arch.json.sha256",
    "metrics/grafana/ticdc_new_arch_next_gen.json.sha256",
    "metrics/grafana/ticdc_new_arch_with_keyspace_name.json.sha256",
]


def discover_repo_root():
    return Path(__file__).absolute().parents[1]


def resolve_repo_path(repo_root, path):
    path = Path(path)
    if path.is_absolute():
        return path
    return repo_root / path


def overlaps(left, right):
    return (
        left["x"] < right["x"] + right["w"]
        and left["x"] + left["w"] > right["x"]
        and left["y"] < right["y"] + right["h"]
        and left["y"] + left["h"] > right["y"]
    )


def collect(items, parents=()):
    result = []
    for item in items:
        title = item.get("title", "<untitled>")
        path = " / ".join(parents + (title,))
        grid_pos = item.get("gridPos")
        if grid_pos:
            result.append(
                {
                    "path": path,
                    "x": grid_pos["x"],
                    "y": grid_pos["y"],
                    "w": grid_pos["w"],
                    "h": grid_pos["h"],
                }
            )
    return result


def collect_ids(items, parents=()):
    result = []
    for item in items:
        title = item.get("title", "<untitled>")
        path = " / ".join(parents + (title,))
        if "id" in item:
            result.append({"id": item["id"], "path": path})
        nested = item.get("panels", [])
        if nested:
            result.extend(collect_ids(nested, parents + (title,)))
    return result


def check_container(items, parents=()):
    messages = []
    panels = collect(items, parents)
    for i, left in enumerate(panels):
        for right in panels[i + 1 :]:
            if overlaps(left, right):
                messages.append(f"Overlap: {left['path']} <-> {right['path']}")

    for item in items:
        nested = item.get("panels", [])
        if nested:
            title = item.get("title", "<untitled>")
            messages.extend(check_container(nested, parents + (title,)))
    return messages


def check_dashboard_file(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    messages = []
    id_groups = defaultdict(list)
    for item in collect_ids(data.get("panels", [])):
        id_groups[item["id"]].append(item["path"])

    for panel_id, paths in sorted(id_groups.items()):
        if len(paths) > 1:
            messages.append(f"Duplicate ID {panel_id}: " + " <-> ".join(paths))

    messages.extend(check_container(data.get("panels", [])))
    return messages


def check_dashboards(repo_root, dashboard_files):
    messages = []
    for dashboard_file in dashboard_files:
        path = resolve_repo_path(repo_root, dashboard_file)
        relative = path.resolve().relative_to(repo_root.resolve()).as_posix()
        if not path.exists():
            messages.append(f"Missing dashboard file: {relative}")
            continue
        for message in check_dashboard_file(path):
            messages.append(f"{relative}: {message}")
    return messages


def check_checksums(repo_root, checksum_files):
    messages = []
    for checksum_file in checksum_files:
        checksum_path = resolve_repo_path(repo_root, checksum_file)
        relative_checksum = checksum_path.resolve().relative_to(repo_root.resolve()).as_posix()
        if not checksum_path.exists():
            messages.append(f"Missing checksum file: {relative_checksum}")
            continue

        for line in checksum_path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            if "  " not in line:
                messages.append(f"Malformed checksum entry in {relative_checksum}: {line}")
                continue
            expected, relative_path = line.split("  ", 1)
            if not relative_path or relative_path.startswith("/"):
                messages.append(
                    f"Checksum path must be repo relative in {relative_checksum}: {line}"
                )
                continue

            data_path = repo_root / relative_path
            if not data_path.exists():
                messages.append(
                    f"Missing dashboard artifact referenced by {relative_checksum}: {relative_path}"
                )
                continue

            actual = hashlib.sha256(data_path.read_bytes()).hexdigest()
            if actual != expected:
                messages.append(
                    f"Checksum mismatch for {relative_path}: expected {expected}, got {actual}"
                )
    return messages


def main(argv=None):
    repo_root = discover_repo_root()
    argv = list(sys.argv[1:] if argv is None else argv)

    if argv:
        dashboard_files = []
        checksum_files = []
        for arg in argv:
            if arg.endswith(".sha256"):
                checksum_files.append(arg)
            else:
                dashboard_files.append(arg)
    else:
        dashboard_files = list(DEFAULT_DASHBOARD_FILES)
        checksum_files = list(DEFAULT_CHECKSUM_FILES)

    messages = []
    messages.extend(check_dashboards(repo_root, dashboard_files))
    messages.extend(check_checksums(repo_root, checksum_files))

    if messages:
        print("\n".join(messages))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
