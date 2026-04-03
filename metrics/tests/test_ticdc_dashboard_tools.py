#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import json
import pathlib
import runpy
import subprocess
import sys
import tempfile
import unittest
from typing import Any

from metrics.panel_ids import PANEL_ID_REGISTRY_FILE, load_panel_id_registry

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
META_PATH = REPO_ROOT / "metrics/dashboard_meta.py"
REFERENCE_DASHBOARD = json.loads(
    (REPO_ROOT / "metrics/grafana/ticdc_new_arch.json").read_text(encoding="utf-8")
)
PREVIOUS_RELEASE_DASHBOARD_VERSION = 40
EXPECTED_GRAPH_DECIMALS = {
    ("Summary", "Sink Event Row Count / s"): 0,
    ("Summary", "Table Dispatcher Count"): 0,
    ("Dataflow", "EventService Output Event Row / s"): 0,
    ("Dataflow", "Event Collector Received Event Rows / s"): 0,
    ("Dataflow", "Sink Flush Rows / s"): 0,
    ("Dataflow", "Sink Flush Rows"): 0,
    ("Server", "Ownership History"): 0,
    ("Server", "PD Leader History"): 0,
    ("Changefeed", "Node Table Count"): 0,
    ("Changefeed", "Changefeed Table Count"): 0,
    ("Lag analyze", "Sink Write Rows / s"): 0,
    ("Coordinator", "Coordinator History"): 0,
    ("Maintainer", "Changefeed Maintainer Count"): 0,
    ("Maintainer", "Maintainer Handle Event Duration"): 0,
    ("Maintainer", "Maintainer Event Channel Length"): 0,
    ("Event Store", "Subscription Num"): 0,
    ("Event Store", "pebble level files"): 0,
    ("Schema Store", "Shared Column Schema Count"): 0,
    ("Event Service", "Event Service Channel Size"): 0,
    ("Event Service", "EventService Output Different DML Event Types / s"): 0,
    ("Dispatcher", "Table Dispatcher Manager Count"): 0,
    ("Dispatcher", "Table Trigger Dispatcher Count"): 0,
    ("Dynamic Stream", "DS Input Channel Length"): 0,
    ("Sink - Transaction Sink", "Row Affected Count / m"): 0,
    ("Sink - Cloud Storage Sink", "Worker Busy Ratio"): 1,
    ("Sink - Cloud Storage Sink", "File Count / s"): 0,
    ("Sink - Cloud Storage Sink", "Flush Reason Count / s"): 1,
    ("Sink - Cloud Storage Sink", "Spool Segment Count"): 0,
    ("Sink - Cloud Storage Sink", "Spool Disk Bytes"): 1,
    ("Sink - Cloud Storage Sink", "Spool Rotate Count / s"): 1,
    ("Sink - Cloud Storage Sink", "Spool Disk Load Bytes / s"): 1,
    ("Sink - Cloud Storage Sink", "Pending PostEnqueue Count"): 0,
    ("Sink - Cloud Storage Sink", "Spool Disk Quota Waiters"): 0,
    ("Scheduler", "Slowest Table ID"): 0,
    ("Scheduler", "Slowest Table Replication State"): 0,
}


def run_python_file(
    testcase: unittest.TestCase,
    path: pathlib.Path,
    extra_sys_path: pathlib.Path | None = None,
):
    testcase.assertTrue(path.exists(), f"missing file: {path}")
    old_sys_path = list(sys.path)
    try:
        if extra_sys_path is not None:
            sys.path.insert(0, str(extra_sys_path))
        return runpy.run_path(str(path))
    finally:
        sys.path[:] = old_sys_path


def load_dashboard_meta(testcase: unittest.TestCase):
    return run_python_file(
        testcase,
        META_PATH,
        REPO_ROOT,
    )


def row_by_title(dashboard: dict[str, Any], title: str) -> dict[str, Any]:
    for row in dashboard["panels"]:
        if row["title"] == title:
            return row
    raise AssertionError(f"missing row {title}")


def panel_by_title(row: dict[str, Any], title: str) -> dict[str, Any]:
    for panel in row.get("panels", []):
        if panel["title"] == title:
            return panel
    raise AssertionError(f"missing panel {title}")


def panel_id_map(spec, dashboard: dict[str, Any]) -> dict[tuple[str, str], int]:
    result: dict[tuple[str, str], int] = {}
    for row_spec, row in zip(spec.rows, dashboard["panels"], strict=False):
        for panel_spec, panel in zip(
            row_spec.panels,
            row.get("panels", []),
            strict=False,
        ):
            row_key = row_spec.key if row_spec.key is not None else row_spec.title
            panel_key = panel_spec.key if panel_spec.key is not None else panel_spec.title
            result[(row_key, panel_key)] = panel["id"]
    return result


def registry_id_map() -> tuple[dict[tuple[str, str], int], int]:
    registry = load_panel_id_registry(REPO_ROOT / PANEL_ID_REGISTRY_FILE)
    return (
        {(entry.row_key, entry.panel_key): entry.id for entry in registry.entries},
        registry.next_id,
    )


def graph_decimals_map(dashboard: dict[str, Any]) -> dict[tuple[str, str], int]:
    result: dict[tuple[str, str], int] = {}
    for row in dashboard["panels"]:
        for panel in row.get("panels", []):
            if panel.get("type") != "graph":
                continue
            decimals = panel.get("yaxes", [{}])[0].get("decimals")
            if decimals is not None:
                result[(row["title"], panel["title"])] = decimals
    return result


class DashboardToolsTest(unittest.TestCase):
    def test_dashboard_version_advances_from_previous_release(self):
        dashboard_meta = load_dashboard_meta(self)

        self.assertGreater(
            dashboard_meta["DASHBOARD_VERSION"],
            PREVIOUS_RELEASE_DASHBOARD_VERSION,
        )

    def test_dashboard_build_matches_reference_outputs(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard_meta = load_dashboard_meta(self)

        dashboard = dashboard_globals["build_dashboard"]()

        self.assertIsInstance(dashboard, dict)
        self.assertEqual(dashboard_meta["BASE_DASHBOARD_TITLE"], dashboard["title"])
        self.assertEqual(dashboard_meta["BASE_DASHBOARD_UID"], dashboard["uid"])
        self.assertEqual(dashboard_meta["DASHBOARD_VERSION"], dashboard["version"])
        self.assertEqual([dashboard_meta["DATASOURCE_INPUT"]], dashboard["__inputs"])
        self.assertEqual(
            REFERENCE_DASHBOARD["templating"]["list"],
            dashboard["templating"]["list"],
        )
        self.assertEqual(
            REFERENCE_DASHBOARD["annotations"]["list"],
            dashboard["annotations"]["list"],
        )
        self.assertEqual(
            [row["title"] for row in REFERENCE_DASHBOARD["panels"]],
            [row["title"] for row in dashboard["panels"]],
        )
        payload = json.dumps(dashboard, ensure_ascii=False)
        self.assertIn("namespace", payload)
        self.assertIn("tidb_cluster", payload)

    def test_dashboard_spec_rows_follow_reference_order(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard_spec = dashboard_globals["build_dashboard_spec"]()

        self.assertEqual(
            [row["title"] for row in REFERENCE_DASHBOARD["panels"]],
            [row.title for row in dashboard_spec.rows],
        )

    def test_generator_cli_succeeds_from_repo_root(self):
        result = subprocess.run(
            ["python3", "metrics/generate_dashboards.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)

    def test_generator_writes_deterministic_json_with_trailing_newline(self):
        generator_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/generate_dashboards.py",
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp) / "dashboard.json"
            payload = {"title": "x", "panels": [], "templating": {"list": []}}
            generator_globals["write_json"](output, payload)
            first = output.read_text(encoding="utf-8")
            generator_globals["write_json"](output, payload)
            second = output.read_text(encoding="utf-8")

        self.assertEqual(first, second)
        self.assertTrue(first.endswith("\n"))
        self.assertEqual(
            '{\n  "title": "x",\n  "panels": [],\n  "templating": {\n    "list": []\n  }\n}\n',
            first,
        )

    def test_checksum_line_uses_repo_relative_path(self):
        generator_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/generate_dashboards.py",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            dashboard = root / "metrics/grafana/ticdc_new_arch.json"
            dashboard.parent.mkdir(parents=True, exist_ok=True)
            dashboard.write_text('{"title": "x"}\n', encoding="utf-8")
            line = generator_globals["make_checksum_line"](root, dashboard)

        self.assertRegex(line, r"^[0-9a-f]{64}  metrics/grafana/ticdc_new_arch\.json$")

    def test_checker_detects_checksum_mismatch(self):
        checker_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/check_dashboards.py",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            data = root / "metrics/grafana/ticdc_new_arch.json"
            sha = root / "metrics/grafana/ticdc_new_arch.json.sha256"
            data.parent.mkdir(parents=True, exist_ok=True)
            data.write_text('{"title": "x"}\n', encoding="utf-8")
            sha.write_text(
                ("0" * 64) + "  metrics/grafana/ticdc_new_arch.json\n",
                encoding="utf-8",
            )
            messages = checker_globals["check_checksums"](root, [sha])

        self.assertTrue(any("checksum mismatch" in message.lower() for message in messages))

    def test_checker_default_file_set_is_canonical(self):
        checker_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/check_dashboards.py",
        )

        self.assertEqual(
            [
                "metrics/grafana/ticdc_new_arch.json",
                "metrics/grafana/ticdc_new_arch_next_gen.json",
                "metrics/grafana/ticdc_new_arch_with_keyspace_name.json",
            ],
            checker_globals["DEFAULT_DASHBOARD_FILES"],
        )
        self.assertEqual(
            [f"{path}.sha256" for path in checker_globals["DEFAULT_DASHBOARD_FILES"]],
            checker_globals["DEFAULT_CHECKSUM_FILES"],
        )

    def test_dashboard_preserves_explicit_graph_decimals(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard = dashboard_globals["build_dashboard"]()

        self.assertEqual(EXPECTED_GRAPH_DECIMALS, graph_decimals_map(dashboard))

    def test_changefeed_error_details_stays_in_changefeed_row(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard = dashboard_globals["build_dashboard"]()
        changefeed_row = row_by_title(dashboard, "Changefeed")

        self.assertIn(
            "Changefeed Error Details",
            [panel["title"] for panel in changefeed_row["panels"]],
        )

    def test_dashboard_duplicate_title_panels_use_distinct_keys(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard_spec = dashboard_globals["build_dashboard_spec"]()
        rows = {row.title: row for row in dashboard_spec.rows}

        self.assertEqual(
            ["scan_duration_heatmap", "scan_duration_graph"],
            [
                panel.key
                for panel in rows["Event Service"].panels
                if panel.title == "Event Service Scan Duration"
            ],
        )
        self.assertEqual(
            ["pebble_flush_duration_primary", "pebble_flush_duration_secondary"],
            [
                panel.key
                for panel in rows["Event Store"].panels
                if panel.title == "pebble flush duration seconds"
            ],
        )

    def test_dashboard_panel_ids_are_registry_backed_and_unique(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard_spec = dashboard_globals["build_dashboard_spec"]()
        dashboard = dashboard_globals["build_dashboard"]()
        registry_ids, next_id = registry_id_map()
        ids_by_identity = panel_id_map(dashboard_spec, dashboard)
        ids = list(ids_by_identity.values())
        expected_ids = {identity: registry_ids[identity] for identity in ids_by_identity}

        self.assertTrue((REPO_ROOT / PANEL_ID_REGISTRY_FILE).exists())
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(panel_id > 0 for panel_id in ids))
        self.assertEqual(expected_ids, ids_by_identity)
        self.assertGreater(next_id, max(ids))

    def test_duration_panels_keep_consistent_grouping_and_legends(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard = dashboard_globals["build_dashboard"]()

        coordinator_panel = panel_by_title(
            row_by_title(dashboard, "Coordinator"),
            "Coordinator Operator Cost Duration",
        )
        self.assertEqual(
            [
                "99.9-{{namespace}}-{{changefeed}}-{{mode}}",
                "avg-{{namespace}}-{{changefeed}}-{{mode}}",
            ],
            [target["legendFormat"] for target in coordinator_panel["targets"]],
        )

        scheduler_panel = panel_by_title(
            row_by_title(dashboard, "Scheduler"),
            "Operator Cost Duration",
        )
        self.assertEqual(
            [
                "99.9-{{namespace}}-{{changefeed}}-{{mode}}",
                "avg-{{namespace}}-{{changefeed}}-{{mode}}",
            ],
            [target["legendFormat"] for target in scheduler_panel["targets"]],
        )

        conflict_panel = panel_by_title(
            row_by_title(dashboard, "Sink - Transaction Sink"),
            "Conflict Detect Duration",
        )
        self.assertIn(
            "by (le, namespace, changefeed, instance)",
            conflict_panel["targets"][0]["expr"],
        )
        self.assertIn(
            "by (namespace, changefeed, instance)",
            conflict_panel["targets"][1]["expr"],
        )

    def test_checker_cli_succeeds_from_non_repo_cwd(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                ["python3", str(REPO_ROOT / "metrics/check_dashboards.py")],
                cwd=tmp,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main()
