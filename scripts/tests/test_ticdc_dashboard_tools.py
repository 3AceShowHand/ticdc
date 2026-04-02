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
import tomllib
import unittest
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
BASELINE_PATH = REPO_ROOT / "metrics/dashboard_baseline.py"
IDENTITY_PATH = REPO_ROOT / "metrics/dashboard_identity.py"
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


def load_baseline(testcase: unittest.TestCase):
    return run_python_file(
        testcase,
        BASELINE_PATH,
        REPO_ROOT,
    )


def load_identity(testcase: unittest.TestCase):
    return run_python_file(
        testcase,
        IDENTITY_PATH,
        REPO_ROOT,
    )


def row_by_title(dashboard: dict[str, Any], title: str) -> dict[str, Any]:
    for row in dashboard["panels"]:
        if row["title"] == title:
            return row
    raise AssertionError(f"missing row {title}")


def panel_id_map(spec, dashboard: dict[str, Any]) -> dict[tuple[str, str], int]:
    result: dict[tuple[str, str], int] = {}
    for row_spec, row in zip(spec.rows, dashboard["panels"], strict=False):
        for panel_spec, panel in zip(
            row_spec.panels,
            row.get("panels", []),
            strict=False,
        ):
            result[(row_spec.title, panel_spec.key)] = panel["id"]
    return result


def ordered_panel_ids(dashboard: dict[str, Any]) -> list[int]:
    result: list[int] = []
    for row in dashboard["panels"]:
        result.extend(panel["id"] for panel in row.get("panels", []))
    return result


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
    def test_metrics_python_baseline_matches_pyproject(self):
        generator_globals = run_python_file(
            self,
            REPO_ROOT / "scripts/gen-ticdc-dashboards",
        )
        with (REPO_ROOT / "pyproject.toml").open("rb") as f:
            pyproject = tomllib.load(f)

        ruff_target = pyproject["tool"]["ruff"]["target-version"]
        expected = (int(ruff_target[2]), int(ruff_target[3:]))
        self.assertEqual(expected, generator_globals["MIN_PYTHON"])

    def test_pyproject_limits_python_tooling_scope_to_metrics_dashboard_code(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as f:
            pyproject = tomllib.load(f)

        ruff = pyproject["tool"]["ruff"]
        self.assertEqual(["metrics", "scripts"], ruff["src"])
        self.assertIn("scripts/gen-ticdc-dashboards", ruff["extend-include"])
        self.assertIn("tests/integration_tests", ruff["extend-exclude"])

    def test_pyproject_declares_uv_managed_metrics_dashboard_project(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as f:
            pyproject = tomllib.load(f)

        project = pyproject["project"]
        self.assertEqual("ticdc-metrics-dashboard-tools", project["name"])
        self.assertEqual(">=3.12", project["requires-python"])
        self.assertEqual([], project["dependencies"])

        dependency_groups = pyproject["dependency-groups"]
        self.assertIn("ruff>=0.11,<0.12", dependency_groups["dev"])
        self.assertIn("ty>=0.0.27,<0.1", dependency_groups["dev"])

    def test_pyproject_declares_ty_scope_for_metrics_dashboard_code(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as f:
            pyproject = tomllib.load(f)

        ty = pyproject["tool"]["ty"]
        self.assertTrue(ty["terminal"]["error-on-warning"])

        environment = ty["environment"]
        self.assertEqual(["."], environment["root"])
        self.assertEqual("3.12", environment["python-version"])

        src = ty["src"]
        self.assertEqual(["metrics", "scripts"], src["include"])
        self.assertIn("tests/integration_tests", src["exclude"])

    def test_python_version_file_matches_metrics_dashboard_runtime(self):
        python_version = (REPO_ROOT / ".python-version").read_text(encoding="utf-8")
        self.assertEqual("3.12\n", python_version)

    def test_makefile_exposes_metrics_python_workflow_targets(self):
        makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")

        self.assertIn("\nmetrics-python-sync:\n", makefile)
        self.assertIn("\nmetrics-python-typecheck:\n", makefile)
        self.assertIn("\nmetrics-python-generate:\n", makefile)
        self.assertIn("\nmetrics-python-check:\n", makefile)
        self.assertIn("\nmetrics-python-test:\n", makefile)
        self.assertIn("@uv sync --group dev", makefile)
        self.assertIn("@uv run ty check", makefile)
        self.assertIn("@uv run ruff format --check metrics scripts", makefile)
        self.assertIn("@uv run ruff check metrics scripts", makefile)
        self.assertIn("@uv run python ./scripts/gen-ticdc-dashboards", makefile)
        self.assertIn(
            "@uv run python -m unittest discover -s scripts -p 'test_*.py' -v",
            makefile,
        )
        self.assertIn("@./scripts/check-ticdc-dashboard.sh", makefile)

    def test_readme_does_not_document_removed_builder_layout_kwargs(self):
        readme = (REPO_ROOT / "metrics/grafana/README.md").read_text(encoding="utf-8")

        self.assertNotIn("default_height", readme)
        self.assertNotIn("default_span", readme)

    def test_checker_shell_script_uses_uv_managed_python(self):
        shell_script = (REPO_ROOT / "scripts/check-ticdc-dashboard.sh").read_text(encoding="utf-8")
        self.assertIn('uv run python "$python_checker" "$@"', shell_script)

    def test_dashboard_module_exposes_dashboard_spec_builder_with_ordered_rows(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        baseline = load_baseline(self)
        self.assertIn("build_dashboard_spec", dashboard_globals)

        dashboard_spec = dashboard_globals["build_dashboard_spec"]()
        self.assertEqual(
            baseline["EXPECTED_ROW_TITLES"],
            [row.title for row in dashboard_spec.rows],
        )

    def test_build_dashboard_contract_returns_dict(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        baseline = load_baseline(self)
        identity = load_identity(self)
        self.assertIn("build_dashboard", dashboard_globals)

        dashboard = dashboard_globals["build_dashboard"]()
        self.assertIsInstance(dashboard, dict)
        self.assertEqual(identity["BASE_DASHBOARD_TITLE"], dashboard["title"])
        self.assertEqual(identity["BASE_DASHBOARD_UID"], dashboard["uid"])
        self.assertEqual(identity["DASHBOARD_VERSION"], dashboard["version"])
        self.assertEqual(identity["DATASOURCE_INPUT_NAME"], dashboard["__inputs"][0]["name"])
        self.assertEqual(
            baseline["EXPECTED_TEMPLATE_NAMES"],
            [item["name"] for item in dashboard["templating"]["list"]],
        )
        self.assertEqual(
            baseline["EXPECTED_ROW_TITLES"],
            [panel["title"] for panel in dashboard["panels"]],
        )
        payload = json.dumps(dashboard, ensure_ascii=False)
        self.assertIn("namespace", payload)
        self.assertIn("tidb_cluster", payload)

    def test_generator_loads_dashboard_builder_with_repo_sys_path(self):
        generator_globals = run_python_file(
            self,
            REPO_ROOT / "scripts/gen-ticdc-dashboards",
        )
        self.assertIn("load_dashboard_builder", generator_globals)

        build_dashboard = generator_globals["load_dashboard_builder"](REPO_ROOT)
        dashboard = build_dashboard()
        identity = load_identity(self)
        self.assertEqual(identity["BASE_DASHBOARD_TITLE"], dashboard["title"])

    def test_generator_writes_deterministic_json_with_trailing_newline(self):
        generator_globals = run_python_file(
            self,
            REPO_ROOT / "scripts/gen-ticdc-dashboards",
        )
        self.assertIn("write_json", generator_globals)

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
            REPO_ROOT / "scripts/gen-ticdc-dashboards",
        )
        self.assertIn("make_checksum_line", generator_globals)

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
            REPO_ROOT / "scripts/check-ticdc-dashboard.py",
        )
        self.assertIn("check_checksums", checker_globals)

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

        self.assertTrue(any("checksum mismatch" in msg.lower() for msg in messages))

    def test_checker_default_file_set_is_canonical(self):
        checker_globals = run_python_file(
            self,
            REPO_ROOT / "scripts/check-ticdc-dashboard.py",
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
            [
                "metrics/grafana/ticdc_new_arch.json.sha256",
                "metrics/grafana/ticdc_new_arch_next_gen.json.sha256",
                "metrics/grafana/ticdc_new_arch_with_keyspace_name.json.sha256",
            ],
            checker_globals["DEFAULT_CHECKSUM_FILES"],
        )

    def test_dashboard_preserves_master_explicit_graph_decimals(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )

        dashboard = dashboard_globals["build_dashboard"]()

        self.assertEqual(EXPECTED_GRAPH_DECIMALS, graph_decimals_map(dashboard))

    def test_checker_default_files_have_adjacent_checksum_files(self):
        checker_globals = run_python_file(
            self,
            REPO_ROOT / "scripts/check-ticdc-dashboard.py",
        )

        dashboard_files = checker_globals["DEFAULT_DASHBOARD_FILES"]
        checksum_files = checker_globals["DEFAULT_CHECKSUM_FILES"]

        self.assertEqual(len(dashboard_files), len(checksum_files))
        self.assertEqual(
            [f"{dashboard_file}.sha256" for dashboard_file in dashboard_files],
            checksum_files,
        )

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

    def test_dashboard_annotations_match_baseline_module(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        baseline = load_baseline(self)
        dashboard = dashboard_globals["build_dashboard"]()

        self.assertEqual(
            baseline["EXPECTED_ANNOTATION_NAMES"],
            [annotation.get("name", "") for annotation in dashboard["annotations"]["list"]],
        )

    def test_baseline_module_owns_full_templating_baseline(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        baseline = load_baseline(self)
        dashboard = dashboard_globals["build_dashboard"]()

        self.assertIn("EXPECTED_TEMPLATING", baseline)
        self.assertEqual(
            baseline["EXPECTED_TEMPLATING"],
            dashboard["templating"]["list"],
        )

    def test_dashboard_templating_matches_baseline_module(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        baseline = load_baseline(self)
        dashboard = dashboard_globals["build_dashboard"]()

        self.assertEqual(
            baseline["EXPECTED_TEMPLATING"],
            dashboard["templating"]["list"],
        )

    def test_identity_module_owns_dashboard_metadata(self):
        identity = load_identity(self)

        self.assertEqual("test-cluster-TiCDC-New-Arch", identity["BASE_DASHBOARD_TITLE"])
        self.assertEqual("YiGL8hBZ0aac", identity["BASE_DASHBOARD_UID"])
        self.assertEqual(1, identity["DASHBOARD_VERSION"])
        self.assertEqual("${DS_TEST-CLUSTER}", identity["DATASOURCE"])
        self.assertEqual("DS_TEST-CLUSTER", identity["DATASOURCE_INPUT"]["name"])
        self.assertNotIn("EXPECTED_ROW_TITLES", identity)

    def test_render_module_imports_dashboard_identity_metadata(self):
        render_source = (REPO_ROOT / "metrics/dsl/render.py").read_text(encoding="utf-8")

        self.assertIn("from metrics.dashboard_identity import", render_source)
        self.assertNotIn('DATASOURCE: Final = "${DS_TEST-CLUSTER}"', render_source)
        self.assertNotIn('"name": "DS_TEST-CLUSTER"', render_source)

    def test_dashboard_duplicate_title_panels_use_distinct_compatibility_keys(self):
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
        self.assertEqual(
            ["old_value_seek_duration_heatmap", "old_value_seek_duration_graph"],
            [
                panel.key
                for panel in rows["TiKV"].panels
                if panel.title == "Old Value Seek Duration"
            ],
        )
        self.assertEqual(
            ["producer_pending_messages", "resolved_message_count"],
            [
                panel.key
                for panel in rows["Pulsar Sink"].panels
                if panel.title == "Pulsar Client Producer Pending Messages"
            ],
        )

    def test_dashboard_panel_ids_follow_render_order(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/dashboard.py",
            REPO_ROOT,
        )
        dashboard_spec = dashboard_globals["build_dashboard_spec"]()
        dashboard = dashboard_globals["build_dashboard"]()
        ids = ordered_panel_ids(dashboard)

        self.assertEqual(
            list(range(1, len(ids) + 1)),
            ids,
        )
        self.assertEqual(len(ids), len(panel_id_map(dashboard_spec, dashboard)))

    def test_checker_cli_succeeds_from_non_repo_cwd(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                ["python3", str(REPO_ROOT / "scripts/check-ticdc-dashboard.py")],
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
