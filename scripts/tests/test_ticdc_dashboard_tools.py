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


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXPECTED_TEMPLATE_NAMES = [
    "k8s_cluster",
    "tidb_cluster",
    "namespace",
    "changefeed",
    "ticdc_instance",
    "tikv_instance",
    "spike_threshold",
    "runtime_instance",
]
EXPECTED_ROW_TITLES = [
    "Summary",
    "Lag Summary",
    "Dataflow",
    "Server",
    "Changefeed",
    "Lag analyze",
    "Coordinator",
    "Maintainer",
    "Log Puller",
    "Event Store",
    "Schema Store",
    "Event Service",
    "Message Center",
    "Dispatcher",
    "Dynamic Stream",
    "Sink - General",
    "Sink - Transaction Sink",
    "Sink - MQ Sink",
    "Sink - Cloud Storage Sink",
    "Scheduler",
    "TiKV",
    "Active Active",
    "Redo",
    "Runtime $runtime_instance",
    "DDL",
    "Pulsar Sink",
]


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


class DashboardToolsTest(unittest.TestCase):
    def test_dashboard_module_exposes_dashboard_spec_builder_with_ordered_rows(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/grafana/ticdc_new_arch.dashboard.py",
            REPO_ROOT / "metrics/grafana",
        )
        self.assertIn("build_dashboard_spec", dashboard_globals)

        dashboard_spec = dashboard_globals["build_dashboard_spec"]()
        self.assertEqual(
            EXPECTED_ROW_TITLES,
            [row.title for row in dashboard_spec.rows],
        )

    def test_build_dashboard_contract_returns_dict(self):
        dashboard_globals = run_python_file(
            self,
            REPO_ROOT / "metrics/grafana/ticdc_new_arch.dashboard.py",
            REPO_ROOT / "metrics/grafana",
        )
        self.assertIn("build_dashboard", dashboard_globals)

        dashboard = dashboard_globals["build_dashboard"]()
        self.assertIsInstance(dashboard, dict)
        self.assertEqual("test-cluster-TiCDC-New-Arch", dashboard["title"])
        self.assertEqual("YiGL8hBZ0aac", dashboard["uid"])
        self.assertEqual("DS_TEST-CLUSTER", dashboard["__inputs"][0]["name"])
        self.assertEqual(
            EXPECTED_TEMPLATE_NAMES,
            [item["name"] for item in dashboard["templating"]["list"]],
        )
        self.assertEqual(
            EXPECTED_ROW_TITLES,
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
        self.assertEqual("test-cluster-TiCDC-New-Arch", dashboard["title"])

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
        self.assertEqual('{\n  "title": "x",\n  "panels": [],\n  "templating": {\n    "list": []\n  }\n}\n', first)

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
            sha.write_text(("0" * 64) + "  metrics/grafana/ticdc_new_arch.json\n", encoding="utf-8")
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
                "metrics/nextgengrafana/ticdc_new_arch_next_gen.json",
                "metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json",
            ],
            checker_globals["DEFAULT_DASHBOARD_FILES"],
        )
        self.assertEqual(
            [
                "metrics/grafana/ticdc_new_arch.json.sha256",
                "metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256",
                "metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256",
            ],
            checker_globals["DEFAULT_CHECKSUM_FILES"],
        )

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
