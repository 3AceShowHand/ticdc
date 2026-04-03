#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import importlib
import importlib.util
import json
import pathlib
import unittest

from metrics.dsl.render import render_row

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
REFERENCE_DASHBOARD = json.loads(
    (REPO_ROOT / "metrics/grafana/ticdc_new_arch.json").read_text(encoding="utf-8")
)


def require_module(testcase: unittest.TestCase, name: str):
    spec = importlib.util.find_spec(name)
    testcase.assertIsNotNone(spec, f"missing module: {name}")
    return importlib.import_module(name)


def row_by_title(dashboard: dict, title: str) -> dict:
    for row in dashboard["panels"]:
        if row["title"] == title:
            return row
    raise AssertionError(f"missing row {title}")


def panel_unit(panel: dict) -> str | None:
    if panel["type"] == "heatmap":
        return panel.get("yAxis", {}).get("format")
    if "yaxes" in panel:
        return panel["yaxes"][0].get("format")
    return None


def panel_min(panel: dict):
    if panel["type"] == "heatmap":
        return panel.get("yAxis", {}).get("min")
    if "yaxes" in panel:
        return panel["yaxes"][0].get("min")
    return None


def panel_decimals(panel: dict):
    if panel["type"] == "heatmap":
        return panel.get("yAxis", {}).get("decimals")
    if "yaxes" in panel:
        return panel["yaxes"][0].get("decimals")
    return None


def normalize_expr(expr: str | None) -> str | None:
    if expr is None:
        return None
    return "".join(expr.split())


def normalize_row(row: dict) -> dict:
    panels = row.get("panels", [])
    min_y = min((panel["gridPos"]["y"] for panel in panels), default=0)
    return {
        "title": row["title"],
        "panels": [
            {
                "type": panel["type"],
                "title": panel["title"],
                "gridPos": {
                    "x": panel["gridPos"]["x"],
                    "y": panel["gridPos"]["y"] - min_y,
                    "w": panel["gridPos"]["w"],
                    "h": panel["gridPos"]["h"],
                },
                "description": panel.get("description"),
                "unit": panel_unit(panel),
                "min": panel_min(panel),
                "decimals": panel_decimals(panel),
                "targets": [
                    {
                        "expr": normalize_expr(target.get("expr")),
                        "legendFormat": target.get("legendFormat"),
                        "refId": target.get("refId"),
                        "hide": target.get("hide", False),
                        "format": target.get("format"),
                        "instant": target.get("instant"),
                    }
                    for target in panel.get("targets", [])
                ],
                "transformations": panel.get("transformations"),
            }
            for panel in panels
        ],
    }


def render_named_row(row_spec) -> dict:
    return render_row(row_spec, row_index=0, start_panel_id=1)


class RowRenderTest(unittest.TestCase):
    def test_row_modules_match_reference_after_normalization(self):
        dashboard = require_module(self, "metrics.dashboard")
        for build_row in dashboard.ROW_BUILDERS:
            row_spec = build_row()
            self.assertEqual(
                normalize_row(row_by_title(REFERENCE_DASHBOARD, row_spec.title)),
                normalize_row(render_named_row(row_spec)),
                row_spec.title,
            )


if __name__ == "__main__":
    unittest.main()
