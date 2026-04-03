#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import unittest
from typing import Any, cast

from metrics.builders import graph, row
from metrics.dsl.api import dashboard, target
from metrics.dsl.api import graph as graph_spec
from metrics.dsl.api import row as row_spec
from metrics.dsl.render import render_dashboard
from metrics.panel_ids import (
    PanelIdRegistry,
    build_panel_id_resolver,
    sync_panel_ids,
)


def build_dashboard_spec(*panels):
    return dashboard(
        title="Example",
        uid="example",
        variables=[],
        rows=[
            row_spec(
                "Example Row",
                list(panels),
                key="example_row",
            )
        ],
    )


def build_inferred_row(panel_title: str):
    row_builder = row("Visible Row")
    primary_panel = graph(panel_title).add_query("up")
    row_builder.add_panel(primary_panel)
    return row_builder.build()


def rendered_panel_ids(dashboard_json: dict[str, object]) -> list[int]:
    dashboard_panels = cast("list[dict[str, Any]]", dashboard_json["panels"])
    row_panels = cast("list[dict[str, Any]]", dashboard_panels[0]["panels"])
    return [panel["id"] for panel in row_panels]


class PanelIdRegistryTest(unittest.TestCase):
    def test_builder_infers_stable_row_and_panel_keys_from_authoring_structure(self):
        row_spec = build_inferred_row("Visible Panel")

        self.assertEqual("inferred", row_spec.key)
        self.assertEqual("primary_panel", row_spec.panels[0].key)

    def test_duplicate_panel_identity_is_rejected(self):
        duplicate_spec = build_dashboard_spec(
            graph_spec("A", targets=[target("up")], key="panel_a"),
            graph_spec("B", targets=[target("up")], key="panel_a"),
        )

        with self.assertRaisesRegex(ValueError, "duplicate panel identity"):
            sync_panel_ids(duplicate_spec, PanelIdRegistry.empty())

    def test_existing_panel_ids_stay_fixed_when_inserting_new_panel(self):
        base_spec = build_dashboard_spec(
            graph_spec("A", targets=[target("up")], key="panel_a"),
            graph_spec("B", targets=[target("up")], key="panel_b"),
        )
        registry = sync_panel_ids(base_spec, PanelIdRegistry.empty())
        dashboard_json = render_dashboard(
            base_spec,
            panel_id_resolver=build_panel_id_resolver(registry),
        )

        self.assertEqual([1, 2], rendered_panel_ids(dashboard_json))

        inserted_spec = build_dashboard_spec(
            graph_spec("A", targets=[target("up")], key="panel_a"),
            graph_spec("Inserted", targets=[target("up")], key="panel_inserted"),
            graph_spec("B", targets=[target("up")], key="panel_b"),
        )
        registry = sync_panel_ids(inserted_spec, registry)
        inserted_dashboard = render_dashboard(
            inserted_spec,
            panel_id_resolver=build_panel_id_resolver(registry),
        )

        self.assertEqual(
            [1, 3, 2],
            rendered_panel_ids(inserted_dashboard),
        )
        self.assertEqual(4, registry.next_id)

    def test_deleted_panel_keeps_its_old_id_reserved(self):
        original_spec = build_dashboard_spec(
            graph_spec("A", targets=[target("up")], key="panel_a"),
            graph_spec("Inserted", targets=[target("up")], key="panel_inserted"),
            graph_spec("B", targets=[target("up")], key="panel_b"),
        )
        registry = sync_panel_ids(original_spec, PanelIdRegistry.empty())

        deleted_spec = build_dashboard_spec(
            graph_spec("A", targets=[target("up")], key="panel_a"),
            graph_spec("B", targets=[target("up")], key="panel_b"),
        )
        registry = sync_panel_ids(deleted_spec, registry)
        deleted_dashboard = render_dashboard(
            deleted_spec,
            panel_id_resolver=build_panel_id_resolver(registry),
        )

        self.assertEqual(
            [1, 3],
            rendered_panel_ids(deleted_dashboard),
        )
        self.assertEqual(4, registry.next_id)

        restored_registry = sync_panel_ids(original_spec, registry)
        restored_dashboard = render_dashboard(
            original_spec,
            panel_id_resolver=build_panel_id_resolver(restored_registry),
        )

        self.assertEqual(
            [1, 2, 3],
            rendered_panel_ids(restored_dashboard),
        )

    def test_title_changes_do_not_change_panel_ids_when_inferred_keys_stay_the_same(self):
        original_spec = dashboard(
            title="Example",
            uid="example",
            variables=[],
            rows=[build_inferred_row("Before Rename")],
        )
        registry = sync_panel_ids(original_spec, PanelIdRegistry.empty())

        renamed_spec = dashboard(
            title="Example",
            uid="example",
            variables=[],
            rows=[build_inferred_row("After Rename")],
        )
        registry = sync_panel_ids(renamed_spec, registry)
        renamed_dashboard = render_dashboard(
            renamed_spec,
            panel_id_resolver=build_panel_id_resolver(registry),
        )

        self.assertEqual([1], rendered_panel_ids(renamed_dashboard))


if __name__ == "__main__":
    unittest.main()
