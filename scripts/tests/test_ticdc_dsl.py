#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import importlib
import importlib.util
import pathlib
import typing
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def require_module(testcase: unittest.TestCase, name: str):
    spec = importlib.util.find_spec(name)
    testcase.assertIsNotNone(spec, f"missing module: {name}")
    return importlib.import_module(name)


class DSLPrimitiveTest(unittest.TestCase):
    def test_dashboard_builders_support_additive_authoring(self):
        builders = require_module(self, "metrics.builders")
        specs = require_module(self, "metrics.dsl.specs")

        dashboard = builders.dashboard(
            title="Example",
            uid="example",
            variables=[],
            annotations=[],
        )
        summary = builders.row("Summary")
        cpu = builders.graph("CPU", unit="percentunit")
        memory = builders.graph("Memory", unit="bytes")

        self.assertIs(
            cpu,
            cpu.add_query(
                "sum(rate(process_cpu_seconds_total[1m]))",
                legend="{{instance}}",
            ),
        )
        self.assertIs(memory, memory.add_query("process_resident_memory_bytes"))
        self.assertIs(summary, summary.add_panels(cpu, memory))
        self.assertIs(dashboard, dashboard.add_row(summary))

        spec = dashboard.build()
        self.assertEqual(["Summary"], [row.title for row in spec.rows])
        self.assertEqual(
            ["CPU", "Memory"],
            [panel.title for panel in spec.rows[0].panels],
        )
        self.assertEqual([12, 12], [panel.span for panel in spec.rows[0].panels])
        self.assertIs(
            specs.DashboardSpec,
            typing.get_type_hints(type(dashboard).build)["return"],
        )

    def test_dashboard_builder_annotations_return_dashboard_spec(self):
        builders = require_module(self, "metrics.builders")
        dashboard_module = require_module(self, "metrics.dashboard")
        specs = require_module(self, "metrics.dsl.specs")

        self.assertIs(
            specs.DashboardSpec,
            typing.get_type_hints(builders.DashboardBuilder.build)["return"],
        )
        self.assertIs(
            specs.DashboardSpec,
            typing.get_type_hints(dashboard_module.build_dashboard_spec)["return"],
        )

    def test_panel_builders_apply_query_refs_and_panel_type_defaults(self):
        builders = require_module(self, "metrics.builders")
        render = require_module(self, "metrics.dsl.render")

        graph_panel = (
            builders.graph("CPU", unit="percentunit")
            .add_query("query_a")
            .add_auto_query("query_b")
            .add_range_query("query_c")
        )

        heatmap_panel = builders.heatmap("Lag").add_query("query_c")
        table_panel = builders.table("Build Info").add_label_query(
            "ticdc_server_build_info",
            columns=["instance", "git_hash"],
        )

        rendered_graph = render.render_panel(
            graph_panel.build(),
            panel_id=1,
            x=0,
            y=0,
        )
        rendered_heatmap = render.render_panel(
            heatmap_panel.build(),
            panel_id=2,
            x=0,
            y=0,
        )
        rendered_table = render.render_panel(
            table_panel.build(),
            panel_id=3,
            x=0,
            y=0,
        )

        self.assertEqual("A", rendered_graph["targets"][0]["refId"])
        self.assertEqual("B", rendered_graph["targets"][1]["refId"])
        self.assertEqual("C", rendered_graph["targets"][2]["refId"])
        self.assertEqual("time_series", rendered_graph["targets"][0]["format"])
        self.assertIsNone(rendered_graph["targets"][1].get("format"))
        self.assertFalse(rendered_graph["targets"][2]["instant"])
        self.assertEqual("heatmap", rendered_heatmap["targets"][0]["format"])
        self.assertIsNone(rendered_heatmap["targets"][0].get("instant"))
        self.assertEqual("time_series", rendered_table["targets"][0]["format"])
        self.assertTrue(rendered_table["targets"][0]["instant"])
        self.assertEqual(
            ["labelsToFields", "organize"],
            [item["id"] for item in rendered_table["transformations"]],
        )

    def test_graph_builder_supports_histogram_shortcuts(self):
        builders = require_module(self, "metrics.builders")
        render = require_module(self, "metrics.dsl.render")

        panel = builders.graph("Flush Duration", unit="s", min="0").add_histogram(
            "ticdc_sink_cloud_storage_flush_duration_seconds",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        )

        rendered = render.render_panel(panel.build(), panel_id=1, x=0, y=0)

        self.assertEqual(2, len(rendered["targets"]))
        self.assertEqual(
            "{{namespace}}-{{changefeed}}-{{instance}}-p99",
            rendered["targets"][0]["legendFormat"],
        )
        self.assertEqual(
            "{{namespace}}-{{changefeed}}-{{instance}}-avg",
            rendered["targets"][1]["legendFormat"],
        )
        self.assertIsNone(rendered["targets"][0].get("format"))
        self.assertIsNone(rendered["targets"][1].get("format"))

    def test_panel_builders_preserve_explicit_compatibility_keys(self):
        builders = require_module(self, "metrics.builders")
        queries = require_module(self, "metrics.queries")

        panel = builders.graph("CPU", key="cpu_rate").add_query(
            "sum(rate(process_cpu_seconds_total[1m]))"
        )

        self.assertEqual("cpu_rate", panel.build().key)
        self.assertEqual(
            "cpu_rate",
            queries.graph_panel(
                title="CPU",
                key="cpu_rate",
                targets=[queries.target("sum(rate(process_cpu_seconds_total[1m]))")],
            ).key,
        )

    def test_row_builder_computes_explicit_line_layout(self):
        builders = require_module(self, "metrics.builders")
        render = require_module(self, "metrics.dsl.render")

        self.assertFalse(hasattr(builders.RowHeights, "COMPACT"))
        self.assertEqual(7, int(builders.RowHeights.NORMAL))

        row = builders.row("Summary")

        full = builders.graph("Full").add_query("up")
        row.add_panel(full)

        left = builders.graph("Left").add_query("up")
        right = builders.graph("Right").add_query("up")
        row.add_panels(left, right)

        first = builders.graph("First").add_query("up")
        second = builders.graph("Second").add_query("up")
        third = builders.graph("Third").add_query("up")
        row.add_panels(first, second, third)

        half = builders.graph("Half").add_query("up")
        row.add_panel(half, layout=builders.LineLayouts.HALVES)

        rendered = render.render_row(row.build(), row_index=0, start_panel_id=1)

        self.assertEqual(
            {"x": 0, "y": 0, "w": 24, "h": 7},
            rendered["panels"][0]["gridPos"],
        )
        self.assertEqual(
            {"x": 0, "y": 7, "w": 12, "h": 7},
            rendered["panels"][1]["gridPos"],
        )
        self.assertEqual(
            {"x": 12, "y": 7, "w": 12, "h": 7},
            rendered["panels"][2]["gridPos"],
        )
        self.assertEqual(
            {"x": 0, "y": 14, "w": 8, "h": 7},
            rendered["panels"][3]["gridPos"],
        )
        self.assertEqual(
            {"x": 8, "y": 14, "w": 8, "h": 7},
            rendered["panels"][4]["gridPos"],
        )
        self.assertEqual(
            {"x": 16, "y": 14, "w": 8, "h": 7},
            rendered["panels"][5]["gridPos"],
        )
        self.assertEqual(
            {"x": 0, "y": 21, "w": 12, "h": 7},
            rendered["panels"][6]["gridPos"],
        )

    def test_row_builder_rejects_per_line_height_overrides(self):
        builders = require_module(self, "metrics.builders")

        row = builders.row("Summary")
        left = builders.graph("Left").add_query("up")
        right = builders.graph("Right").add_query("up")

        with self.assertRaises(TypeError):
            row.add_panels(left, right, height=builders.RowHeights.NORMAL)

    def test_flat_layout_modules_are_importable(self):
        require_module(self, "metrics.builders")
        require_module(self, "metrics.dashboard")
        require_module(self, "metrics.dashboard_identity")
        require_module(self, "metrics.dashboard_baseline")
        require_module(self, "metrics.queries")
        require_module(self, "metrics.templating")
        require_module(self, "metrics.annotations")
        require_module(self, "metrics.rows.summary")
        require_module(self, "metrics.rows.changefeed")

        self.assertIsNone(importlib.util.find_spec("metrics.compatibility"))

    def test_dashboard_identity_and_baseline_modules_have_clear_boundaries(self):
        identity = require_module(self, "metrics.dashboard_identity")
        baseline = require_module(self, "metrics.dashboard_baseline")

        self.assertTrue(hasattr(identity, "BASE_DASHBOARD_TITLE"))
        self.assertTrue(hasattr(identity, "BASE_DASHBOARD_UID"))
        self.assertTrue(hasattr(identity, "DASHBOARD_VERSION"))
        self.assertTrue(hasattr(identity, "DATASOURCE"))
        self.assertTrue(hasattr(identity, "DATASOURCE_INPUT"))
        self.assertTrue(hasattr(identity, "DATASOURCE_INPUT_NAME"))
        self.assertFalse(hasattr(identity, "EXPECTED_ROW_TITLES"))
        self.assertFalse(hasattr(identity, "EXPECTED_TEMPLATE_NAMES"))

        self.assertTrue(hasattr(baseline, "EXPECTED_ROW_TITLES"))
        self.assertTrue(hasattr(baseline, "EXPECTED_TEMPLATE_NAMES"))
        self.assertTrue(hasattr(baseline, "EXPECTED_ANNOTATION_NAMES"))
        self.assertTrue(hasattr(baseline, "EXPECTED_TEMPLATING"))
        self.assertTrue(hasattr(baseline, "validate_dashboard_identity"))
        self.assertTrue(hasattr(baseline, "validate_dashboard_compatibility"))
        self.assertFalse(hasattr(baseline, "BASE_DASHBOARD_TITLE"))
        self.assertFalse(hasattr(baseline, "DATASOURCE"))

    def test_queries_module_does_not_expose_query_defaults_context_api(self):
        common = require_module(self, "metrics.queries")

        self.assertFalse(hasattr(common, "query_defaults"))

    def test_queries_module_does_not_expose_dashboard_identity_or_builder_api(self):
        common = require_module(self, "metrics.queries")

        self.assertFalse(hasattr(common, "BASE_DASHBOARD_TITLE"))
        self.assertFalse(hasattr(common, "BASE_DASHBOARD_UID"))
        self.assertFalse(hasattr(common, "DATASOURCE"))
        self.assertFalse(hasattr(common, "DATASOURCE_INPUT"))
        self.assertFalse(hasattr(common, "DATASOURCE_INPUT_NAME"))
        self.assertFalse(hasattr(common, "EXPECTED_ROW_TITLES"))
        self.assertFalse(hasattr(common, "EXPECTED_TEMPLATE_NAMES"))
        self.assertFalse(hasattr(common, "validate_dashboard_identity"))
        self.assertFalse(hasattr(common, "custom_var"))
        self.assertFalse(hasattr(common, "graph"))
        self.assertFalse(hasattr(common, "heatmap"))
        self.assertFalse(hasattr(common, "query_var"))
        self.assertFalse(hasattr(common, "row"))
        self.assertFalse(hasattr(common, "table"))
        self.assertFalse(hasattr(common, "timeseries"))

    def test_builders_module_no_longer_exposes_timeseries_builder(self):
        builders = require_module(self, "metrics.builders")

        self.assertFalse(hasattr(builders, "timeseries"))

    def test_builder_factories_reject_removed_legacy_layout_kwargs(self):
        builders = require_module(self, "metrics.builders")

        with self.assertRaises(TypeError):
            builders.row("Summary", default_height=7)
        with self.assertRaises(TypeError):
            builders.graph("CPU", width=12)
        with self.assertRaises(TypeError):
            builders.table("Errors", height=8)

    def test_queries_expr_sum_uses_ticdc_changefeed_scope(self):
        common = require_module(self, "metrics.queries")

        expr = common.expr_sum(
            "ticdc_scheduler_slow_table_replication_state",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        )

        self.assertEqual(
            "sum(ticdc_scheduler_slow_table_replication_state"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance", namespace=~"$namespace", '
            'changefeed=~"$changefeed"}) by (namespace, changefeed)',
            str(expr),
        )

    def test_queries_target_infers_legend_from_expr_by_labels(self):
        common = require_module(self, "metrics.queries")

        target = common.target(
            common.expr_sum(
                "ticdc_scheduler_slow_table_replication_state",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            )
        )

        self.assertEqual("{{namespace}}-{{changefeed}}", target.legend)

    def test_queries_expr_sum_rate_uses_explicit_scope_and_by_labels(self):
        common = require_module(self, "metrics.queries")

        expr = common.expr_sum_rate(
            "ticdc_kvclient_pull_event_count",
            scope="instance",
            by_labels=["instance", "type"],
        )

        self.assertEqual(
            "sum(rate(ticdc_kvclient_pull_event_count"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance, type)',
            str(expr),
        )

    def test_queries_graph_panel_applies_time_series_target_defaults(self):
        common = require_module(self, "metrics.queries")
        render = require_module(self, "metrics.dsl.render")

        panel = common.graph_panel(
            title="Slowest Table Replication State",
            unit="none",
            min="0",
            targets=[
                common.target(
                    expr=common.expr_sum(
                        "ticdc_scheduler_slow_table_replication_state",
                        by_labels=["namespace", "changefeed"],
                        scope="changefeed",
                    ),
                    legend_format="{{namespace}}-{{changefeed}}",
                )
            ],
        )
        rendered = render.render_panel(panel, panel_id=1, x=0, y=0)

        self.assertEqual("graph", rendered["type"])
        self.assertEqual("time_series", rendered["targets"][0]["format"])
        self.assertEqual(
            "{{namespace}}-{{changefeed}}",
            rendered["targets"][0]["legendFormat"],
        )
        self.assertEqual("none", rendered["yaxes"][0]["format"])
        self.assertEqual("0", rendered["yaxes"][0]["min"])

    def test_queries_module_does_not_expose_legacy_layout_api(self):
        common = require_module(self, "metrics.queries")

        self.assertFalse(hasattr(common, "Layout"))

    def test_promql_helper_normalizes_indentation(self):
        helpers = require_module(self, "metrics.dsl.promql")

        self.assertEqual(
            "sum(rate(ticdc_sink_dml_event_count[1m]))",
            helpers.promql(
                """
                sum(rate(ticdc_sink_dml_event_count[1m]))
                """
            ),
        )

    def test_target_preset_helpers_apply_expected_defaults(self):
        helpers = require_module(self, "metrics.dsl.promql")

        self.assertEqual(
            (
                "sum(rate(ticdc_sink_dml_event_count[1m]))",
                "{{instance}}",
                "time_series",
                None,
            ),
            (
                helpers.series_query(
                    """
                    sum(rate(ticdc_sink_dml_event_count[1m]))
                    """,
                    legend="{{instance}}",
                ).expr,
                helpers.series_query(
                    "sum(rate(ticdc_sink_dml_event_count[1m]))",
                    legend="{{instance}}",
                ).legend,
                helpers.series_query("sum(rate(ticdc_sink_dml_event_count[1m]))").format,
                helpers.series_query("sum(rate(ticdc_sink_dml_event_count[1m]))").instant,
            ),
        )
        self.assertEqual(
            ("time_series", True),
            (
                helpers.instant_query("up").format,
                helpers.instant_query("up").instant,
            ),
        )
        self.assertIsNone(helpers.series_query("up", format=None).format)
        self.assertEqual(
            ("heatmap", False, "{{le}}"),
            (
                helpers.heatmap_query("sum(rate(metric_bucket[1m])) by (le)").format,
                helpers.heatmap_query("sum(rate(metric_bucket[1m])) by (le)").instant,
                helpers.heatmap_query("sum(rate(metric_bucket[1m])) by (le)").legend,
            ),
        )

    def test_promql_matchers_and_aggregations_render_readably(self):
        helpers = require_module(self, "metrics.dsl.promql")

        matchers = [
            helpers.eq("k8s_cluster", "$k8s_cluster"),
            helpers.eq("tidb_cluster", "$tidb_cluster"),
            helpers.regex("namespace", "$namespace"),
            helpers.regex("changefeed", "$changefeed"),
        ]
        expr = helpers.max_by(
            helpers.selector("ticdc_owner_checkpoint_ts_lag", *matchers),
            "namespace",
            "changefeed",
        )

        self.assertEqual(
            'max(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", namespace=~"$namespace", '
            'changefeed=~"$changefeed"}) by (namespace, changefeed)',
            expr,
        )

    def test_histogram_helpers_follow_tikv_style_query_patterns(self):
        helpers = require_module(self, "metrics.dsl.promql")

        matchers = [
            helpers.eq("k8s_cluster", "$k8s_cluster"),
            helpers.eq("tidb_cluster", "$tidb_cluster"),
            helpers.regex("instance", "$ticdc_instance"),
        ]

        self.assertEqual(
            "histogram_quantile(0.99, sum(rate("
            "ticdc_dispatcher_received_event_lag_duration_bucket"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            helpers.histogram_quantile_rate(
                "ticdc_dispatcher_received_event_lag_duration",
                quantile=0.99,
                matchers=matchers,
                by=["instance"],
                window="1m",
            ),
        )
        self.assertEqual(
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_sum"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance) / '
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_count"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance)',
            helpers.histogram_average_rate(
                "ticdc_dispatcher_received_event_lag_duration",
                matchers=matchers,
                by=["instance"],
                window="1m",
            ),
        )

    def test_histogram_panel_presets_build_heatmap_and_quantile_graph(self):
        panels = require_module(self, "metrics.dsl.presets")
        render = require_module(self, "metrics.dsl.render")
        helpers = require_module(self, "metrics.dsl.promql")

        built = panels.histogram_panel_pair(
            heatmap_title="EventCollector Resolved Ts Lag",
            graph_title="Region Request Finish Scan Duration",
            metric="ticdc_dispatcher_received_event_lag_duration",
            matchers=[
                helpers.eq("k8s_cluster", "$k8s_cluster"),
                helpers.eq("tidb_cluster", "$tidb_cluster"),
                helpers.regex("instance", "$ticdc_instance"),
            ],
            by=["instance"],
            unit="ms",
            width=12,
            height=6,
            quantile_legend="{{instance}}-p99",
            average_legend="{{instance}}-avg",
        )

        self.assertEqual(2, len(built))
        rendered_heatmap = render.render_panel(built[0], panel_id=1, x=0, y=0)
        rendered_graph = render.render_panel(built[1], panel_id=2, x=12, y=0)

        self.assertEqual("heatmap", rendered_heatmap["type"])
        self.assertEqual("graph", rendered_graph["type"])
        self.assertEqual("heatmap", rendered_heatmap["targets"][0]["format"])
        self.assertEqual(
            "{{instance}}-p99",
            rendered_graph["targets"][0]["legendFormat"],
        )
        self.assertEqual(
            "{{instance}}-avg",
            rendered_graph["targets"][1]["legendFormat"],
        )
        self.assertEqual(
            "histogram_quantile(0.99, sum(rate("
            "ticdc_dispatcher_received_event_lag_duration_bucket"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            rendered_graph["targets"][0]["expr"],
        )
        self.assertEqual(
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_sum"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance) / '
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_count"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance)',
            rendered_graph["targets"][1]["expr"],
        )

    def test_dsl_does_not_expose_ticdc_specific_business_wrappers(self):
        dsl = require_module(self, "metrics.dsl")

        self.assertFalse(hasattr(dsl, "changefeed_graph"))
        self.assertFalse(hasattr(dsl, "instance_graph"))
        self.assertFalse(hasattr(dsl, "metric_graph"))
        self.assertIsNone(importlib.util.find_spec("metrics.dsl.ticdc"))

    def test_legacy_nested_grafana_sources_are_removed(self):
        self.assertFalse((REPO_ROOT / "metrics/grafana/common.py").exists())
        self.assertFalse((REPO_ROOT / "metrics/grafana/ticdc_new_arch.dashboard.py").exists())
        self.assertFalse((REPO_ROOT / "metrics/grafana/ticdc_new_arch").exists())
        self.assertFalse((REPO_ROOT / "metrics/nextgengrafana").exists())

    def test_legend_builder_matches_grouped_series_labels(self):
        helpers = require_module(self, "metrics.dsl.promql")

        self.assertEqual(
            "{{namespace}}-{{changefeed}}-{{instance}}",
            helpers.legend_for("namespace", "changefeed", "instance"),
        )
        self.assertEqual(
            "p99-{{instance}}",
            helpers.legend_for("instance", prefix="p99"),
        )

    def test_query_alias_matches_target(self):
        api = require_module(self, "metrics.dsl.api")

        self.assertEqual(
            api.target(
                "sum(rate(process_cpu_seconds_total[1m]))",
                legend="{{instance}}",
                ref="B",
                hide=True,
                format="time_series",
                instant=False,
            ),
            api.query(
                "sum(rate(process_cpu_seconds_total[1m]))",
                legend="{{instance}}",
                ref="B",
                hide=True,
                format="time_series",
                instant=False,
            ),
        )

    def test_query_var_renders_default_datasource_and_all_value(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.query_var(
            "namespace",
            query="label_values(ticdc_owner_checkpoint_ts_lag, namespace)",
            multi=True,
            include_all=True,
            all_value=".*",
        )
        rendered = render.render_variable(spec)
        self.assertEqual("query", rendered["type"])
        self.assertEqual("${DS_TEST-CLUSTER}", rendered["datasource"])
        self.assertTrue(rendered["includeAll"])
        self.assertEqual(".*", rendered["allValue"])

    def test_query_var_renders_master_compatible_single_select_defaults(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.query_var(
            "k8s_cluster",
            query="label_values(go_goroutines, k8s_cluster)",
            label="K8s-cluster",
            sort=1,
        )
        rendered = render.render_variable(spec)

        self.assertEqual(
            {"isNone": True, "selected": False, "text": "None", "value": ""},
            rendered["current"],
        )
        self.assertEqual("", rendered["definition"])
        self.assertEqual(
            "local-k8s_cluster-Variable-Query",
            rendered["query"]["refId"],
        )
        self.assertEqual("K8s-cluster", rendered["label"])
        self.assertEqual(1, rendered["sort"])
        self.assertFalse(rendered["skipUrlSync"])
        self.assertEqual([], rendered["tags"])
        self.assertEqual("", rendered["tagValuesQuery"])
        self.assertEqual("", rendered["tagsQuery"])
        self.assertFalse(rendered["useTags"])

    def test_query_var_with_include_all_uses_query_definition(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.query_var(
            "runtime_instance",
            query=(
                'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
            ),
            label="Runtime metrics",
            include_all=True,
            all_value="",
        )
        rendered = render.render_variable(spec)

        self.assertEqual(
            {"selected": False, "text": "All", "value": "$__all"},
            rendered["current"],
        )
        self.assertEqual(spec.query, rendered["definition"])
        self.assertEqual(
            "local-runtime_instance-Variable-Query",
            rendered["query"]["refId"],
        )

    def test_graph_renders_default_tooltip_and_axis_defaults(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.graph(
            "Checkpoint Lag",
            targets=[api.target("max(ticdc_owner_checkpoint_ts_lag)")],
            unit="s",
            min=0,
        )
        rendered = render.render_graph_panel(spec, panel_id=1)
        self.assertEqual("graph", rendered["type"])
        self.assertEqual(
            {"shared": True, "sort": 0, "value_type": "individual"},
            rendered["tooltip"],
        )
        self.assertEqual("s", rendered["yaxes"][0]["format"])
        self.assertEqual("0", rendered["yaxes"][0]["min"])

    def test_row_layout_assigns_grid_positions_from_span_and_height(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.row(
            "Summary",
            [
                api.graph("A", targets=[api.target("up")], span=12, height=7),
                api.graph("B", targets=[api.target("up")], span=12, height=9),
            ],
        )
        rendered = render.render_row(spec, row_index=0, start_panel_id=100)
        panels = rendered["panels"]
        self.assertEqual({"x": 0, "y": 0, "w": 12, "h": 7}, panels[0]["gridPos"])
        self.assertEqual({"x": 12, "y": 0, "w": 12, "h": 9}, panels[1]["gridPos"])

    def test_width_alias_maps_to_grafana_span(self):
        api = require_module(self, "metrics.dsl.api")

        spec = api.graph(
            "Checkpoint Lag",
            targets=[api.query("max(ticdc_owner_checkpoint_ts_lag)")],
            width=8,
            height=6,
        )
        self.assertEqual(8, spec.span)
        self.assertEqual(6, spec.height)

    def test_dsl_api_no_longer_exposes_timeseries_panel(self):
        api = require_module(self, "metrics.dsl.api")

        self.assertFalse(hasattr(api, "timeseries"))

    def test_row_layout_supports_explicit_x_gaps(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.row(
            "Gap",
            [
                api.graph("A", targets=[api.target("up")], span=12, height=5),
                api.graph("B", targets=[api.target("up")], span=12, height=5),
                api.graph("C", targets=[api.target("up")], span=12, height=5, x=12),
            ],
        )
        rendered = render.render_row(spec, row_index=0, start_panel_id=1)
        panels = rendered["panels"]
        self.assertEqual({"x": 0, "y": 0, "w": 12, "h": 5}, panels[0]["gridPos"])
        self.assertEqual({"x": 12, "y": 0, "w": 12, "h": 5}, panels[1]["gridPos"])
        self.assertEqual({"x": 12, "y": 5, "w": 12, "h": 5}, panels[2]["gridPos"])

    def test_width_and_span_conflict_is_rejected(self):
        api = require_module(self, "metrics.dsl.api")

        with self.assertRaises(ValueError):
            api.graph(
                "Checkpoint Lag",
                targets=[api.target("up")],
                span=12,
                width=8,
            )


if __name__ == "__main__":
    unittest.main()
