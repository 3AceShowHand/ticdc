#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import importlib
import importlib.util
import unittest


def require_module(testcase: unittest.TestCase, name: str):
    spec = importlib.util.find_spec(name)
    testcase.assertIsNotNone(spec, f"missing module: {name}")
    return importlib.import_module(name)


class DSLPrimitiveTest(unittest.TestCase):
    def test_dashboard_builders_support_additive_authoring(self):
        builders = require_module(self, "metrics.builders")

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

        rendered_graph = render.render_panel(graph_panel.build(), panel_id=1, x=0, y=0)
        rendered_heatmap = render.render_panel(heatmap_panel.build(), panel_id=2, x=0, y=0)
        rendered_table = render.render_panel(table_panel.build(), panel_id=3, x=0, y=0)

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

    def test_graph_builder_uses_explicit_histogram_queries(self):
        builders = require_module(self, "metrics.builders")
        queries = require_module(self, "metrics.queries")
        render = require_module(self, "metrics.dsl.render")

        panel = (
            builders.graph("Flush Duration", unit="s", min="0")
            .add_auto_query(
                queries.expr_histogram_quantile(
                    0.99,
                    "ticdc_sink_cloud_storage_flush_duration_seconds",
                    by_labels=["namespace", "changefeed", "instance"],
                    scope="changefeed",
                ),
                legend="{{namespace}}-{{changefeed}}-{{instance}}-p99",
            )
            .add_auto_query(
                queries.expr_histogram_avg(
                    "ticdc_sink_cloud_storage_flush_duration_seconds",
                    by_labels=["namespace", "changefeed", "instance"],
                    scope="changefeed",
                ),
                legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
            )
        )
        rendered = render.render_panel(panel.build(), panel_id=1, x=0, y=0)

        self.assertFalse(hasattr(builders.graph("Flush Duration"), "add_histogram"))
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

        panel = builders.graph("CPU", key="cpu_rate").add_query(
            "sum(rate(process_cpu_seconds_total[1m]))"
        )

        self.assertEqual("cpu_rate", panel.build().key)

    def test_row_builder_computes_explicit_line_layout(self):
        builders = require_module(self, "metrics.builders")
        render = require_module(self, "metrics.dsl.render")

        row = builders.row("Summary")
        row.add_panel(builders.graph("Full").add_query("up"))
        row.add_panels(
            builders.graph("Left").add_query("up"),
            builders.graph("Right").add_query("up"),
        )
        row.add_panels(
            builders.graph("First").add_query("up"),
            builders.graph("Second").add_query("up"),
            builders.graph("Third").add_query("up"),
        )
        row.add_panel(
            builders.graph("Half").add_query("up"),
            layout=builders.LineLayouts.HALVES,
        )

        rendered = render.render_row(row.build(), row_index=0, start_panel_id=1)

        self.assertEqual({"x": 0, "y": 0, "w": 24, "h": 7}, rendered["panels"][0]["gridPos"])
        self.assertEqual({"x": 0, "y": 7, "w": 12, "h": 7}, rendered["panels"][1]["gridPos"])
        self.assertEqual({"x": 12, "y": 7, "w": 12, "h": 7}, rendered["panels"][2]["gridPos"])
        self.assertEqual({"x": 0, "y": 14, "w": 8, "h": 7}, rendered["panels"][3]["gridPos"])
        self.assertEqual({"x": 8, "y": 14, "w": 8, "h": 7}, rendered["panels"][4]["gridPos"])
        self.assertEqual({"x": 16, "y": 14, "w": 8, "h": 7}, rendered["panels"][5]["gridPos"])
        self.assertEqual({"x": 0, "y": 21, "w": 12, "h": 7}, rendered["panels"][6]["gridPos"])

    def test_queries_expr_sum_uses_ticdc_changefeed_scope(self):
        queries = require_module(self, "metrics.queries")

        expr = queries.expr_sum(
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
        queries = require_module(self, "metrics.queries")

        built_target = queries.target(
            queries.expr_sum(
                "ticdc_scheduler_slow_table_replication_state",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            )
        )

        self.assertEqual("{{namespace}}-{{changefeed}}", built_target.legend)

    def test_queries_expr_op_preserves_legend_metadata_for_scalar_rhs(self):
        queries = require_module(self, "metrics.queries")

        built_target = queries.target(
            queries.expr_sum_rate(
                "ticdc_owner_ownership_counter",
                by_labels=["instance"],
                scope="instance",
                window="240s",
            ).op("> BOOL", "0.5")
        )

        self.assertEqual("{{instance}}", built_target.legend)

    def test_queries_expr_sum_rate_uses_explicit_scope_and_by_labels(self):
        queries = require_module(self, "metrics.queries")

        expr = queries.expr_sum_rate(
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

    def test_promql_matchers_and_aggregations_render_readably(self):
        queries = require_module(self, "metrics.queries")

        expr = queries.expr_max(
            "ticdc_owner_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="none",
            selectors=[
                queries.eq("k8s_cluster", "$k8s_cluster"),
                queries.eq("tidb_cluster", "$tidb_cluster"),
                queries.regex("namespace", "$namespace"),
                queries.regex("changefeed", "$changefeed"),
            ],
        )

        self.assertEqual(
            'max(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", namespace=~"$namespace", '
            'changefeed=~"$changefeed"}) by (namespace, changefeed)',
            str(expr),
        )

    def test_histogram_helpers_follow_tikv_style_query_patterns(self):
        queries = require_module(self, "metrics.queries")

        matchers = [
            queries.eq("k8s_cluster", "$k8s_cluster"),
            queries.eq("tidb_cluster", "$tidb_cluster"),
            queries.regex("instance", "$ticdc_instance"),
        ]

        self.assertEqual(
            "histogram_quantile(0.99, sum(rate("
            "ticdc_dispatcher_received_event_lag_duration_bucket"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            str(
                queries.expr_histogram_quantile(
                    0.99,
                    "ticdc_dispatcher_received_event_lag_duration",
                    by_labels=["instance"],
                    scope="none",
                    selectors=matchers,
                    window="1m",
                )
            ),
        )
        self.assertEqual(
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_sum"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance) / '
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_count"
            '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", '
            'instance=~"$ticdc_instance"}[1m])) by (instance)',
            str(
                queries.expr_histogram_avg(
                    "ticdc_dispatcher_received_event_lag_duration",
                    by_labels=["instance"],
                    scope="none",
                    selectors=matchers,
                    window="1m",
                )
            ),
        )

    def test_query_var_renders_expected_defaults(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        namespace_var = render.render_variable(
            api.query_var(
                "namespace",
                query="label_values(ticdc_owner_checkpoint_ts_lag, namespace)",
                multi=True,
                include_all=True,
                all_value=".*",
            )
        )
        runtime_var = render.render_variable(
            api.query_var(
                "runtime_instance",
                query=(
                    'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
                    'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
                ),
                label="Runtime metrics",
                include_all=True,
                all_value="",
            )
        )

        self.assertEqual("${DS_TEST-CLUSTER}", namespace_var["datasource"])
        self.assertTrue(namespace_var["includeAll"])
        self.assertEqual(".*", namespace_var["allValue"])
        self.assertEqual(
            {"selected": False, "text": "All", "value": "$__all"},
            runtime_var["current"],
        )
        self.assertEqual(
            "local-runtime_instance-Variable-Query",
            runtime_var["query"]["refId"],
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

        self.assertEqual({"x": 0, "y": 0, "w": 12, "h": 7}, rendered["panels"][0]["gridPos"])
        self.assertEqual({"x": 12, "y": 0, "w": 12, "h": 9}, rendered["panels"][1]["gridPos"])

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

        self.assertEqual({"x": 0, "y": 0, "w": 12, "h": 5}, rendered["panels"][0]["gridPos"])
        self.assertEqual({"x": 12, "y": 0, "w": 12, "h": 5}, rendered["panels"][1]["gridPos"])
        self.assertEqual({"x": 12, "y": 5, "w": 12, "h": 5}, rendered["panels"][2]["gridPos"])

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
