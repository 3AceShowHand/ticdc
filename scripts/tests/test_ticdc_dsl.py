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
        builders = require_module(self, "metrics.grafana.ticdc_new_arch.builders")

        dashboard = builders.dashboard(
            title="Example",
            uid="example",
            variables=[],
            annotations=[],
        )
        summary = builders.row("Summary", default_height=6, default_span=12)
        cpu = builders.graph("CPU", unit="percentunit")

        self.assertIs(cpu, cpu.add_query("sum(rate(process_cpu_seconds_total[1m]))", legend="{{instance}}"))
        self.assertIs(summary, summary.add_graph(cpu))
        self.assertIs(dashboard, dashboard.add_row(summary))

        spec = dashboard.build()
        self.assertEqual(["Summary"], [row.title for row in spec.rows])
        self.assertEqual(["CPU"], [panel.title for panel in spec.rows[0].panels])

    def test_panel_builders_apply_query_refs_and_panel_type_defaults(self):
        builders = require_module(self, "metrics.grafana.ticdc_new_arch.builders")
        render = require_module(self, "metrics.dsl.render")

        graph_panel = builders.graph("CPU", unit="percentunit")
        graph_panel.add_query("query_a")
        graph_panel.add_query("query_b")

        heatmap_panel = builders.heatmap("Lag")
        heatmap_panel.add_query("query_c")

        rendered_graph = render.render_panel(graph_panel.build(), panel_id=1, x=0, y=0)
        rendered_heatmap = render.render_panel(heatmap_panel.build(), panel_id=2, x=0, y=0)

        self.assertEqual("A", rendered_graph["targets"][0]["refId"])
        self.assertEqual("B", rendered_graph["targets"][1]["refId"])
        self.assertEqual("time_series", rendered_graph["targets"][0]["format"])
        self.assertEqual("heatmap", rendered_heatmap["targets"][0]["format"])
        self.assertIsNone(rendered_heatmap["targets"][0].get("instant"))

    def test_row_builder_applies_default_layout_with_panel_overrides(self):
        builders = require_module(self, "metrics.grafana.ticdc_new_arch.builders")

        row = builders.row("Summary", default_height=6, default_span=12)

        left = builders.graph("Left")
        left.add_query("up")
        row.add_graph(left)

        right = builders.graph("Right", span=24, height=9)
        right.add_query("up")
        row.add_graph(right)

        built = row.build()

        self.assertEqual((12, 6), (built.panels[0].span, built.panels[0].height))
        self.assertEqual((24, 9), (built.panels[1].span, built.panels[1].height))

    def test_common_does_not_expose_query_defaults_context_api(self):
        common = require_module(self, "metrics.grafana.common")

        self.assertFalse(hasattr(common, "query_defaults"))

    def test_common_expr_sum_uses_ticdc_changefeed_scope(self):
        common = require_module(self, "metrics.grafana.common")

        expr = common.expr_sum(
            "ticdc_scheduler_slow_table_replication_state",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        )

        self.assertEqual(
            'sum(ticdc_scheduler_slow_table_replication_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
            str(expr),
        )

    def test_common_target_infers_legend_from_expr_by_labels(self):
        common = require_module(self, "metrics.grafana.common")

        target = common.target(
            common.expr_sum(
                "ticdc_scheduler_slow_table_replication_state",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            )
        )

        self.assertEqual("{{namespace}}-{{changefeed}}", target.legend)

    def test_common_expr_sum_rate_uses_explicit_scope_and_by_labels(self):
        common = require_module(self, "metrics.grafana.common")

        expr = common.expr_sum_rate(
            "ticdc_kvclient_pull_event_count",
            scope="instance",
            by_labels=["instance", "type"],
        )

        self.assertEqual(
            'sum(rate(ticdc_kvclient_pull_event_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, type)',
            str(expr),
        )

    def test_common_graph_panel_applies_time_series_target_defaults(self):
        common = require_module(self, "metrics.grafana.common")
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
        self.assertEqual("{{namespace}}-{{changefeed}}", rendered["targets"][0]["legendFormat"])
        self.assertEqual("none", rendered["yaxes"][0]["format"])
        self.assertEqual("0", rendered["yaxes"][0]["min"])

    def test_common_layout_evenly_assigns_panel_widths(self):
        common = require_module(self, "metrics.grafana.common")
        render = require_module(self, "metrics.dsl.render")

        layout = common.Layout(title="Scheduler")
        layout.row(
            [
                common.graph_panel(
                    title="A",
                    targets=[common.target(expr=common.expr_sum("metric_a", scope="instance"))],
                ),
                common.graph_panel(
                    title="B",
                    targets=[common.target(expr=common.expr_sum("metric_b", scope="instance"))],
                ),
            ]
        )
        rendered = render.render_row(layout.row_panel, row_index=0, start_panel_id=1)

        self.assertEqual({"x": 0, "y": 0, "w": 12, "h": 7}, rendered["panels"][0]["gridPos"])
        self.assertEqual({"x": 12, "y": 0, "w": 12, "h": 7}, rendered["panels"][1]["gridPos"])

    def test_common_layout_uses_row_panel_height_defaults(self):
        common = require_module(self, "metrics.grafana.common")
        render = require_module(self, "metrics.dsl.render")

        layout = common.Layout(title="Scheduler", panel_height=6)
        layout.row(
            [
                common.graph_panel(
                    title="A",
                    targets=[common.target(common.expr_sum("metric_a", scope="instance"))],
                ),
                common.graph_panel(
                    title="B",
                    targets=[common.target(common.expr_sum("metric_b", scope="instance"))],
                ),
            ]
        )
        rendered = render.render_row(layout.row_panel, row_index=0, start_panel_id=1)

        self.assertEqual(6, rendered["panels"][0]["gridPos"]["h"])
        self.assertEqual(6, rendered["panels"][1]["gridPos"]["h"])

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
                helpers.series_query("sum(rate(ticdc_sink_dml_event_count[1m]))", legend="{{instance}}").legend,
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
            'max(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
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
            'histogram_quantile(0.99, sum(rate(ticdc_dispatcher_received_event_lag_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            helpers.histogram_quantile_rate(
                "ticdc_dispatcher_received_event_lag_duration",
                quantile=0.99,
                matchers=matchers,
                by=["instance"],
                window="1m",
            ),
        )
        self.assertEqual(
            'sum(rate(ticdc_dispatcher_received_event_lag_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance) / sum(rate(ticdc_dispatcher_received_event_lag_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
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
        self.assertEqual("{{instance}}-p99", rendered_graph["targets"][0]["legendFormat"])
        self.assertEqual("{{instance}}-avg", rendered_graph["targets"][1]["legendFormat"])
        self.assertEqual(
            "histogram_quantile(0.99, sum(rate(ticdc_dispatcher_received_event_lag_duration_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$ticdc_instance\"}[1m])) by (le, instance))",
            rendered_graph["targets"][0]["expr"],
        )
        self.assertEqual(
            "sum(rate(ticdc_dispatcher_received_event_lag_duration_sum{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$ticdc_instance\"}[1m])) by (instance) / sum(rate(ticdc_dispatcher_received_event_lag_duration_count{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$ticdc_instance\"}[1m])) by (instance)",
            rendered_graph["targets"][1]["expr"],
        )

    def test_changefeed_graph_hides_common_scope_and_promql_assembly(self):
        ticdc = require_module(self, "metrics.dsl.ticdc")
        render = require_module(self, "metrics.dsl.render")
        helpers = require_module(self, "metrics.dsl.promql")

        spec = ticdc.changefeed_graph(
            "Slowest Table Replication State",
            metric="ticdc_scheduler_slow_table_replication_state",
            aggregate="sum",
            by=["namespace", "changefeed"],
            legend=helpers.legend_for("namespace", "changefeed"),
            description="desc",
            unit="none",
            min=0,
            width=12,
            height=6,
        )
        rendered = render.render_panel(spec, panel_id=1, x=0, y=0)

        self.assertEqual("graph", rendered["type"])
        self.assertEqual(
            'sum(ticdc_scheduler_slow_table_replication_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
            rendered["targets"][0]["expr"],
        )
        self.assertEqual("{{namespace}}-{{changefeed}}", rendered["targets"][0]["legendFormat"])

    def test_instance_graph_supports_rate_sum_without_raw_query_strings(self):
        ticdc = require_module(self, "metrics.dsl.ticdc")
        render = require_module(self, "metrics.dsl.render")
        helpers = require_module(self, "metrics.dsl.promql")

        spec = ticdc.instance_graph(
            "TiCDC Input Bytes / s",
            metric="ticdc_event_store_write_bytes",
            transform="rate",
            aggregate="sum",
            by=["instance"],
            legend=helpers.legend_for("instance"),
            unit="binBps",
            min=0,
            width=12,
            height=6,
        )
        rendered = render.render_panel(spec, panel_id=2, x=0, y=0)

        self.assertEqual(
            'sum(rate(ticdc_event_store_write_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
            rendered["targets"][0]["expr"],
        )
        self.assertEqual("{{instance}}", rendered["targets"][0]["legendFormat"])

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
            query='label_values(ticdc_owner_checkpoint_ts_lag, namespace)',
            multi=True,
            include_all=True,
            all_value=".*",
        )
        rendered = render.render_variable(spec)
        self.assertEqual("query", rendered["type"])
        self.assertEqual("${DS_TEST-CLUSTER}", rendered["datasource"])
        self.assertTrue(rendered["includeAll"])
        self.assertEqual(".*", rendered["allValue"])

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

    def test_timeseries_panel_renders_core_fields(self):
        api = require_module(self, "metrics.dsl.api")
        render = require_module(self, "metrics.dsl.render")

        spec = api.timeseries(
            "Changefeed Status",
            targets=[
                api.target(
                    "max(ticdc_owner_status) by (namespace, changefeed)",
                    legend="{{namespace}}-{{changefeed}}",
                    format="time_series",
                    instant=False,
                )
            ],
            unit="short",
            min=0,
            height=6,
        )
        rendered = render.render_panel(spec, panel_id=9, x=0, y=0)
        self.assertEqual("timeseries", rendered["type"])
        self.assertEqual("short", rendered["fieldConfig"]["defaults"]["unit"])
        self.assertEqual(0, rendered["fieldConfig"]["defaults"]["min"])
        self.assertEqual(False, rendered["targets"][0]["instant"])

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
