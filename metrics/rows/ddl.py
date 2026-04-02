# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_ddl_row() -> RowSpec:
    row_builder = row("DDL")

    output_ddl_executing_duration = (
        graph(
            "Output DDL Executing Duration",
            description="DDL executing duration",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_ddl_exec_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance) / sum(rate(ticdc_ddl_exec_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance)',
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_ddl_exec_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, namespace, changefeed, instance))',
            legend="99.9-duration-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    sink_running_ddl_count = graph(
        "Sink Running DDL Count",
        description="Count of running DDL.",
        min="0",
    ).add_query(
        'sum(ticdc_ddl_exec_running{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}",
    )

    maintainer_blocking_ddl_count = graph(
        "Maintainer Blocking DDL Count",
        description="Count of blocking DDL.",
        min="0",
    ).add_query(
        'sum(ticdc_ddl_exec_blocking{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, mode)',
        legend="{{namespace}}-{{changefeed}}-{{mode}}",
    )

    sink_ddl_count_m = graph(
        "Sink DDL Count / m",
        description="Execution count of different DDL types in the last minute.",
        min="0",
    ).add_auto_query(
        'sum(delta(ticdc_ddl_execution{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, ddl_type)',
        legend="{{namespace}}-{{changefeed}}-{{ddl_type}}",
    )

    handle_ddl_duration = heatmap(
        "Handle DDL Duration",
        description="DDL handling duration distribution.",
        unit="s",
    ).add_range_query(
        'sum(rate(ticdc_ddl_handle_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le)',
        legend="{{le}}",
        format="heatmap",
    )

    row_builder.add_panels(
        output_ddl_executing_duration,
        sink_running_ddl_count,
    )

    row_builder.add_panels(
        maintainer_blocking_ddl_count,
        sink_ddl_count_m,
    )

    row_builder.add_half_panel(handle_ddl_duration)

    return row_builder.build()
