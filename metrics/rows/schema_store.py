# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_schema_store_row() -> RowSpec:
    row_builder = row("Schema Store")

    resolved_ts_lag = graph(
        "Resolved Ts Lag",
        unit="s",
        min="0",
    ).add_auto_query(
        'ticdc_schema_store_resolved_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}-resolvedts",
    )

    register_table_num = graph(
        "Register Table Num",
        min="0",
    ).add_auto_query(
        'ticdc_schema_store_register_table_num{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}",
    )

    get_table_info_count_s = graph(
        "Get Table Info Count / s",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_schema_store_get_table_info_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    get_table_info_duration = heatmap(
        "Get Table Info Duration",
        unit="s",
    ).add_range_query(
        'sum(rate(ticdc_schema_store_get_table_info_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
        format="heatmap",
    )

    shared_column_schema_count = graph(
        "Shared Column Schema Count",
        min="0",
        decimals=0,
    ).add_auto_query(
        'ticdc_common_shared_column_schema_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}",
    )

    wait_resolved_ts_duration = (
        graph(
            "Wait Resolved Ts Duration",
            description="The duration of waiting for resolved ts in schema store. It shows the p80, p95, and max latency.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.8, sum(rate(ticdc_schema_store_wait_resolved_ts_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p80",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_schema_store_wait_resolved_ts_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-p95",
        )
        .add_auto_query(
            'histogram_quantile(1.0, sum(rate(ticdc_schema_store_wait_resolved_ts_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le, instance))',
            legend="{{instance}}-max",
        )
    )

    row_builder.add_panels(
        resolved_ts_lag,
        register_table_num,
    )

    row_builder.add_panels(
        get_table_info_count_s,
        get_table_info_duration,
    )

    row_builder.add_panels(
        shared_column_schema_count,
        wait_resolved_ts_duration,
    )

    return row_builder.build()
