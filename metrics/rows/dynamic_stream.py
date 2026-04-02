# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_dynamic_stream_row() -> RowSpec:
    row_builder = row("Dynamic Stream")

    ds_input_channel_length = graph(
        "DS Input Channel Length",
        description="",
        min="0",
        decimals=0,
    ).add_auto_query(
        'sum(rate(ticdc_dynamic_stream_event_chan_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, module)',
        legend="{{module}}-Input-chanel-len-{{instance}}",
        ref="B",
    )

    ds_pending_queue_length = graph(
        "DS Pending Queue Length",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_dynamic_stream_pending_queue_len{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, module)',
        legend="{{module}}-pending-queue-len-{{instance}}",
        ref="B",
    )

    p99_batch_count = (
        graph(
            "P99 - Batch Count",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
            hide=True,
        )
    )

    avg_batch_count = (
        graph(
            "Avg - Batch Count",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
        )
    )

    p99_batch_bytes = (
        graph(
            "P99 - Batch Bytes",
            unit="bytes",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
            hide=True,
        )
    )

    avg_batch_bytes = (
        graph(
            "Avg - Batch Bytes",
            unit="bytes",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_bytes_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_bytes_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
            hide=True,
        )
    )

    p99_batch_duration = (
        graph(
            "P99 - Batch Duration",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_dynamic_stream_batch_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (le, instance, module, area))',
            legend="{{module}}-{{area}}-{{instance}}",
            hide=True,
        )
    )

    avg_batch_duration = (
        graph(
            "Avg - Batch Duration",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module=~"event-collector|log-puller"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_dynamic_stream_batch_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area) / sum(rate(ticdc_dynamic_stream_batch_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", module!~"^(event-collector|log-puller)$"}[1m])) by (instance, module, area)',
            legend="{{module}}-{{area}}-{{instance}}",
            hide=True,
        )
    )

    row_builder.add_panels(
        ds_input_channel_length,
        ds_pending_queue_length,
    )

    row_builder.add_panels(
        p99_batch_count,
        avg_batch_count,
    )

    row_builder.add_panels(
        p99_batch_bytes,
        avg_batch_bytes,
    )

    row_builder.add_panels(
        p99_batch_duration,
        avg_batch_duration,
    )

    return row_builder.build()
