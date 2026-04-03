# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_sink_transaction_row() -> RowSpec:
    row_builder = row("Sink - Transaction Sink")

    conflict_detect_duration = (
        graph(
            "Conflict Detect Duration",
            description="Duration of event staying in conflict detector",
            unit="s",
        )
        .add_histogram(
            "ticdc_sink_txn_conflict_detect_duration",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
            quantile=0.999,
            quantile_legend="{{namespace}}-{{changefeed}}-{{instance}}-detect-P999",
            average_legend="{{namespace}}-{{changefeed}}-{{instance}}-detect-avg",
        )
        .add_histogram(
            "ticdc_sink_txn_queue_duration",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
            quantile=0.999,
            quantile_legend="{{namespace}}-{{changefeed}}-{{instance}}-queue-P999",
            average_legend="{{namespace}}-{{changefeed}}-{{instance}}-queue-avg",
        )
    )

    full_flush_duration = graph(
        "Full Flush Duration",
        description="Full flush (backend flush + callback + conflict detector notify) duration",
        unit="s",
    ).add_histogram(
        "ticdc_sink_txn_worker_flush_duration",
        by_labels=["namespace", "changefeed", "instance"],
        scope="changefeed",
        quantile=0.999,
        quantile_legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        average_legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
    )

    worker_busy_ratio = graph(
        "Worker Busy Ratio",
        description="Sink worker busy ratio",
        unit="percent",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_sink_txn_worker_batch_flush_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance, id) / sum(rate(ticdc_sink_txn_worker_total_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance, id) * 100',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-worker-{{id}}",
    )

    worker_input_rows_s = graph(
        "Worker Input Rows / s",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_sink_txn_worker_handled_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance, id)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{id}}",
    )

    backend_flush_duration = graph(
        "Backend Flush Duration",
        description="Distribution of flush transaction duration to backend",
        unit="s",
        min="0",
    ).add_histogram(
        "ticdc_sink_txn_sink_dml_batch_commit",
        by_labels=["namespace", "changefeed", "instance"],
        scope="changefeed",
        quantile=0.999,
        quantile_legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        average_legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
    )

    row_affected_count_m = graph(
        "Row Affected Count / m",
        description="The number of affected rows",
        min="0",
        decimals=0,
    ).add_query(
        'sum(increase(ticdc_sink_dml_event_affected_row_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, count_type, row_type)',
        legend="{{namespace}}-{{changefeed}}-{{count_type}}-{{row_type}}",
    )

    row_builder.add_panels(
        conflict_detect_duration,
        full_flush_duration,
    )

    row_builder.add_panels(
        worker_busy_ratio,
        worker_input_rows_s,
    )

    row_builder.add_panels(
        backend_flush_duration,
        row_affected_count_m,
    )

    return row_builder.build()
