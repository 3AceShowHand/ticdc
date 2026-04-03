# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_sum_increase,
    expr_sum_rate,
)


def build_sink_transaction_row() -> RowSpec:
    row_builder = row("Sink - Transaction Sink")

    conflict_detect_duration = (
        graph(
            "Conflict Detect Duration",
            description="Duration of event staying in conflict detector",
            unit="s",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_sink_txn_conflict_detect_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-detect-P999",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_txn_conflict_detect_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-detect-avg",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_sink_txn_queue_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-queue-P999",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_txn_queue_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-queue-avg",
        )
    )

    full_flush_duration = (
        graph(
            "Full Flush Duration",
            description="Full flush (backend flush + callback + conflict detector notify) duration",
            unit="s",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_sink_txn_worker_flush_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_txn_worker_flush_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    worker_busy_ratio = graph(
        "Worker Busy Ratio",
        description="Sink worker busy ratio",
        unit="percent",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_txn_worker_batch_flush_duration_sum",
            by_labels=["namespace", "changefeed", "instance", "id"],
            scope="changefeed",
        )
        .op(
            "/",
            expr_sum_rate(
                "ticdc_sink_txn_worker_total_duration_sum",
                by_labels=["namespace", "changefeed", "instance", "id"],
                scope="changefeed",
            ),
        )
        .op("*", "100"),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-worker-{{id}}",
    )

    worker_input_rows_s = graph(
        "Worker Input Rows / s",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_txn_worker_handled_rows",
            by_labels=["namespace", "changefeed", "instance", "id"],
            scope="changefeed",
        ),
    )

    backend_flush_duration = (
        graph(
            "Backend Flush Duration",
            description="Distribution of flush transaction duration to backend",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_sink_txn_sink_dml_batch_commit",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_txn_sink_dml_batch_commit",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    row_affected_count_m = graph(
        "Row Affected Count / m",
        description="The number of affected rows",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum_increase(
            "ticdc_sink_dml_event_affected_row_count",
            by_labels=["namespace", "changefeed", "count_type", "row_type"],
            scope="changefeed",
        ),
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
