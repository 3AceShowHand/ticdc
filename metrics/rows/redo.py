# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    expr_max,
    expr_max_rate,
    expr_sum,
    expr_sum_rate,
    regex,
)


def build_redo_row() -> RowSpec:
    row_builder = row("Redo")
    worker_busy_ratio_expr = expr_sum_rate(
        "ticdc_redo_worker_busy_ratio",
        by_labels=["changefeed", "instance"],
        scope="cluster",
        selectors=[
            regex("changefeed", "$changefeed"),
            regex("instance", "$ticdc_instance"),
        ],
    )

    redo_fsync_duration = heatmap(
        "Redo Fsync Duration",
        description="The latency distributions of fsync called by redo writer",
        unit="s",
    ).add_query(
        expr_max_rate(
            "ticdc_redo_fsync_duration_seconds_bucket",
            by_labels=["le"],
            scope="cluster",
            selectors=[regex("changefeed", "$changefeed"), regex("instance", "$ticdc_instance")],
        )
    )

    redo_flushall_duration = heatmap(
        "Redo Flushall Duration",
        description="The latency distributions of flushall called by redo writer",
        unit="s",
    ).add_query(
        expr_max_rate(
            "ticdc_redo_flush_all_duration_seconds_bucket",
            by_labels=["le"],
            scope="cluster",
            selectors=[regex("changefeed", "$changefeed"), regex("instance", "$ticdc_instance")],
        )
    )

    row_builder.add_panels(redo_fsync_duration, redo_flushall_duration)

    redo_write_log_duration = heatmap(
        "Redo Write Log Duration",
        description="The latency distributions of writeLog called by redoManager",
        unit="s",
    ).add_query(
        expr_max_rate(
            "ticdc_redo_write_log_duration_seconds_bucket",
            by_labels=["le"],
            scope="cluster",
            selectors=[regex("changefeed", "$changefeed"), regex("instance", "$ticdc_instance")],
        )
    )

    redo_flush_log_duration = heatmap(
        "Redo Flush Log Duration",
        description="The latency distributions of flushLog called by redoManager",
        unit="s",
    ).add_query(
        expr_max_rate(
            "ticdc_redo_flush_log_duration_seconds_bucket",
            by_labels=["le"],
            scope="cluster",
            selectors=[regex("changefeed", "$changefeed"), regex("instance", "$ticdc_instance")],
        )
    )

    row_builder.add_panels(redo_write_log_duration, redo_flush_log_duration)

    redo_write_rows_s = (
        graph(
            "Redo Write Rows / s",
            description="The total count of rows that are processed by redo writer",
            unit="short",
        )
        .add_query(
            expr_sum_rate(
                "ticdc_redo_total_rows_count",
                by_labels=["instance"],
                scope="cluster",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("instance", "$ticdc_instance"),
                ],
            )
        )
        .add_query(
            expr_sum_rate(
                "ticdc_redo_total_rows_count",
                by_labels=["changefeed"],
                scope="cluster",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("instance", "$ticdc_instance"),
                ],
            ),
            legend="total",
        )
    )

    redo_write_bytes_s = graph(
        "Redo Write Bytes / s", description="Total number of bytes redo log written", unit="bytes"
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_redo_write_bytes_total",
            by_labels=["instance"],
            scope="cluster",
            selectors=[regex("changefeed", "$changefeed"), regex("instance", "$ticdc_instance")],
        )
    )

    row_builder.add_panels(redo_write_rows_s, redo_write_bytes_s)

    worker_busy_ratio = graph(
        "Worker Busy Ratio", description="Redo bgUpdateLog worker busy ratio", unit="percent"
    ).add_auto_query(f"{worker_busy_ratio_expr} * 100", legend="{{changefeed}}-{{instance}}")

    memory_quota = graph(
        "Memory Quota", description="Changefeed memory quota", unit="bytes", min="0"
    ).add_auto_query(
        expr_sum(
            "ticdc_dynamic_stream_memory_usage",
            by_labels=["namespace", "area", "instance", "module", "type"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("area", "$changefeed"),
                regex("instance", "$ticdc_instance"),
                regex("module", "event-collector-redo"),
            ],
        )
    )

    row_builder.add_panels(worker_busy_ratio, memory_quota)

    redo_checkpoint_ts = (
        graph(
            "Redo Checkpoint Ts",
            description="The checkpoint ts persisted by redo meta.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"), legend="approximate current time (s)"
        )
        .add_query(
            expr_max(
                "ticdc_redo_checkpoint_ts",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            )
        )
    )

    redo_resolved_ts = (
        graph(
            "Redo Resolved Ts",
            description="The resolved ts persisted by redo meta.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"), legend="approximate current time (s)"
        )
        .add_query(
            expr_max(
                "ticdc_redo_resolved_ts",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            )
        )
    )

    row_builder.add_panels(redo_checkpoint_ts, redo_resolved_ts)

    return row_builder.build()
