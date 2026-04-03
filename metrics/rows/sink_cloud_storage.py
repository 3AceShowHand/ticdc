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
    expr_sum,
    expr_sum_rate,
    legend_for,
    regex,
)


def build_sink_cloud_storage_row() -> RowSpec:
    row_builder = row("Sink - Cloud Storage Sink")

    flush_bytes_s = graph(
        "Flush Bytes / s",
        description="The cloud storage flushed bytes to the external storage",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_flush_bytes_sum",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
    )

    worker_busy_ratio = graph(
        "Worker Busy Ratio",
        description="Busy ratio (X ms in 1s) for cloud storage sink dml worker",
        unit="percent",
        min="0",
        decimals=1,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_worker_busy_ratio",
            by_labels=["namespace", "changefeed", "id", "instance"],
            scope="changefeed",
        ).op("*", "100"),
    )

    flush_duration = (
        graph(
            "Flush Duration",
            description="The time duration of flush data to the external storage system",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_sink_cloud_storage_flush_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="p99"),
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_cloud_storage_flush_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="avg"),
        )
    )

    file_count_s = graph(
        "File Count / s",
        description="The count of files flushed per second",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_flush_bytes_count",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    flush_reason_count_s = graph(
        "Flush Reason Count / s",
        min="0",
        decimals=1,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_flush_total",
            by_labels=["namespace", "changefeed", "reason", "instance"],
            scope="changefeed",
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{reason}}",
    )

    flush_dml_by_ddl_block_duration = (
        graph(
            "Flush DML By DDL Block Duration",
            description="The time duration of flush DMLs by the DDL",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_sink_cloud_storage_ddl_flush_dml_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="p99"),
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_cloud_storage_ddl_flush_dml_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="avg"),
        )
    )

    spool_memory_bytes = graph(
        "Spool Memory Bytes",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum(
            "ticdc_sink_cloud_storage_spool_memory_bytes",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
    )

    spool_segment_count = graph(
        "Spool Segment Count",
        description="The count of spool segment files",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum(
            "ticdc_sink_cloud_storage_spool_segment_count",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    spool_disk_bytes = graph(
        "Spool Disk Bytes",
        unit="bytes",
        min="0",
        decimals=1,
    ).add_auto_query(
        expr_sum(
            "ticdc_sink_cloud_storage_spool_disk_bytes",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
    )

    spool_rotate_count_s = graph(
        "Spool Rotate Count / s",
        description="The spool rotate segment per second",
        min="0",
        decimals=1,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_rotate_total",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    spool_disk_load_bytes_s = graph(
        "Spool Disk Load Bytes / s",
        description="The bytes load from the spool disk",
        unit="bytes",
        min="0",
        decimals=1,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_cloud_storage_load_bytes_sum",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
    )

    pending_postenqueue_count = graph(
        "Pending PostEnqueue Count",
        description="The number of pending post enqueue",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum(
            "ticdc_sink_cloud_storage_pending_post_enqueue",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    spool_disk_quota_waiters = graph(
        "Spool Disk Quota Waiters",
        description="The number of waiters on the disk quota",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum(
            "ticdc_sink_cloud_storage_spool_disk_quota_waiters",
            by_labels=["namespace", "changefeed", "instance"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    spool_disk_quota_wait_duration = (
        graph(
            "Spool Disk Quota Wait Duration",
            description="The time duration of waiting for the spool disk quota",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_sink_cloud_storage_spool_disk_quota_wait_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="p99"),
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_cloud_storage_spool_disk_quota_wait_duration_seconds",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend=legend_for("namespace", "changefeed", "instance", suffix="avg"),
        )
    )

    row_builder.add_panels(
        flush_bytes_s,
        worker_busy_ratio,
    )

    row_builder.add_panels(
        flush_duration,
        file_count_s,
    )

    row_builder.add_panels(
        flush_reason_count_s,
        flush_dml_by_ddl_block_duration,
    )

    row_builder.add_panels(
        spool_memory_bytes,
        spool_segment_count,
    )

    row_builder.add_panels(
        spool_disk_bytes,
        spool_rotate_count_s,
    )

    row_builder.add_panels(
        spool_disk_load_bytes_s,
        pending_postenqueue_count,
    )

    row_builder.add_panels(
        spool_disk_quota_waiters,
        spool_disk_quota_wait_duration,
    )

    return row_builder.build()
