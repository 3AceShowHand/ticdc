# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_sum,
    expr_sum_delta,
    expr_sum_rate,
)


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
            expr_histogram_quantile(
                0.999,
                "ticdc_ddl_exec_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="99.9-duration-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_ddl_exec_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    sink_running_ddl_count = graph(
        "Sink Running DDL Count",
        description="Count of running DDL.",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_ddl_exec_running",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
    )

    maintainer_blocking_ddl_count = graph(
        "Maintainer Blocking DDL Count",
        description="Count of blocking DDL.",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_ddl_exec_blocking",
            by_labels=["namespace", "changefeed", "mode"],
            scope="changefeed",
        ),
    )

    sink_ddl_count_m = graph(
        "Sink DDL Count / m",
        description="Execution count of different DDL types in the last minute.",
        min="0",
    ).add_auto_query(
        expr_sum_delta(
            "ticdc_ddl_execution",
            by_labels=["namespace", "changefeed", "ddl_type"],
            scope="changefeed",
        ),
    )

    handle_ddl_duration = heatmap(
        "Handle DDL Duration",
        description="DDL handling duration distribution.",
        unit="s",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_ddl_handle_duration_bucket",
            by_labels=["le"],
            scope="changefeed",
        ),
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
