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
    expr_sum_delta,
    expr_sum_rate,
)


def build_sink_general_row() -> RowSpec:
    row_builder = row("Sink - General")

    output_row_batch_count = (
        graph(
            "Output Row Batch Count",
            description="Row count for batch to the downstream sink.",
            unit="short",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_sink_batch_row_count",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="99.9%-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_sink_batch_row_count",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    output_row_count_per_second = graph(
        "Output Row Count (per second)",
        description="Row count for total output rows.",
        unit="short",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_batch_row_count_sum",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        )
    )

    row_builder.add_panels(
        output_row_batch_count,
        output_row_count_per_second,
    )

    sink_error_count_m = graph(
        "Sink Error Count / m",
        description="Count of errors in the last minute.",
        unit="short",
        min="0",
    ).add_auto_query(
        expr_sum_delta(
            "ticdc_sink_execution_error",
            by_labels=["namespace", "changefeed", "instance", "event_type"],
            scope="changefeed",
        )
    )

    row_builder.add_half_panel(sink_error_count_m)

    return row_builder.build()
