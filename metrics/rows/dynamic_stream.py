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
    expr_sum_rate,
    not_regex,
    regex,
)


def build_dynamic_stream_row() -> RowSpec:
    row_builder = row("Dynamic Stream")

    ds_input_channel_length = graph(
        "DS Input Channel Length",
        description="",
        min="0",
        decimals=0,
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_dynamic_stream_event_chan_size",
            by_labels=["instance", "module"],
            scope="instance",
        ),
        legend="{{module}}-Input-chanel-len-{{instance}}",
        ref="B",
    )

    ds_pending_queue_length = graph(
        "DS Pending Queue Length",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_dynamic_stream_pending_queue_len",
            by_labels=["instance", "module"],
            scope="instance",
        ),
        legend="{{module}}-pending-queue-len-{{instance}}",
        ref="B",
    )

    p99_batch_count = (
        graph(
            "P99 - Batch Count",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_count",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_count",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_count",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_count",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_bytes",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_bytes",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_bytes",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_bytes",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_duration",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_dynamic_stream_batch_duration",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_duration",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[regex("module", "event-collector|log-puller")],
            ),
            legend="{{module}}-{{area}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_dynamic_stream_batch_duration",
                by_labels=["instance", "module", "area"],
                scope="instance",
                selectors=[not_regex("module", "^(event-collector|log-puller)$")],
            ),
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
