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
    expr_max,
    expr_sum,
    legend_for,
)


def build_maintainer_row() -> RowSpec:
    row_builder = row("Maintainer")

    maintainer_checkpoint_lag = graph(
        "Maintainer Checkpoint Lag",
        description="The lag between maintainer checkpoint ts and PD TSO of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_maintainer_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
    )

    maintainer_resolved_ts_lag = graph(
        "Maintainer Resolved Ts Lag",
        description="The lag between maintainer resolved ts and PD TSO of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_maintainer_resolved_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
        legend=legend_for("namespace", "changefeed", suffix="resolvedts"),
    )

    changefeed_maintainer_count = graph(
        "Changefeed Maintainer Count",
        description="",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_changefeed_maintainer_counter",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        ref="B",
    )

    maintainer_handle_event_duration = (
        graph(
            "Maintainer Handle Event Duration",
            unit="s",
            min="0",
            decimals=0,
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_maintainer_handle_event_duration",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            ),
            legend="99.9-{{namespace}}-{{changefeed}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_maintainer_handle_event_duration",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}",
        )
    )

    maintainer_event_channel_length = graph(
        "Maintainer Event Channel Length",
        description="Length of maintainer event channel.",
        min="0",
        decimals=0,
    ).add_query(
        expr_max(
            "ticdc_maintainer_event_ch_len",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
    )

    row_builder.add_panels(
        maintainer_checkpoint_lag,
        maintainer_resolved_ts_lag,
    )

    row_builder.add_panels(
        changefeed_maintainer_count,
        maintainer_handle_event_duration,
    )

    row_builder.add_half_panel(maintainer_event_channel_length)

    return row_builder.build()
