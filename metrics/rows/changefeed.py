# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import LineLayouts, graph, row, table
from metrics.dsl.specs import RowSpec
from metrics.queries import expr_max, expr_sum, legend_for, regex


def build_changefeed_row() -> RowSpec:
    row_builder = row("Changefeed")

    node_table_count = graph(
        "Node Table Count",
        description="The table count of each ticdc node",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanager_table_dispatcher_count",
            by_labels=["instance"],
            scope="changefeed",
        )
    )

    changefeed_table_count = graph(
        "Changefeed Table Count",
        description="The table count of each changefeed",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanager_table_dispatcher_count",
            by_labels=["changefeed"],
            scope="changefeed",
        )
    )

    gc_time = (
        graph(
            "GC Time",
            unit="dateTimeAsIso",
        )
        .add_auto_query(
            expr_max("ticdc_gc_min_service_gc_safepoint", scope="none"),
            legend="gc time",
        )
        .add_auto_query(
            expr_max("ticdc_gc_cdc_gc_safepoint", scope="none"),
            legend="cdc service safepoint",
        )
    )

    changefeed_checkpoint = (
        graph(
            "Changefeed Checkpoint",
            description="The checkpoint ts of changefeeds.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"),
            legend="approximate current time (s)",
        )
        .add_query(
            expr_max(
                "ticdc_owner_checkpoint_ts",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
        )
    )

    changefeed_resolved_ts = (
        graph(
            "Changefeed Resolved Ts",
            description="The resolved ts of changefeeds.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"),
            legend="approximate current time (s)",
        )
        .add_query(
            expr_max(
                "ticdc_owner_resolved_ts",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
            legend=legend_for("namespace", "changefeed", suffix="barrier"),
            ref="C",
        )
        .add_auto_query(
            expr_max(
                "ticdc_scheduler_slow_table_puller_resolved_ts",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
            legend=legend_for("namespace", "changefeed", suffix="puller"),
        )
    )

    changefeed_checkpoint_lag = graph(
        "Changefeed Checkpoint Lag",
        description="The lag between changefeed checkpoint ts and PD TSO of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    changefeed_resolved_ts_lag = (
        graph(
            "Changefeed Resolved Ts Lag",
            description="The lag between changefeed resolved ts and PD TSO of upstream TiDB.",
            unit="s",
            min="0",
        )
        .add_query(
            expr_max(
                "ticdc_owner_resolved_ts_lag",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
            legend=legend_for("namespace", "changefeed", suffix="barrier"),
            ref="C",
        )
        .add_auto_query(
            expr_max(
                "ticdc_scheduler_slow_table_puller_resolved_ts_lag",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
            legend=legend_for("namespace", "changefeed", suffix="puller"),
        )
    )

    changefeed_error_details = table(
        "Changefeed Error Details",
        description="Current warning or failed reason of each changefeed. The metric message is normalized to a single line and truncated to 256 characters.",
    ).add_label_query(
        expr_max(
            "ticdc_owner_changefeed_error_info",
            by_labels=["namespace", "changefeed", "state", "code", "message"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
        columns=["namespace", "changefeed", "state", "code", "message"],
        legend="",
    )

    row_builder.add_panels(
        node_table_count,
        changefeed_table_count,
        gc_time,
        # Keep the legacy 1/4 + 1/4 + 1/2 split so this row stays visually
        # compatible with the checked-in dashboard layout.
        layout=LineLayouts.QUARTER_QUARTER_HALF,
    )

    row_builder.add_panels(
        changefeed_checkpoint,
        changefeed_resolved_ts,
    )

    row_builder.add_panels(
        changefeed_checkpoint_lag,
        changefeed_resolved_ts_lag,
    )

    row_builder.add_panel(changefeed_error_details)

    return row_builder.build()
