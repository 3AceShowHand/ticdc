# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import expr_max, expr_simple, expr_sum, expr_sum_rate, legend_for, regex


def build_lag_summary_row() -> RowSpec:
    row_builder = row("Lag Summary")

    maintainer_checkpoint_lag = graph(
        "Maintainer Checkpoint Lag",
        description="The lag between changefeed checkpoint ts and the lac1 ts of upstream TiDB.",
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
        "Maintainer Resolved Ts lag",
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

    eventstore_resolved_ts_lag = graph(
        "EventStore Resolved Ts Lag ",
        description="",
        unit="s",
        min="0",
    ).add_auto_query(
        expr_sum(
            "ticdc_event_store_resolved_ts_lag",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    eventservice_resolved_ts_lag = (
        graph(
            "EventService Resolved Ts Lag ",
            description="",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_sum(
                "ticdc_event_service_resolved_ts_lag",
                by_labels=["instance", "type"],
                scope="instance",
                selectors=[regex("type", "received")],
            ),
            legend=legend_for("type", "instance"),
        )
        .add_auto_query(
            expr_sum(
                "ticdc_event_service_resolved_ts_lag",
                by_labels=["instance", "type"],
                scope="instance",
                selectors=[regex("type", "sent")],
            ),
            legend=legend_for("type", "instance"),
        )
    )

    dispatchermanager_checkpoint_lag = graph(
        "DispatcherManager Checkpoint Lag",
        description="",
        unit="s",
        min="0",
    ).add_query(
        expr_simple(
            "ticdc_dispatchermanager_checkpoint_ts_lag",
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-checkpointTsLag",
        ref="B",
    )

    dispatchermanager_resolved_ts_lag = graph(
        "DispatcherManager Resolved Ts Lag",
        description="",
        unit="s",
        min="0",
    ).add_query(
        expr_simple(
            "ticdc_dispatchermanager_resolved_ts_lag",
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-resolvedts",
        ref="B",
    )

    eventcollector_resolved_ts_lag = heatmap(
        "EventCollector Resolved Ts Lag",
        description="",
        unit="ms",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_dispatcher_received_event_lag_duration_bucket",
            by_labels=["le"],
            scope="instance",
        ),
        format="heatmap",
    )

    row_builder.add_panels(
        maintainer_checkpoint_lag,
        maintainer_resolved_ts_lag,
    )

    row_builder.add_panels(
        eventstore_resolved_ts_lag,
        eventservice_resolved_ts_lag,
    )

    row_builder.add_panels(
        dispatchermanager_checkpoint_lag,
        dispatchermanager_resolved_ts_lag,
    )

    row_builder.add_half_panel(eventcollector_resolved_ts_lag)

    return row_builder.build()
