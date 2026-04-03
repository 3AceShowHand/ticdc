# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec
from metrics.queries import expr_histogram_quantile, expr_max, expr_simple, expr_sum, expr_sum_rate


def build_dispatcher_row() -> RowSpec:
    row_builder = row("Dispatcher")

    table_dispatcher_manager_count = graph(
        "Table Dispatcher Manager Count",
        description="",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanagermanager_event_dispatcher_manager_count",
            by_labels=["instance"],
            scope="changefeed",
        ),
    )

    table_dispatcher_count = graph(
        "Table Dispatcher Count",
        description="",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanager_table_dispatcher_count",
            by_labels=["instance", "changefeed", "event_type"],
            scope="changefeed",
        ),
    )

    table_trigger_dispatcher_count = graph(
        "Table Trigger Dispatcher Count",
        description="",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanager_table_trigger_dispatcher_count",
            by_labels=["instance", "changefeed", "event_type"],
            scope="changefeed",
        ),
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}",
    )

    create_dispatcher_duration = (
        graph(
            "Create Dispatcher Duration",
            description="Duration of dispatcher creation",
            unit="s",
            min="0",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.9,
                "ticdc_sink_create_dispatcher_duration",
                by_labels=["changefeed", "instance", "event_type"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P90",
            ref="C",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.95,
                "ticdc_sink_create_dispatcher_duration",
                by_labels=["changefeed", "instance", "event_type"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P95",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.99,
                "ticdc_sink_create_dispatcher_duration",
                by_labels=["changefeed", "instance", "event_type"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P99",
        )
    )

    dispatcher_request_handle_result = graph(
        "Dispatcher Request Handle Result",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_sink_handle_dispatcher_request",
            by_labels=["namespace", "changefeed", "instance", "type"],
            scope="changefeed",
        ),
        legend="{{instance}}-{{target}}-{{type}}",
    )

    event_collector_registered_dispatcher_count = graph(
        "Event Collector Registered Dispatcher Count",
        description="the number of registered dispatchers in event collector",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_event_service_dispatcher_count",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    event_collector_received_resolved_ts_s = graph(
        "Event Collector Received Resolved Ts / s",
        description="The number of rows that event collector received per second.",
        min="0",
    ).add_query(
        expr_sum_rate(
            "ticdc_dispatcher_received_event_count",
            by_labels=["instance"],
            scope="instance",
            selectors=['type="ResolvedTs"'],
        ),
    )

    event_collector_handle_message_duration = heatmap(
        "Event Collector Handle Message Duration",
        description="",
        unit="s",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_event_collector_handle_event_duration_bucket",
            by_labels=["le"],
            scope="instance",
        ),
        format="heatmap",
    )

    block_statuses_channel_length = graph(
        "Block Statuses Channel Length",
        description="Length of dispatcher manager block statuses channel.",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_dispatchermanager_block_statuses_chan_len",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
    )

    block_status_request_queue_length = graph(
        "Block Status Request Queue Length",
        description="Length of heartbeat collector block status request queue.",
        min="0",
    ).add_query(
        expr_simple(
            "ticdc_heartbeat_collector_block_status_request_queue_len",
            scope="instance",
        ),
        legend="{{instance}}",
    )

    event_collector_receive_event_lag = heatmap(
        "Event Collector Receive Event Lag",
        description="",
        unit="s",
    ).add_range_query(
        expr_sum_rate(
            "ticdc_dispatcher_received_event_lag_duration_bucket",
            by_labels=["le"],
            scope="instance",
        ),
        format="heatmap",
    )

    row_builder.add_panels(
        table_dispatcher_manager_count,
        table_dispatcher_count,
    )

    row_builder.add_panels(
        table_trigger_dispatcher_count,
        create_dispatcher_duration,
    )

    row_builder.add_panels(
        dispatcher_request_handle_result,
        event_collector_registered_dispatcher_count,
    )

    row_builder.add_panels(
        event_collector_received_resolved_ts_s,
        event_collector_handle_message_duration,
    )

    row_builder.add_panels(
        block_statuses_channel_length,
        block_status_request_queue_length,
    )

    row_builder.add_half_panel(event_collector_receive_event_lag)

    return row_builder.build()
