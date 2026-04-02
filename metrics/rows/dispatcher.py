# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, heatmap, row
from metrics.dsl.specs import RowSpec


def build_dispatcher_row() -> RowSpec:
    row_builder = row("Dispatcher")

    table_dispatcher_manager_count = graph(
        "Table Dispatcher Manager Count",
        description="",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        'sum(ticdc_dispatchermanagermanager_event_dispatcher_manager_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (instance)',
        legend="{{instance}}",
    )

    table_dispatcher_count = graph(
        "Table Dispatcher Count",
        description="",
        min="0",
    ).add_query(
        'sum(ticdc_dispatchermanager_table_dispatcher_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (instance, changefeed, event_type)',
        legend="{{instance}}-{{changefeed}}-{{event_type}}",
    )

    table_trigger_dispatcher_count = graph(
        "Table Trigger Dispatcher Count",
        description="",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        'sum(ticdc_dispatchermanager_table_trigger_dispatcher_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (instance, changefeed, event_type)',
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
            'histogram_quantile(0.9, sum(rate(ticdc_sink_create_dispatcher_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, changefeed, instance, event_type))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P90",
            ref="C",
        )
        .add_auto_query(
            'histogram_quantile(0.95, sum(rate(ticdc_sink_create_dispatcher_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, changefeed, instance, event_type))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P95",
        )
        .add_auto_query(
            'histogram_quantile(0.99, sum(rate(ticdc_sink_create_dispatcher_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, changefeed, instance, event_type))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P99",
        )
    )

    dispatcher_request_handle_result = graph(
        "Dispatcher Request Handle Result",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_sink_handle_dispatcher_request{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance, type)',
        legend="{{instance}}-{{target}}-{{type}}",
    )

    event_collector_registered_dispatcher_count = graph(
        "Event Collector Registered Dispatcher Count",
        description="the number of registered dispatchers in event collector",
        min="0",
    ).add_query(
        'sum(ticdc_event_service_dispatcher_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance)',
        legend="{{instance}}",
    )

    event_collector_received_resolved_ts_s = graph(
        "Event Collector Received Resolved Ts / s",
        description="The number of rows that event collector received per second.",
        min="0",
    ).add_query(
        'sum(rate(ticdc_dispatcher_received_event_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", type="ResolvedTs"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    event_collector_handle_message_duration = heatmap(
        "Event Collector Handle Message Duration",
        description="",
        unit="s",
    ).add_range_query(
        'sum(rate(ticdc_event_collector_handle_event_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
        format="heatmap",
    )

    block_statuses_channel_length = graph(
        "Block Statuses Channel Length",
        description="Length of dispatcher manager block statuses channel.",
        min="0",
    ).add_query(
        'max(ticdc_dispatchermanager_block_statuses_chan_len{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    block_status_request_queue_length = graph(
        "Block Status Request Queue Length",
        description="Length of heartbeat collector block status request queue.",
        min="0",
    ).add_query(
        'ticdc_heartbeat_collector_block_status_request_queue_len{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}',
        legend="{{instance}}",
    )

    event_collector_receive_event_lag = heatmap(
        "Event Collector Receive Event Lag",
        description="",
        unit="s",
    ).add_range_query(
        'sum(rate(ticdc_dispatcher_received_event_lag_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (le)',
        legend="{{le}}",
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
