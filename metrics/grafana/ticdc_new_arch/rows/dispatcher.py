# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    eq,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_dispatcher_row() -> RowSpec:
    row_builder = row('Dispatcher', default_height=6, default_span=12)

    table_dispatcher_manager_count = graph(
        'Table Dispatcher Manager Count',
        description='',
        unit='none',
        min='0',
    )

    table_dispatcher_manager_count.add_query(
        expr_sum(
            'ticdc_dispatchermanagermanager_event_dispatcher_manager_count',
            by_labels=['instance'],
            scope='changefeed',
        ),
        legend='{{instance}}',
    )

    row_builder.add_graph(table_dispatcher_manager_count)

    table_dispatcher_count = graph('Table Dispatcher Count', description='', unit='short', min='0')

    table_dispatcher_count.add_query(
        expr_sum(
            'ticdc_dispatchermanager_table_dispatcher_count',
            by_labels=['instance', 'changefeed', 'event_type'],
            scope='changefeed',
        ),
        legend='{{instance}}-{{changefeed}}-{{event_type}}',
    )

    row_builder.add_graph(table_dispatcher_count)

    table_trigger_dispatcher_count = graph(
        'Table Trigger Dispatcher Count',
        description='',
        unit='none',
        min='0',
    )

    table_trigger_dispatcher_count.add_query(
        expr_sum(
            'ticdc_dispatchermanager_table_trigger_dispatcher_count',
            by_labels=['instance', 'changefeed', 'event_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}',
    )

    row_builder.add_graph(table_trigger_dispatcher_count)

    create_dispatcher_duration = graph(
        'Create Dispatcher Duration',
        description='Duration of dispatcher creation',
        unit='s',
        min='0',
    )

    create_dispatcher_duration.add_query(
        expr_histogram_quantile(
            0.9,
            'ticdc_sink_create_dispatcher_duration',
            by_labels=['changefeed', 'instance', 'event_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P90',
        ref='C',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_sink_create_dispatcher_duration',
            by_labels=['changefeed', 'instance', 'event_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P95',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_sink_create_dispatcher_duration',
            by_labels=['changefeed', 'instance', 'event_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{event_type}}-detect-P99',
        ref='B',
        format=None,
    )

    row_builder.add_graph(create_dispatcher_duration)

    dispatcher_request_handle_result = graph(
        'Dispatcher Request Handle Result',
        unit='short',
        min='0',
        height=7,
    )

    dispatcher_request_handle_result.add_query(
        expr_sum_rate(
            'ticdc_sink_handle_dispatcher_request',
            by_labels=['namespace', 'changefeed', 'instance', 'type'],
            scope='changefeed',
        ),
        legend='{{instance}}-{{target}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(dispatcher_request_handle_result)

    event_collector_registered_dispatcher_count = graph(
        'Event Collector Registered Dispatcher Count',
        description='the number of registered dispatchers in event collector',
        unit='short',
        min='0',
        height=7,
    )

    event_collector_registered_dispatcher_count.add_query(
        expr_sum(
            'ticdc_event_service_dispatcher_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
    )

    row_builder.add_graph(event_collector_registered_dispatcher_count)

    event_collector_received_resolved_ts_s = graph(
        'Event Collector Received Resolved Ts / s',
        description='The number of rows that event collector received per second.',
        unit='short',
        min='0',
        height=8,
    )

    event_collector_received_resolved_ts_s.add_query(
        expr_sum_rate(
            'ticdc_dispatcher_received_event_count',
            by_labels=['instance'],
            scope='instance',
            selectors=[eq('type', 'ResolvedTs')],
        ),
        legend='{{instance}}',
    )

    row_builder.add_graph(event_collector_received_resolved_ts_s)

    event_collector_handle_message_duration = heatmap(
        'Event Collector Handle Message Duration',
        description='',
        unit='s',
        height=8,
    )

    event_collector_handle_message_duration.add_query(
        expr_sum_rate(
            'ticdc_event_collector_handle_event_duration_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(event_collector_handle_message_duration)

    block_statuses_channel_length = graph(
        'Block Statuses Channel Length',
        description='Length of dispatcher manager block statuses channel.',
        unit='short',
        min='0',
    )

    block_statuses_channel_length.add_query(
        expr_max(
            'ticdc_dispatchermanager_block_statuses_chan_len',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(block_statuses_channel_length)

    block_status_request_queue_length = graph(
        'Block Status Request Queue Length',
        description='Length of heartbeat collector block status request queue.',
        unit='short',
        min='0',
    )

    block_status_request_queue_length.add_query(
        expr_simple(
            'ticdc_heartbeat_collector_block_status_request_queue_len',
            scope='instance',
        ),
        legend='{{instance}}',
    )

    row_builder.add_graph(block_status_request_queue_length)

    event_collector_receive_event_lag = heatmap(
        'Event Collector Receive Event Lag',
        description='',
        unit='s',
        height=8,
    )

    event_collector_receive_event_lag.add_query(
        expr_sum_rate(
            'ticdc_dispatcher_received_event_lag_duration_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(event_collector_receive_event_lag)

    return row_builder.build()
