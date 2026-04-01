# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_simple,
    expr_sum,
    expr_sum_rate,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_dataflow_row() -> RowSpec:
    row_builder = row('Dataflow', default_height=5, default_span=12)

    sink_selectors = [
        regex('namespace', '$namespace'),
        regex('changefeed', '$changefeed'),
        regex('instance', '$ticdc_instance'),
    ]

    puller_output_events_s = graph(
        'Puller Output Events / s',
        description='The number of events that puller outputs to sorter \n per second',
        unit='short',
        min='0',
    )

    puller_output_events_s.add_query(
        expr_sum_rate(
            'ticdc_kvclient_pull_event_count',
            scope='instance',
            by_labels=['instance', 'type'],
        ),
    )

    row_builder.add_graph(puller_output_events_s)

    puller_output_event_rows = graph(
        'Puller Output Event Rows',
        description='The total number of events that puller outputs',
        unit='short',
    )

    puller_output_event_rows.add_query(
        expr_simple('ticdc_kvclient_pull_event_count', scope='instance'),
        legend='{{instance}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(puller_output_event_rows)

    eventservice_output_event_row_s = graph(
        'EventService Output Event Row / s',
        description='The total number of events that EventService outputs',
        unit='short',
        min='0',
    )

    eventservice_output_event_row_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_send_event_count',
            scope='instance',
            by_labels=['instance', 'type'],
        ),
    )

    row_builder.add_graph(eventservice_output_event_row_s)

    eventservice_output_event_rows = graph(
        'EventService Output Event Rows',
        description='The total number of events that puller outputs',
        unit='short',
        min='0',
    )

    eventservice_output_event_rows.add_query(
        expr_sum(
            'ticdc_event_service_send_event_count',
            scope='instance',
            by_labels=['instance', 'type'],
        ),
    )

    row_builder.add_graph(eventservice_output_event_rows)

    event_collector_received_event_rows_s = graph(
        'Event Collector Received Event Rows / s',
        description='The number of rows that event collector received per second.',
        unit='short',
        min='0',
    )

    event_collector_received_event_rows_s.add_query(
        expr_sum_rate(
            'ticdc_dispatcher_received_event_count',
            scope='instance',
            by_labels=['instance', 'type'],
        ),
    )

    row_builder.add_graph(event_collector_received_event_rows_s)

    event_collector_received_event_rows = graph(
        'Event Collector Received Event Rows',
        description='The total number of events that Event Collector received',
        unit='short',
        min='0',
    )

    event_collector_received_event_rows.add_query(
        expr_sum(
            'ticdc_dispatcher_received_event_count',
            scope='instance',
            by_labels=['instance', 'type'],
        ),
    )

    row_builder.add_graph(event_collector_received_event_rows)

    sink_flush_rows_s = graph(
        'Sink Flush Rows / s',
        description='The number of rows that sink flushes to downstream per second.',
        unit='short',
        min='0',
    )

    sink_flush_rows_s.add_query(
        expr_sum_rate(
            'ticdc_sink_batch_row_count_sum',
            scope='cluster',
            selectors=sink_selectors,
            by_labels=['namespace', 'changefeed', 'instance'],
        ),
    )

    row_builder.add_graph(sink_flush_rows_s)

    sink_flush_rows = graph(
        'Sink Flush Rows',
        description='The number of rows(events) that are flushed by sink.',
        unit='short',
        min='0',
    )

    sink_flush_rows.add_query(
        expr_sum(
            'ticdc_sink_batch_row_count_sum',
            scope='cluster',
            selectors=sink_selectors,
            by_labels=['namespace', 'changefeed', 'instance'],
        ),
    )

    row_builder.add_graph(sink_flush_rows)

    return row_builder.build()
