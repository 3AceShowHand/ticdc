# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    eq,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_event_service_row() -> RowSpec:
    row_builder = row('Event Service', default_height=8, default_span=12)

    scan_window_interval = graph(
        'Scan window interval',
        description='The lag between changefeed checkpoint ts and the lac1 ts of upstream TiDB.',
        unit='s',
        min='0',
        height=6,
    )

    scan_window_interval.add_query(
        expr_max(
            'ticdc_event_service_scan_window_interval',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{instance}}{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(scan_window_interval)

    scan_window_base_ts = graph('Scan window base ts', description='', unit='none', height=6)

    scan_window_base_ts.add_query(
        expr_max(
            'ticdc_event_service_scan_window_base_ts',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{instance}}{{namespace}}-{{changefeed}}',
        ref='B',
    )

    row_builder.add_graph(scan_window_base_ts)

    event_service_scan_duration = heatmap(
        'Event Service Scan Duration',
        description='',
        unit='s',
        height=6,
    )

    event_service_scan_duration.add_query(
        expr_sum_rate(
            'ticdc_event_service_scan_duration_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(event_service_scan_duration)

    event_service_scan_duration_2 = graph(
        'Event Service Scan Duration',
        description='',
        unit='s',
        min='0',
        height=6,
    )

    event_service_scan_duration_2.add_query(
        expr_histogram_avg(
            'ticdc_event_service_scan_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        instant=False,
    ).add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_event_service_scan_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p999',
        format=None,
    )

    row_builder.add_graph(event_service_scan_duration_2)

    event_service_scanned_entry_count = graph('Event Service Scanned Entry Count', unit='short', min='0')

    event_service_scanned_entry_count.add_query(
        expr_sum_rate(
            'ticdc_event_service_scanned_count_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(event_service_scanned_entry_count)

    event_service_scanned_transaction_count = graph('Event Service Scanned Transaction Count', unit='short', min='0')

    event_service_scanned_transaction_count.add_query(
        expr_sum_rate(
            'ticdc_event_service_scanned_txn_count_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(event_service_scanned_transaction_count)

    event_service_scanned_entry_bytes_s = graph('Event Service Scanned Entry Bytes / s', unit='bytes', min='0')

    event_service_scanned_entry_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_scan_bytes',
            by_labels=['instance'],
            scope='instance',
            selectors=[eq('type', 'scanned')],
        ),
        legend='{{instance}}',
        format=None,
    ).add_query(
        expr_sum_rate(
            'ticdc_event_store_scan_bytes',
            by_labels=['instance'],
            scope='instance',
            selectors=[eq('type', 'skipped')],
        ),
        legend='{{instance}}-skipped',
        format=None,
    )

    row_builder.add_graph(event_service_scanned_entry_bytes_s)

    event_service_scanned_transaction_bytes_s = graph(
        'Event Service Scanned Transaction Bytes / s',
        unit='bytes',
        min='0',
    )

    event_service_scanned_transaction_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_scanned_dml_size_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(event_service_scanned_transaction_bytes_s)

    event_service_finished_scan_task_count = graph('Event Service Finished Scan Task Count', unit='short', min='0')

    event_service_finished_scan_task_count.add_query(
        expr_sum_rate(
            'ticdc_event_service_scan_task_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='finished-scan-task-{{instance}}',
        format=None,
    )

    row_builder.add_graph(event_service_finished_scan_task_count)

    event_service_resolved_ts_lag = graph(
        'Event Service Resolved Ts Lag',
        description='',
        unit='s',
        min='0',
    )

    event_service_resolved_ts_lag.add_query(
        expr_sum(
            'ticdc_event_service_resolved_ts_lag',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-resolvedts',
    )

    row_builder.add_graph(event_service_resolved_ts_lag)

    event_service_pending_scan_task = graph('Event Service Pending Scan Task', unit='short', min='0')

    event_service_pending_scan_task.add_query(
        expr_sum_rate(
            'ticdc_event_service_pending_scan_task_count',
            by_labels=['namespace', 'instance'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('instance', '$ticdc_instance')],
        ),
        legend='pending-task-{{instance}}',
        format=None,
    )

    row_builder.add_graph(event_service_pending_scan_task)

    event_service_dispatcher_status = graph('Event Service Dispatcher Status', unit='short', min='0')

    event_service_dispatcher_status.add_query(
        expr_simple(
            'ticdc_event_service_dispatcher_status_count',
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}-dispatcherStatus-{{status}}',
        ref='B',
        format=None,
    )

    row_builder.add_graph(event_service_dispatcher_status)

    event_service_available_memory = graph(
        'Event Service Available Memory',
        description='Event Service Available Memory',
        unit='bytes',
        min='0',
    )

    event_service_available_memory.add_query(
        expr_sum(
            'ticdc_event_service_available_memory_quota',
            by_labels=['instance', 'changefeed'],
            scope='changefeed',
        ),
        legend='{{instance}}-{{changefeed}}',
        format=None,
    )

    row_builder.add_graph(event_service_available_memory)

    event_service_channel_size = graph(
        'Event Service Channel Size',
        description='',
        unit='short',
        min='0',
    )

    event_service_channel_size.add_query(
        expr_sum_rate(
            'ticdc_event_service_channel_size',
            by_labels=['instance', 'type'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(event_service_channel_size)

    scanned_entry_count_s = graph(
        'Scanned Entry Count / s',
        description='The number of entries scanned by event store.',
        unit='short',
        min='0',
    )

    scanned_entry_count_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_scanned_count_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(scanned_entry_count_s)

    reset_dispatcher_s = graph(
        'Reset Dispatcher / s',
        description='The number of event dispatcher reset operations performed',
        unit='ops',
        min='0',
    )

    reset_dispatcher_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_reset_dispatcher_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        instant=False,
        format=None,
    )

    row_builder.add_graph(reset_dispatcher_s)

    skip_scan_count_s = graph(
        'Skip Scan Count / s',
        description='The rate of skip scan count in eventService',
        unit='short',
        min='0',
    )

    skip_scan_count_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_skip_scan_count_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(skip_scan_count_s)

    intterrupt_scan_count_s = graph(
        'Intterrupt Scan Count / s',
        description='The rate of intterupt scan count in eventService',
        unit='short',
        min='0',
    )

    intterrupt_scan_count_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_interrupt_scan_count_count_sum',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(intterrupt_scan_count_s)

    decode_dmlevent_duration = graph(
        'Decode DMLEvent Duration',
        description='',
        unit='s',
        min='0',
        height=11,
    )

    decode_dmlevent_duration.add_query(
        expr_histogram_avg(
            'ticdc_event_decode_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        instant=False,
    ).add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_event_decode_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p999',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_event_decode_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p95',
        format=None,
    )

    row_builder.add_graph(decode_dmlevent_duration)

    eventservice_output_different_dml_event_types_s = graph(
        'EventService Output Different DML Event Types / s',
        description='The total number of different dml events type that EventService outputs',
        unit='short',
        min='0',
        height=11,
    )

    eventservice_output_different_dml_event_types_s.add_query(
        expr_sum_rate(
            'ticdc_event_service_send_dml_type_count',
            by_labels=['instance', 'dml_type'],
            scope='instance',
        ),
        legend='{{instance}}-{{dml_type}}',
    )

    row_builder.add_graph(eventservice_output_different_dml_event_types_s)

    return row_builder.build()
