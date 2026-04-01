# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_log_puller_row() -> RowSpec:
    row_builder = row('Log Puller', default_height=8, default_span=12)

    input_events_s = graph(
        'Input Events / s',
        description='The number of KV client dispatched event per second',
        unit='short',
        min='0',
    )

    input_events_s.add_query(
        expr_sum_rate(
            'ticdc_kvclient_pull_event_count',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}',
        ref='B',
    )

    row_builder.add_graph(input_events_s)

    unresolved_region_request_count = graph(
        'Unresolved Region Request Count ',
        description='To prevent excessive accumulation of region request tasks on the TiKV side, CDC will implement rate limiting on the number of requests it initiates.',
        unit='short',
        min='0',
    )

    unresolved_region_request_count.add_query(
        expr_simple(
            'ticdc_subscription_client_requested_region_count',
            scope='instance',
        ),
        legend='{{instance}}-count',
        format=None,
    )

    row_builder.add_graph(unresolved_region_request_count)

    region_request_finish_scan_duration = graph(
        'Region Request Finish Scan Duration',
        description='',
        unit='s',
        min='0',
    )

    region_request_finish_scan_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_subscription_client_region_request_finish_scan_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p99',
        format='heatmap',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'ticdc_subscription_client_region_request_finish_scan_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(region_request_finish_scan_duration)

    subscribed_region_count = graph(
        'Subscribed Region Count',
        description='To prevent excessive accumulation of region request tasks on the TiKV side, CDC will implement rate limiting on the number of requests it initiates.',
        unit='short',
        min='0',
    )

    subscribed_region_count.add_query(
        expr_simple(
            'ticdc_subscription_client_subscribed_region_count',
            scope='instance',
        ),
        legend='{{instance}}-count',
        format=None,
    )

    row_builder.add_graph(subscribed_region_count)

    memory_quota = graph(
        'Memory Quota',
        description='Log puller memory quota',
        unit='bytes',
        min='0',
    )

    memory_quota.add_query(
        expr_sum(
            'ticdc_dynamic_stream_memory_usage',
            by_labels=['instance', 'type'],
            scope='cluster',
            selectors=[regex('instance', '$ticdc_instance'), regex('module', 'log-puller')],
        ),
        legend='{{instance}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(memory_quota)

    resolved_ts_batch_size_regions = heatmap(
        'Resolved Ts Batch Size (Regions)',
        description='The size of batch resolved regions count',
        unit='none',
    )

    resolved_ts_batch_size_regions.add_query(
        expr_sum_rate(
            'ticdc_kvclient_batch_resolved_event_size_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(resolved_ts_batch_size_regions)

    region_event_handle_duration = graph('Region Event Handle Duration', description='', unit='s', min='0')

    region_event_handle_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_subscription_client_region_event_handle_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p99',
        format='heatmap',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'ticdc_subscription_client_region_event_handle_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(region_event_handle_duration)

    region_event_consume_callback_duration = graph(
        'Region Event Consume Callback Duration',
        description='',
        unit='s',
        min='0',
    )

    region_event_consume_callback_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_subscription_client_consume_kv_events_callback_duration',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-p99',
        format='heatmap',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'ticdc_subscription_client_consume_kv_events_callback_duration',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-avg',
        format=None,
    )

    row_builder.add_graph(region_event_consume_callback_duration)

    dropped_resolve_lock_tasks_s = graph(
        'Dropped Resolve Lock Tasks / s',
        description='Dropped resolve lock tasks when resolveLockTaskCh is full.',
        unit='ops',
        min='0',
    )

    dropped_resolve_lock_tasks_s.add_query(
        expr_sum_rate(
            'ticdc_subscription_client_resolve_lock_task_drop_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(dropped_resolve_lock_tasks_s)

    return row_builder.build()
