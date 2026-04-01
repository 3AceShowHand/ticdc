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
    expr_sum_increase,
    expr_sum_rate,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_event_store_row() -> RowSpec:
    row_builder = row('Event Store', default_height=8, default_span=12)

    resolved_ts_lag = graph('Resolved Ts Lag', unit='s', min='0')

    resolved_ts_lag.add_query(
        expr_simple('ticdc_event_store_resolved_ts_lag', scope='instance'),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(resolved_ts_lag)

    register_dispatcher_startts_lag = graph(
        'Register Dispatcher StartTs Lag',
        description='The lag of startTs when registering a dispatcher.',
        unit='s',
        min='0',
    )

    register_dispatcher_startts_lag.add_query(
        expr_histogram_quantile(
            1.0,
            'ticdc_event_store_register_dispatcher_start_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-max',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_event_store_register_dispatcher_start_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p95',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.8,
            'ticdc_event_store_register_dispatcher_start_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p80',
        hide=True,
        format=None,
    )

    row_builder.add_graph(register_dispatcher_startts_lag)

    subscriptions_resolved_ts_lag = graph(
        'Subscriptions Resolved Ts Lag',
        description='The Resolved Ts lag of subscriptions for event store.',
        unit='s',
        min='0',
    )

    subscriptions_resolved_ts_lag.add_query(
        expr_histogram_quantile(
            1,
            'ticdc_event_store_subscription_resolved_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-max',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_event_store_subscription_resolved_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p95',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.8,
            'ticdc_event_store_subscription_resolved_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p80',
        hide=True,
        format=None,
    )

    row_builder.add_graph(subscriptions_resolved_ts_lag)

    subscriptions_data_gc_lag = graph(
        'Subscriptions Data GC Lag',
        description='The data gc lag of subscriptions for event store.',
        unit='s',
        min='0',
    )

    subscriptions_data_gc_lag.add_query(
        expr_histogram_quantile(
            1,
            'ticdc_event_store_subscription_data_gc_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-max',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_event_store_subscription_data_gc_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p95',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.8,
            'ticdc_event_store_subscription_data_gc_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p80',
        hide=True,
        format=None,
    )

    row_builder.add_graph(subscriptions_data_gc_lag)

    input_event_count_s = graph(
        'Input Event Count / s',
        description='The number of events received by event store.',
        unit='short',
        min='0',
    )

    input_event_count_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_input_event_count',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}',
    )

    row_builder.add_graph(input_event_count_s)

    input_bytes_s = graph(
        'Input Bytes / s',
        description='The number of bytes written by event store.',
        unit='binBps',
        min='0',
    )

    input_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_write_bytes',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(input_bytes_s)

    write_requests_s = graph(
        'Write Requests / s',
        description='The number of write requests received by event store',
        unit='short',
        min='0',
    )

    write_requests_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_write_requests_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(write_requests_s)

    write_worker_busy_ratio = graph(
        'Write Worker Busy Ratio',
        description='Busy ratio for event store write worker.',
        unit='percent',
    )

    write_worker_busy_ratio.add_query(
        f'{expr_sum_rate('ticdc_event_store_write_worker_io_duration_sum', by_labels=['instance', 'db', 'worker'], scope='instance')} / {expr_sum_rate('ticdc_event_store_write_worker_total_duration_sum', by_labels=['instance', 'db', 'worker'], scope='instance')} * 100',
        legend='{{instance}}-db-{{db}}-worker-{{worker}}',
        format=None,
    )

    row_builder.add_graph(write_worker_busy_ratio)

    compressed_rows_s = graph(
        'Compressed Rows / s',
        description='The number of rows compressed by event store per second.',
        unit='short',
        min='0',
    )

    compressed_rows_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_compressed_rows_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(compressed_rows_s)

    write_duration = graph(
        'Write Duration',
        description='The time of commit batch to sorter',
        unit='s',
    )

    write_duration.add_query(
        expr_histogram_quantile(
            1,
            'ticdc_event_store_write_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-max',
        instant=False,
    ).add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_write_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p99',
        hide=True,
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_event_store_write_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(write_duration)

    write_queue_duration = graph(
        'Write Queue Duration',
        description="Each event's duration staying in write queue",
        unit='s',
        min='0',
    )

    write_queue_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_write_queue_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p99',
        format='heatmap',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'ticdc_event_store_write_queue_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(write_queue_duration)

    write_prepare_duration = graph('Write Prepare Duration', description='', unit='s', min='0')

    write_prepare_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_write_prepare_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p99',
        format='heatmap',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'ticdc_event_store_write_prepare_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(write_prepare_duration)

    write_batch_size = heatmap('Write Batch Size', unit='bytes')

    write_batch_size.add_query(
        expr_sum_increase(
            'ticdc_event_store_write_batch_size_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
    )

    row_builder.add_heatmap(write_batch_size)

    write_batch_event_count = heatmap('Write Batch Event Count', unit='short')

    write_batch_event_count.add_query(
        expr_sum_increase(
            'ticdc_event_store_write_batch_events_count_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
    )

    row_builder.add_heatmap(write_batch_event_count)

    data_size_on_disk = graph(
        'Data Size On Disk',
        description='The amount of pending data stored on-disk for event store',
        unit='bytes',
    )

    data_size_on_disk.add_query(
        expr_sum(
            'ticdc_event_store_on_disk_data_size',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(data_size_on_disk)

    data_size_in_memory = graph(
        'Data Size In Memory',
        description='The amount of pending data stored in-memory for event store',
        unit='bytes',
    )

    data_size_in_memory.add_query(
        expr_sum(
            'ticdc_event_store_in_memory_data_size',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(data_size_in_memory)

    scan_requests_s = graph(
        'Scan Requests / s',
        description='The number of scan requests received by event store',
        unit='ops',
        min='0',
    )

    scan_requests_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_scan_requests_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        instant=False,
        format=None,
    )

    row_builder.add_graph(scan_requests_s)

    scan_bytes_s = graph(
        'Scan Bytes / s',
        description='The number of bytes scanned by event store.',
        unit='binBps',
        min='0',
    )

    scan_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_scan_bytes',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(scan_bytes_s)

    subscription_num = graph(
        'Subscription Num',
        description='The number of subscriptions created by event store.',
        unit='short',
        min='0',
    )

    subscription_num.add_query(
        expr_simple('ticdc_event_store_subscription_num', scope='instance'),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(subscription_num)

    scan_operation_duration = graph(
        'Scan Operation Duration ',
        description='The time of event store iterator scan operation duration',
        unit='s',
        min='0',
    )

    scan_operation_duration.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_read_duration',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-p99',
        instant=False,
    )

    row_builder.add_graph(scan_operation_duration)

    pebble_block_cache_access_s = graph(
        'pebble block cache access /s',
        description='The number of scan requests received by event store',
        unit='ops',
        min='0',
    )

    pebble_block_cache_access_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_pebble_block_cache_access_total',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}',
        instant=False,
        format=None,
    )

    row_builder.add_graph(pebble_block_cache_access_s)

    pebble_block_cache_hit_ratio = graph(
        'pebble block cache hit ratio',
        description='',
        unit='percent',
        min='0',
    )

    pebble_block_cache_hit_ratio.add_query(
        f'{expr_sum_rate('ticdc_event_store_pebble_block_cache_access_total', by_labels=['instance'], scope='instance', selectors=[eq('type', 'hit')])} / {expr_sum_rate('ticdc_event_store_pebble_block_cache_access_total', by_labels=['instance'], scope='instance', selectors=[regex('type', 'hit|miss')])}',
        legend='{{instance}}',
        instant=False,
    )

    row_builder.add_graph(pebble_block_cache_hit_ratio)

    pebble_compaction_duration_seconds = graph(
        'pebble compaction duration seconds',
        description='',
        unit='s',
        min='0',
    )

    pebble_compaction_duration_seconds.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_pebble_compaction_duration_seconds',
            by_labels=['instance', 'id'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-p99',
        instant=False,
    )

    row_builder.add_graph(pebble_compaction_duration_seconds)

    pebble_flush_duration_seconds = graph(
        'pebble flush duration seconds',
        description='',
        unit='s',
        min='0',
    )

    pebble_flush_duration_seconds.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_pebble_flush_duration_seconds',
            by_labels=['instance', 'id'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-p99',
        instant=False,
    )

    row_builder.add_graph(pebble_flush_duration_seconds)

    pebble_compaction_bytes = graph('pebble compaction bytes', description='', unit='bytes')

    pebble_compaction_bytes.add_query(
        expr_max(
            'ticdc_event_store_pebble_compaction_debt_bytes',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='debt-{{instance}}',
        format=None,
    ).add_query(
        expr_max(
            'ticdc_event_store_pebble_compaction_in_progress',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='inprogress-{{instance}}',
        format=None,
    )

    row_builder.add_graph(pebble_compaction_bytes)

    pebble_flush_duration_seconds_2 = graph(
        'pebble flush duration seconds',
        description='',
        unit='s',
        min='0',
    )

    pebble_flush_duration_seconds_2.add_query(
        expr_histogram_quantile(
            0.99,
            'ticdc_event_store_pebble_flush_duration_seconds',
            by_labels=['instance', 'id'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}-p99',
        instant=False,
    )

    row_builder.add_graph(pebble_flush_duration_seconds_2)

    pebble_write_stall_s = graph(
        'pebble write stall / s',
        description='The number of scan requests received by event store',
        unit='ops',
        min='0',
    )

    pebble_write_stall_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_pebble_write_stall_total',
            by_labels=['instance', 'id', 'reason'],
            scope='instance',
        ),
        legend='{{instance}}',
        instant=False,
        format=None,
    )

    row_builder.add_graph(pebble_write_stall_s)

    pebble_level_files = graph(
        'pebble level files',
        description='The number of subscriptions created by event store.',
        unit='short',
        min='0',
    )

    pebble_level_files.add_query(
        expr_max(
            'ticdc_event_store_pebble_level_files',
            by_labels=['instance', 'level'],
            scope='instance',
        ),
        legend='{{instance}}-{{level}}',
        format=None,
    )

    row_builder.add_graph(pebble_level_files)

    return row_builder.build()
