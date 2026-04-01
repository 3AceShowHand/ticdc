# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_sum_increase,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_sink_transaction_row() -> RowSpec:
    row_builder = row('Sink - Transaction Sink', default_height=8, default_span=12)

    conflict_detect_duration = graph(
        'Conflict Detect Duration',
        description='Duration of event staying in conflict detector',
        unit='s',
    )

    conflict_detect_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_txn_conflict_detect_duration',
            by_labels=['changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-detect-P999',
        ref='C',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_txn_conflict_detect_duration',
            by_labels=['changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-detect-avg',
        ref='D',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_txn_queue_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-queue-P999',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_txn_queue_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-queue-avg',
        ref='B',
        format=None,
    )

    row_builder.add_graph(conflict_detect_duration)

    full_flush_duration = graph(
        'Full Flush Duration',
        description='Full flush (backend flush + callback + conflict detector notify) duration',
        unit='s',
    )

    full_flush_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_txn_worker_flush_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='99.9-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_txn_worker_flush_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='avg-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(full_flush_duration)

    worker_busy_ratio = graph(
        'Worker Busy Ratio',
        description='Sink worker busy ratio',
        unit='percent',
        min='0',
    )

    worker_busy_ratio.add_query(
        f'{expr_sum_rate('ticdc_sink_txn_worker_batch_flush_duration_sum', by_labels=['namespace', 'changefeed', 'instance', 'id'], scope='changefeed')} / {expr_sum_rate('ticdc_sink_txn_worker_total_duration_sum', by_labels=['namespace', 'changefeed', 'instance', 'id'], scope='changefeed')} * 100',
        legend='{{namespace}}-{{changefeed}}-{{instance}}-worker-{{id}}',
        format=None,
    )

    row_builder.add_graph(worker_busy_ratio)

    worker_input_rows_s = graph('Worker Input Rows / s', description='', unit='short', min='0')

    worker_input_rows_s.add_query(
        expr_sum_rate(
            'ticdc_sink_txn_worker_handled_rows',
            by_labels=['namespace', 'changefeed', 'instance', 'id'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{id}}',
        format=None,
    )

    row_builder.add_graph(worker_input_rows_s)

    backend_flush_duration = graph(
        'Backend Flush Duration',
        description='Distribution of flush transaction duration to backend',
        unit='s',
        min='0',
    )

    backend_flush_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_txn_sink_dml_batch_commit',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='99.9-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_txn_sink_dml_batch_commit',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='avg-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(backend_flush_duration)

    row_affected_count_m = graph(
        'Row Affected Count / m',
        description='The number of affected rows',
        unit='short',
        min='0',
    )

    row_affected_count_m.add_query(
        expr_sum_increase(
            'ticdc_sink_dml_event_affected_row_count',
            by_labels=['namespace', 'changefeed', 'count_type', 'row_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{count_type}}-{{row_type}}',
    )

    row_builder.add_graph(row_affected_count_m)

    return row_builder.build()
