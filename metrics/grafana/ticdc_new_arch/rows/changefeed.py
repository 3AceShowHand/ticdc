# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_max,
    expr_sum,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_changefeed_row() -> RowSpec:
    row_builder = row('Changefeed', default_height=7, default_span=12)

    node_table_count = graph(
        'Node Table Count',
        description='The table count of each ticdc node',
        unit='short',
        min='0',
        span=6,
    )

    node_table_count.add_query(
        expr_sum(
            'ticdc_dispatchermanager_table_dispatcher_count',
            by_labels=['instance'],
            scope='changefeed',
        ),
        legend='{{instance}}',
        format='time_series',
    )

    row_builder.add_graph(node_table_count)

    changefeed_table_count = graph(
        'Changefeed Table Count',
        description='The table count of each changefeed',
        unit='short',
        min='0',
        span=6,
    )

    changefeed_table_count.add_query(
        expr_sum(
            'ticdc_dispatchermanager_table_dispatcher_count',
            by_labels=['changefeed'],
            scope='changefeed',
        ),
        legend='{{changefeed}}',
        format='time_series',
    )

    row_builder.add_graph(changefeed_table_count)

    gc_time = graph('GC Time', unit='dateTimeAsIso')

    gc_time.add_query(
        expr_max('ticdc_gc_min_service_gc_safepoint', scope='none'),
        legend='gc time',
        format=None,
    ).add_query(
        expr_max('ticdc_gc_cdc_gc_safepoint', scope='none'),
        legend='cdc service safepoint',
        ref='B',
        format=None,
    )

    row_builder.add_graph(gc_time)

    changefeed_checkpoint = graph(
        'Changefeed Checkpoint',
        description='The checkpoint ts of changefeeds.',
        unit='dateTimeAsIso',
    )

    changefeed_checkpoint.add_query(
        expr_max('pd_cluster_tso', scope='cluster'),
        legend='approximate current time (s)',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_owner_checkpoint_ts',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}',
        ref='B',
        format='time_series',
    )

    row_builder.add_graph(changefeed_checkpoint)

    changefeed_resolved_ts = graph(
        'Changefeed Resolved Ts',
        description='The resolved ts of changefeeds.',
        unit='dateTimeAsIso',
    )

    changefeed_resolved_ts.add_query(
        expr_max('pd_cluster_tso', scope='cluster'),
        legend='approximate current time (s)',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_owner_resolved_ts',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-barrier',
        ref='C',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_scheduler_slow_table_puller_resolved_ts',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-puller',
        ref='B',
        format=None,
    )

    row_builder.add_graph(changefeed_resolved_ts)

    changefeed_checkpoint_lag = graph(
        'Changefeed Checkpoint Lag',
        description='The lag between changefeed checkpoint ts and PD TSO of upstream TiDB.',
        unit='s',
        min='0',
    )

    changefeed_checkpoint_lag.add_query(
        expr_max(
            'ticdc_owner_checkpoint_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}',
        format='time_series',
    )

    row_builder.add_graph(changefeed_checkpoint_lag)

    changefeed_resolved_ts_lag = graph(
        'Changefeed Resolved Ts Lag',
        description='The lag between changefeed resolved ts and PD TSO of upstream TiDB.',
        unit='s',
        min='0',
    )

    changefeed_resolved_ts_lag.add_query(
        expr_max(
            'ticdc_owner_resolved_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-barrier',
        ref='C',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_scheduler_slow_table_puller_resolved_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-puller',
        format=None,
    )

    row_builder.add_graph(changefeed_resolved_ts_lag)

    return row_builder.build()
