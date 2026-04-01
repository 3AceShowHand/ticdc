# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_max,
    expr_over_time,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    legend_for,
    regex,
    transformation,
)
from metrics.grafana.ticdc_new_arch.builders import graph, table, timeseries, row

def build_summary_row() -> RowSpec:
    row_builder = row('Summary', default_height=6, default_span=12)

    changefeed_checkpoint_lag = graph(
        'Changefeed Checkpoint Lag',
        description='The lag between changefeed coordinator checkpoint ts and the tso of upstream TiDB.',
        unit='s',
        min='0',
        height=7,
    )

    changefeed_checkpoint_lag.add_query(
        expr_max(
            'ticdc_owner_checkpoint_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend_format=legend_for('namespace', 'changefeed'),
    )

    row_builder.add_graph(changefeed_checkpoint_lag)

    changefeed_resolved_ts_lag = graph(
        'Changefeed Resolved Ts Lag',
        description='The lag between changefeed coordinator resolved ts and the tso of upstream TiDB.',
        unit='s',
        min='0',
        height=7,
    )

    changefeed_resolved_ts_lag.add_query(
        expr_max(
            'ticdc_owner_resolved_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend_format='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(changefeed_resolved_ts_lag)

    upstream_write_bytes_s = graph(
        'Upstream Write Bytes / s',
        description='Represents the total amount of data written by the upstream cluster of TiCDC, not the total amount of data that CDC needs to pull.',
        unit='binBps',
        min='0',
    )

    upstream_write_bytes_s.add_query(
        expr_sum_rate(
            'tidb_tikvclient_txn_write_size_bytes_sum',
            scope='cluster',
            selectors=[regex('scope', 'general')],
            window='30s',
        ),
        legend_format='sum',
        format=None,
    )

    row_builder.add_graph(upstream_write_bytes_s)

    ticdc_input_bytes_s = graph(
        'TiCDC Input Bytes / s',
        description='The number of bytes written into the event store pebble storage',
        unit='binBps',
        min='0',
    )

    ticdc_input_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_event_store_write_bytes',
            by_labels=['instance'],
            scope='instance',
        ),
        legend_format=legend_for('instance'),
        format=None,
    )

    row_builder.add_graph(ticdc_input_bytes_s)

    sink_event_row_count_s = graph(
        'Sink Event Row Count / s',
        description='The number of dml events that sink flushes to downstream per second.',
        unit='short',
        min='0',
    )

    sink_event_row_count_s.add_query(
        expr_sum_rate(
            'ticdc_sink_dml_event_count',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('changefeed', '$changefeed'),
                regex('instance', '$ticdc_instance'),
            ],
        ),
        legend_format='{{namespace}}-{{changefeed}}-{{instance}}',
    )

    row_builder.add_graph(sink_event_row_count_s)

    sink_write_bytes_s = graph('Sink Write Bytes / s', unit='binBps', min='0')

    sink_write_bytes_s.add_query(
        expr_sum_rate(
            'ticdc_sink_write_bytes_total',
            by_labels=['instance', 'type', 'changefeed'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('changefeed', '$changefeed'),
                regex('instance', '$ticdc_instance'),
            ],
        ),
        legend_format='{{instance}}-{{changefeed}}-{{type}}',
        format=None,
    ).add_query(
        expr_over_time(
            'avg_over_time',
            expr_sum_rate(
                'ticdc_sink_write_bytes_total',
                by_labels=['instance', 'type', 'changefeed'],
                scope='cluster',
                selectors=[
                    regex('namespace', '$namespace'),
                    regex('changefeed', '$changefeed'),
                    regex('instance', '$ticdc_instance'),
                ],
            ),
            window='1m:',
        ),
        legend_format='{{instance}}-{{changefeed}}-AVG',
        hide=True,
        format=None,
    )

    row_builder.add_graph(sink_write_bytes_s)

    the_status_of_changefeeds = timeseries(
        'The Status of Changefeeds',
        description='The status of each changefeed.\n\n0: Normal\n\n1: Pending\n\n2: Failed\n\n3: Stopped\n\n4: Finished\n\n5: Removed\n\n6: Warning\n\n7: Uninitialized\n\n-1: Unknown',
        unit='short',
        min=0,
    )

    the_status_of_changefeeds.add_query(
        expr_max(
            'ticdc_owner_status',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend_format='{{namespace}}-{{changefeed}}',
        instant=False,
    )

    row_builder.add_timeseries(the_status_of_changefeeds)

    table_dispatcher_count = graph('Table Dispatcher Count', description='', unit='short', min='0')

    table_dispatcher_count.add_query(
        expr_sum(
            'ticdc_dispatchermanager_table_dispatcher_count',
            by_labels=['instance', 'changefeed', 'event_type'],
            scope='changefeed',
        ),
        legend_format='{{instance}}-{{changefeed}}-{{event_type}}',
    )

    row_builder.add_graph(table_dispatcher_count)

    memory_quota = graph(
        'Memory Quota',
        description='Changefeed memory quota',
        unit='bytes',
        min='0',
    )

    memory_quota.add_query(
        expr_sum(
            'ticdc_dynamic_stream_memory_usage',
            by_labels=['namespace', 'area', 'instance', 'module', 'type'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('area', '$changefeed'),
                regex('instance', '$ticdc_instance'),
                regex('module', 'event-collector'),
            ],
        ),
        legend='{{namespace}}-{{area}}-{{instance}}-{{module}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(memory_quota)

    table_count = graph(
        'Table Count',
        description='The total number of tables',
        unit='short',
    )

    table_count.add_query(
        expr_sum(
            'ticdc_scheduler_table_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend_format='{{namespace}}-{{changefeed}}-{{mode}}',
    )

    row_builder.add_graph(table_count)

    changefeed_error_details = table(
        'Changefeed Error Details',
        description='Current warning or failed reason of each changefeed. The metric message is normalized to a single line and truncated to 256 characters.',
        span=24,
        height=8,
    )

    changefeed_error_details.add_query(
        expr_simple(
            'max by (namespace, changefeed, state, code, message) (ticdc_owner_changefeed_error_info{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"})',
            scope='none',
        ),
        format='time_series',
        instant=True,
    )

    changefeed_error_details.add_transformation(
        transformation('labelsToFields', {}),
    ).add_transformation(
        transformation(
            'organize',
            {
                'excludeByName': {'Metric': True, 'Time': True, 'Value': True, '__name__': True},
                'indexByName': {'changefeed': 1, 'code': 3, 'message': 4, 'namespace': 0, 'state': 2},
                'renameByName': {},
            },
        ),
    )

    row_builder.add_table(changefeed_error_details)

    return row_builder.build()
