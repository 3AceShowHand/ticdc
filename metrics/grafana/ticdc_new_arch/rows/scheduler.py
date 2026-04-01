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
    expr_sum,
    neq,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

REPLICATION_STATE_DETAILS = (
    "0: ReplicationSetStateUnknown means the replication state is unknown, "
    "it should not happen.\n\n"
    "1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n"
    "2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n"
    "3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n"
    "4: ReplicationSetStateReplicating means there is exactly one capture that is "
    "replicating the table.\n\n"
    "5: ReplicationSetStateRemoving means all captures need to stop replication "
    "eventually.\n\n"
)

SLOWEST_TABLE_REPLICATION_STATE_DESCRIPTION = (
    "The state of the slowest table.\n\n"
    f"{REPLICATION_STATE_DETAILS}"
)

TABLE_REPLICATION_STATE_DESCRIPTION = (
    "The total number of tables in different replication states.\n\n"
    f"{REPLICATION_STATE_DETAILS}"
)

def build_scheduler_row() -> RowSpec:
    row_builder = row('Scheduler', default_height=6, default_span=12)

    table_replication_state = graph(
        'Table Replication State',
        description=TABLE_REPLICATION_STATE_DESCRIPTION,
        unit='short',
        min='0',
    )

    table_replication_state.add_query(
        expr_sum(
            'ticdc_scheduler_table_replication_state',
            by_labels=['namespace', 'changefeed', 'state'],
            scope='changefeed',
        ),
    )

    row_builder.add_graph(table_replication_state)

    schedule_tasks = graph(
        'Schedule Tasks',
        description='The total number of different schedule tasks.',
        unit='none',
        min='0',
    )

    schedule_tasks.add_query(
        expr_sum(
            'ticdc_scheduler_task',
            by_labels=['namespace', 'changefeed', 'scheduler', 'task', 'mode'],
            scope='changefeed',
        ),
    )

    row_builder.add_graph(schedule_tasks)

    span_count = graph(
        'Span Count',
        description='The total number of spans',
        unit='short',
    )

    span_count.add_query(
        expr_sum(
            'ticdc_scheduler_span_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
    )

    row_builder.add_graph(span_count)

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
    )

    row_builder.add_graph(table_count)

    operator_count = graph(
        'Operator Count',
        description='The number of current operator count',
        unit='short',
    )

    operator_count.add_query(
        expr_sum(
            'ticdc_maintainer_created_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
            selectors=[eq('type', 'add')],
        ),
        legend_format='add-operator-{{namespace}}-{{changefeed}}-{{mode}}',
    ).add_query(
        expr_sum(
            'ticdc_maintainer_created_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
            selectors=[eq('type', 'move')],
        ),
        legend_format='move-operator-{{namespace}}-{{changefeed}}-{{mode}}',
        format=None,
    ).add_query(
        expr_sum(
            'ticdc_maintainer_created_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
            selectors=[eq('type', 'split')],
        ),
        legend_format='split-operator-{{namespace}}-{{changefeed}}-{{mode}}',
        format=None,
    ).add_query(
        expr_sum(
            'ticdc_maintainer_created_count',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
            selectors=[eq('type', 'merge')],
        ),
        legend_format='merge-operator-{{namespace}}-{{changefeed}}-{{mode}}',
        format=None,
    )

    row_builder.add_graph(operator_count)

    total_operator_count = graph(
        'Total Operator Count',
        description='The number of total operator count ',
        unit='short',
    )

    total_operator_count.add_query(
        expr_sum(
            'ticdc_maintainer_total_operator_count',
            by_labels=['namespace', 'changefeed', 'type', 'mode'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('changefeed', '$changefeed'),
                regex('instance', '$ticdc_instance'),
                neq('type', 'occupy'),
            ],
        ),
        legend_format='{{type}}-{{namespace}}-{{changefeed}}-{{mode}}',
    )

    row_builder.add_graph(total_operator_count)

    split_span_check_duration = graph(
        'Split Span Check Duration',
        description='duration for split span do once check',
        unit='s',
    )

    split_span_check_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_maintainer_split_span_check_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('changefeed', '$changefeed'),
                regex('instance', '$ticdc_instance'),
            ],
        ),
        legend_format='99.9-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_maintainer_split_span_check_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='cluster',
            selectors=[
                regex('namespace', '$namespace'),
                regex('changefeed', '$changefeed'),
                regex('instance', '$ticdc_instance'),
            ],
        ),
        legend_format='avg-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(split_span_check_duration)

    operator_cost_duration = graph(
        'Operator Cost Duration',
        description='duration for each operator',
        unit='s',
    )

    operator_cost_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_maintainer_finish_operators_duration_seconds',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend_format='99.9-{{namespace}}-{{changefeed}}-{{instance}}-{{mode}}',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_maintainer_finish_operators_duration_seconds',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend_format='avg-{{namespace}}-{{changefeed}}-{{instance}}-{{mode}}',
        format=None,
    )

    row_builder.add_graph(operator_cost_duration)

    slowest_table_checkpoint = graph(
        'Slowest Table Checkpoint',
        description='The checkpoint ts of the slowest table.',
        unit='dateTimeAsIso',
    )

    slowest_table_checkpoint.add_query(
        expr_max('pd_cluster_tso', scope='cluster'),
        legend_format='approximate current time (s)',
    ).add_query(
        expr_max(
            'ticdc_scheduler_slow_table_checkpoint_ts',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend_format='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(slowest_table_checkpoint)

    slowest_table_id = graph(
        'Slowest Table ID',
        description='The ID of the slowest table',
        unit='none',
        min='0',
    )

    slowest_table_id.add_query(
        expr_sum(
            'ticdc_scheduler_slow_table_id',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend_format='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(slowest_table_id)

    slowest_table_replication_state = graph(
        'Slowest Table Replication State',
        description=SLOWEST_TABLE_REPLICATION_STATE_DESCRIPTION,
        unit='none',
        min='0',
    )

    slowest_table_replication_state.add_query(
        expr_sum(
            'ticdc_scheduler_slow_table_replication_state',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
    )

    row_builder.add_graph(slowest_table_replication_state)

    slowest_table_resolved_ts = graph(
        'Slowest Table Resolved Ts',
        description='The resolved ts of the slowest table.',
        unit='dateTimeAsIso',
    )

    slowest_table_resolved_ts.add_query(
        expr_max('pd_cluster_tso', scope='cluster'),
        legend_format='approximate current time (s)',
    ).add_query(
        expr_max(
            'ticdc_scheduler_slow_table_resolved_ts',
            by_labels=['namespace', 'changefeed'],
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend_format='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(slowest_table_resolved_ts)

    return row_builder.build()
