# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_sum,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_coordinator_row() -> RowSpec:
    row_builder = row('Coordinator', default_height=6, default_span=12)

    changefeed_status = graph(
        'Changefeed Status',
        description='The number of changefeed in different status\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n',
        unit='short',
        min='0',
    )

    changefeed_status.add_query(
        expr_sum(
            'ticdc_coordinator_changefeed_state',
            by_labels=['namespace', 'changefeed', 'state'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{state}}',
    )

    row_builder.add_graph(changefeed_status)

    coordinator_operator_cost_duration = graph(
        'Coordinator Operator Cost Duration',
        description='duration for each operator for changefeed',
        unit='s',
    )

    coordinator_operator_cost_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_coordinator_finish_operators_duration_seconds',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend='99.9',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_coordinator_finish_operators_duration_seconds',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend='avg',
        format=None,
    )

    row_builder.add_graph(coordinator_operator_cost_duration)

    coordinator_history = graph(
        'Coordinator History',
        description='The history of TiCDC cluster coordinator, owner node has a value that is great than 0',
        unit='none',
        min='0',
        height=7,
    )

    coordinator_history.add_query(
        expr_sum_rate(
            'ticdc_owner_ownership_counter',
            by_labels=['instance'],
            scope='instance',
            window='240s',
        ).op('> BOOL', '0.5'),
        legend='{{instance}}',
    )

    row_builder.add_graph(coordinator_history)

    return row_builder.build()
