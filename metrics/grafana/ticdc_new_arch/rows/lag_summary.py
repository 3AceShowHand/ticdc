# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    eq,
    expr_max,
    expr_simple,
    expr_sum,
    histogram_heatmap_panel,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_lag_summary_row() -> RowSpec:
    row_builder = row('Lag Summary', default_height=6, default_span=12)

    maintainer_checkpoint_lag = graph(
        'Maintainer Checkpoint Lag',
        description='The lag between changefeed checkpoint ts and the lac1 ts of upstream TiDB.',
        unit='s',
        min='0',
        height=5,
    )

    maintainer_checkpoint_lag.add_query(
        expr_max(
            'ticdc_maintainer_checkpoint_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(maintainer_checkpoint_lag)

    maintainer_resolved_ts_lag = graph(
        'Maintainer Resolved Ts lag',
        description='The lag between maintainer resolved ts and PD TSO of upstream TiDB.',
        unit='s',
        min='0',
        height=5,
    )

    maintainer_resolved_ts_lag.add_query(
        expr_max(
            'ticdc_maintainer_resolved_ts_lag',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-resolvedts',
    )

    row_builder.add_graph(maintainer_resolved_ts_lag)

    eventstore_resolved_ts_lag = graph('EventStore Resolved Ts Lag ', description='', unit='s', min='0')

    eventstore_resolved_ts_lag.add_query(
        expr_sum(
            'ticdc_event_store_resolved_ts_lag',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(eventstore_resolved_ts_lag)

    eventservice_resolved_ts_lag = graph(
        'EventService Resolved Ts Lag ',
        description='',
        unit='s',
        min='0',
    )

    eventservice_resolved_ts_lag.add_query(
        expr_sum(
            'ticdc_event_service_resolved_ts_lag',
            by_labels=['instance', 'type'],
            scope='instance',
            selectors=[regex('type', 'received')],
        ),
        legend='{{type}}-{{instance}}',
        format=None,
    ).add_query(
        expr_sum(
            'ticdc_event_service_resolved_ts_lag',
            by_labels=['instance', 'type'],
            scope='instance',
            selectors=[regex('type', 'sent')],
        ),
        legend='{{type}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(eventservice_resolved_ts_lag)

    dispatchermanager_checkpoint_lag = graph(
        'DispatcherManager Checkpoint Lag',
        description='',
        unit='s',
        min='0',
    )

    dispatchermanager_checkpoint_lag.add_query(
        expr_simple(
            'ticdc_dispatchermanager_checkpoint_ts_lag',
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-checkpointTsLag',
        ref='B',
    )

    row_builder.add_graph(dispatchermanager_checkpoint_lag)

    dispatchermanager_resolved_ts_lag = graph(
        'DispatcherManager Resolved Ts Lag',
        description='',
        unit='s',
        min='0',
    )

    dispatchermanager_resolved_ts_lag.add_query(
        expr_simple(
            'ticdc_dispatchermanager_resolved_ts_lag',
            scope='cluster',
            selectors=[regex('namespace', '$namespace'), regex('changefeed', '$changefeed')],
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-resolvedts',
        ref='B',
    )

    row_builder.add_graph(dispatchermanager_resolved_ts_lag)

    eventcollector_resolved_ts_lag = histogram_heatmap_panel(
        'EventCollector Resolved Ts Lag',
        metric='ticdc_dispatcher_received_event_lag_duration',
        matchers=[
            eq('k8s_cluster', '$k8s_cluster'),
            eq('tidb_cluster', '$tidb_cluster'),
            regex('instance', '$ticdc_instance'),
        ],
        description='',
        unit='ms',
        width=12,
        height=6,
    )

    row_builder.add_heatmap(eventcollector_resolved_ts_lag)

    return row_builder.build()
