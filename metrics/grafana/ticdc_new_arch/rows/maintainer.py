# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_sum,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_maintainer_row() -> RowSpec:
    row_builder = row('Maintainer', default_height=5, default_span=12)

    maintainer_checkpoint_lag = graph(
        'Maintainer Checkpoint Lag',
        description='The lag between maintainer checkpoint ts and PD TSO of upstream TiDB.',
        unit='s',
        min='0',
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
        'Maintainer Resolved Ts Lag',
        description='The lag between maintainer resolved ts and PD TSO of upstream TiDB.',
        unit='s',
        min='0',
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

    changefeed_maintainer_count = graph(
        'Changefeed Maintainer Count',
        description='',
        unit='none',
        min='0',
        height=6,
    )

    changefeed_maintainer_count.add_query(
        expr_sum(
            'ticdc_changefeed_maintainer_counter',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}',
        ref='B',
    )

    row_builder.add_graph(changefeed_maintainer_count)

    maintainer_handle_event_duration = graph('Maintainer Handle Event Duration', unit='s', min='0', height=6)

    maintainer_handle_event_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_maintainer_handle_event_duration',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='99.9-{{namespace}}-{{changefeed}}',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_maintainer_handle_event_duration',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='avg-{{namespace}}-{{changefeed}}',
        format=None,
    )

    row_builder.add_graph(maintainer_handle_event_duration)

    maintainer_event_channel_length = graph(
        'Maintainer Event Channel Length',
        description='Length of maintainer event channel.',
        unit='short',
        min='0',
    )

    maintainer_event_channel_length.add_query(
        expr_max(
            'ticdc_maintainer_event_ch_len',
            by_labels=['namespace', 'changefeed'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}',
    )

    row_builder.add_graph(maintainer_event_channel_length)

    return row_builder.build()
