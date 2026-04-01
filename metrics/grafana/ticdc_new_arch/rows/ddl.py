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
    expr_sum_delta,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_ddl_row() -> RowSpec:
    row_builder = row('DDL', default_height=8, default_span=12)

    output_ddl_executing_duration = graph(
        'Output DDL Executing Duration',
        description='DDL executing duration',
        unit='s',
        min='0',
    )

    output_ddl_executing_duration.add_query(
        expr_histogram_avg(
            'ticdc_ddl_exec_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='avg-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_ddl_exec_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='99.9-duration-{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(output_ddl_executing_duration)

    sink_running_ddl_count = graph(
        'Sink Running DDL Count',
        description='Count of running DDL.',
        unit='short',
        min='0',
    )

    sink_running_ddl_count.add_query(
        expr_sum(
            'ticdc_ddl_exec_running',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}',
    )

    row_builder.add_graph(sink_running_ddl_count)

    maintainer_blocking_ddl_count = graph(
        'Maintainer Blocking DDL Count',
        description='Count of blocking DDL.',
        unit='short',
        min='0',
    )

    maintainer_blocking_ddl_count.add_query(
        expr_sum(
            'ticdc_ddl_exec_blocking',
            by_labels=['namespace', 'changefeed', 'mode'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{mode}}',
    )

    row_builder.add_graph(maintainer_blocking_ddl_count)

    sink_ddl_count_m = graph(
        'Sink DDL Count / m',
        description='Execution count of different DDL types in the last minute.',
        unit='short',
        min='0',
    )

    sink_ddl_count_m.add_query(
        expr_sum_delta(
            'ticdc_ddl_execution',
            by_labels=['namespace', 'changefeed', 'ddl_type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{ddl_type}}',
        format=None,
    )

    row_builder.add_graph(sink_ddl_count_m)

    handle_ddl_duration = heatmap(
        'Handle DDL Duration',
        description='DDL handling duration distribution.',
        unit='s',
    )

    handle_ddl_duration.add_query(
        expr_sum_rate(
            'ticdc_ddl_handle_duration_bucket',
            by_labels=['le'],
            scope='changefeed',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(handle_ddl_duration)

    return row_builder.build()
