# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_histogram_quantile,
    expr_simple,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, heatmap, row

def build_schema_store_row() -> RowSpec:
    row_builder = row('Schema Store', default_height=8, default_span=12)

    resolved_ts_lag = graph('Resolved Ts Lag', unit='s', min='0')

    resolved_ts_lag.add_query(
        expr_simple('ticdc_schema_store_resolved_ts_lag', scope='instance'),
        legend='{{instance}}-resolvedts',
        format=None,
    )

    row_builder.add_graph(resolved_ts_lag)

    register_table_num = graph('Register Table Num', unit='short', min='0')

    register_table_num.add_query(
        expr_simple('ticdc_schema_store_register_table_num', scope='instance'),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(register_table_num)

    get_table_info_count_s = graph('Get Table Info Count / s', unit='short', min='0')

    get_table_info_count_s.add_query(
        expr_sum_rate(
            'ticdc_schema_store_get_table_info_count',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(get_table_info_count_s)

    get_table_info_duration = heatmap('Get Table Info Duration', unit='s')

    get_table_info_duration.add_query(
        expr_sum_rate(
            'ticdc_schema_store_get_table_info_duration_bucket',
            by_labels=['le'],
            scope='instance',
        ),
        legend='{{le}}',
        instant=False,
    )

    row_builder.add_heatmap(get_table_info_duration)

    shared_column_schema_count = graph('Shared Column Schema Count', unit='short', min='0')

    shared_column_schema_count.add_query(
        expr_simple('ticdc_common_shared_column_schema_count', scope='instance'),
        legend='{{instance}}',
        format=None,
    )

    row_builder.add_graph(shared_column_schema_count)

    wait_resolved_ts_duration = graph(
        'Wait Resolved Ts Duration',
        description='The duration of waiting for resolved ts in schema store. It shows the p80, p95, and max latency.',
        unit='s',
        min='0',
    )

    wait_resolved_ts_duration.add_query(
        expr_histogram_quantile(
            0.8,
            'ticdc_schema_store_wait_resolved_ts_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p80',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            0.95,
            'ticdc_schema_store_wait_resolved_ts_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-p95',
        format=None,
    ).add_query(
        expr_histogram_quantile(
            1.0,
            'ticdc_schema_store_wait_resolved_ts_duration',
            by_labels=['instance'],
            scope='instance',
        ),
        legend='{{instance}}-max',
        format=None,
    )

    row_builder.add_graph(wait_resolved_ts_duration)

    return row_builder.build()
