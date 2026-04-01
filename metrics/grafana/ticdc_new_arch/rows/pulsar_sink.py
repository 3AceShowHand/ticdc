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
    expr_increase,
    expr_simple,
    expr_sum,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, timeseries, row

def build_pulsar_sink_row() -> RowSpec:
    row_builder = row('Pulsar Sink', default_height=9, default_span=12)

    pulsar_published_ddl_schema_count = graph('Pulsar Published DDL Schema Count', unit='short')

    pulsar_published_ddl_schema_count.add_query(
        f'{expr_increase('ticdc_pulsar_published_DDL_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'count')], window='30s')} / 2',
        legend='',
    ).add_query(
        expr_simple(
            'ticdc_pulsar_published_DDL_schema_table_count',
            scope='none',
            selectors=[
                regex('changefeed', '$changefeed'),
                regex('schema', '$schema'),
                regex('topic', '$topic'),
                eq('type', 'count'),
            ],
        ),
        legend='',
        ref='A',
    )

    row_builder.add_graph(pulsar_published_ddl_schema_count)

    pulsar_published_ddl_schema_success = graph('Pulsar Published DDL Schema Success', unit='short')

    pulsar_published_ddl_schema_success.add_query(
        f'{expr_increase('ticdc_pulsar_published_DDL_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'success')], window='30s')} / 2',
        legend='',
    ).add_query(
        expr_simple(
            'ticdc_pulsar_published_DDL_schema_table_count',
            scope='none',
            selectors=[
                regex('changefeed', '$changefeed'),
                regex('schema', '$schema'),
                regex('topic', '$topic'),
                eq('type', 'success'),
            ],
        ),
        legend='',
        ref='A',
    )

    row_builder.add_graph(pulsar_published_ddl_schema_success)

    pulsar_published_ddl_schema_fail = graph('Pulsar Published DDL Schema Fail', unit='short')

    pulsar_published_ddl_schema_fail.add_query(
        f'{expr_increase('ticdc_pulsar_published_DDL_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'fail')], window='30s')} / 2',
        legend='',
    ).add_query(
        expr_simple(
            'ticdc_pulsar_published_DDL_schema_table_count',
            scope='none',
            selectors=[
                regex('changefeed', '$changefeed'),
                regex('schema', '$schema'),
                regex('topic', '$topic'),
                eq('type', 'fail'),
            ],
        ),
        legend='',
        ref='A',
    )

    row_builder.add_graph(pulsar_published_ddl_schema_fail)

    pulsar_published_dml_schema_count = graph('Pulsar Published DML Schema Count', unit='short')

    pulsar_published_dml_schema_count.add_query(
        f'{expr_increase('ticdc_pulsar_published_DML_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'count')], window='30s')} / 2',
        legend='',
    )

    row_builder.add_graph(pulsar_published_dml_schema_count)

    pulsar_published_dml_schema_success = graph('Pulsar Published DML Schema Success', unit='short')

    pulsar_published_dml_schema_success.add_query(
        f'{expr_increase('ticdc_pulsar_published_DML_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'success')], window='30s')} / 2',
        legend='',
    )

    row_builder.add_graph(pulsar_published_dml_schema_success)

    pulsar_published_dml_schema_fail = graph('Pulsar Published DML Schema Fail', unit='short')

    pulsar_published_dml_schema_fail.add_query(
        f'{expr_increase('ticdc_pulsar_published_DML_schema_table_count', scope='none', selectors=[regex('changefeed', '$changefeed'), regex('schema', '$schema'), regex('topic', '$topic'), eq('type', 'fail')], window='30s')} / 2',
        legend='',
    )

    row_builder.add_graph(pulsar_published_dml_schema_fail)

    pulsar_client_bytes_published = graph('Pulsar Client Bytes Published', unit='bytes')

    pulsar_client_bytes_published.add_query(
        expr_sum(
            'pulsar_client_bytes_published',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}',
    )

    row_builder.add_graph(pulsar_client_bytes_published)

    pulsar_client_connections_opened = graph('Pulsar Client Connections Opened', unit='none')

    pulsar_client_connections_opened.add_query(
        expr_sum(
            'pulsar_client_connections_opened',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}',
        instant=False,
    )

    row_builder.add_graph(pulsar_client_connections_opened)

    pulsar_client_rpc_count = graph('Pulsar Client RPC Count', unit='none')

    pulsar_client_rpc_count.add_query(
        expr_sum(
            'pulsar_client_rpc_count',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}',
        instant=False,
    )

    row_builder.add_graph(pulsar_client_rpc_count)

    pulsar_client_producer_latency = timeseries('Pulsar Client Producer Latency', unit='s')

    pulsar_client_producer_latency.add_query(
        expr_histogram_quantile(
            0.999,
            'pulsar_client_producer_latency_seconds',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}-P999',
        instant=False,
    ).add_query(
        expr_histogram_avg(
            'pulsar_client_producer_latency_seconds',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='',
        format=None,
    )

    row_builder.add_timeseries(pulsar_client_producer_latency)

    pulsar_client_producer_rpc_latency = graph('Pulsar Client Producer RPC Latency', unit='s')

    pulsar_client_producer_rpc_latency.add_query(
        expr_histogram_quantile(
            0.999,
            'pulsar_client_producer_rpc_latency_seconds',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}',
        instant=False,
    )

    row_builder.add_graph(pulsar_client_producer_rpc_latency)

    pulsar_client_producer_pending_messages = graph(
        'Pulsar Client Producer Pending Messages',
        description='',
        unit='none',
    )

    pulsar_client_producer_pending_messages.add_query(
        expr_sum(
            'pulsar_client_producer_pending_messages',
            by_labels=['changefeed', 'instance'],
            scope='none',
            selectors=[regex('changefeed', '$changefeed')],
        ),
        legend='{{changefeed}}-{{instance}}',
        instant=False,
    )

    row_builder.add_graph(pulsar_client_producer_pending_messages)

    pulsar_client_producer_pending_messages_2 = graph(
        'Pulsar Client Producer Pending Messages',
        description='',
        unit='none',
        x=12,
    )

    pulsar_client_producer_pending_messages_2.add_query(
        expr_sum(
            'published_message_type_resolved_count',
            by_labels=['changefeed'],
            scope='none',
            selectors=[
                regex('changefeed', '$changefeed'),
                regex('schema', '$schema'),
                regex('topic', '$topic'),
            ],
        ),
        legend='{{changefeed}}-{{topic}}',
        instant=False,
    )

    row_builder.add_graph(pulsar_client_producer_pending_messages_2)

    return row_builder.build()
