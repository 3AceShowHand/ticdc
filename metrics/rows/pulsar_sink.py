# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import LineLayouts, graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_increase,
    expr_simple,
    expr_sum,
    regex,
)


def build_pulsar_sink_row() -> RowSpec:
    row_builder = row("Pulsar Sink")

    pulsar_published_ddl_schema_count = (
        graph(
            "Pulsar Published DDL Schema Count",
        )
        .add_query(
            expr_increase(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "count"),
                ],
                window="30s",
            ).op("/", "2"),
            legend="",
        )
        .add_query(
            expr_simple(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "count"),
                ],
            ),
            legend="",
            ref="A",
        )
    )

    pulsar_published_ddl_schema_success = (
        graph(
            "Pulsar Published DDL Schema Success",
        )
        .add_query(
            expr_increase(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "success"),
                ],
                window="30s",
            ).op("/", "2"),
            legend="",
        )
        .add_query(
            expr_simple(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "success"),
                ],
            ),
            legend="",
            ref="A",
        )
    )

    pulsar_published_ddl_schema_fail = (
        graph(
            "Pulsar Published DDL Schema Fail",
        )
        .add_query(
            expr_increase(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "fail"),
                ],
                window="30s",
            ).op("/", "2"),
            legend="",
        )
        .add_query(
            expr_simple(
                "ticdc_pulsar_published_DDL_schema_table_count",
                scope="none",
                selectors=[
                    regex("changefeed", "$changefeed"),
                    regex("schema", "$schema"),
                    regex("topic", "$topic"),
                    eq("type", "fail"),
                ],
            ),
            legend="",
            ref="A",
        )
    )

    pulsar_published_dml_schema_count = graph(
        "Pulsar Published DML Schema Count",
    ).add_query(
        expr_increase(
            "ticdc_pulsar_published_DML_schema_table_count",
            scope="none",
            selectors=[
                regex("changefeed", "$changefeed"),
                regex("schema", "$schema"),
                regex("topic", "$topic"),
                eq("type", "count"),
            ],
            window="30s",
        ).op("/", "2"),
        legend="",
    )

    pulsar_published_dml_schema_success = graph(
        "Pulsar Published DML Schema Success",
    ).add_query(
        expr_increase(
            "ticdc_pulsar_published_DML_schema_table_count",
            scope="none",
            selectors=[
                regex("changefeed", "$changefeed"),
                regex("schema", "$schema"),
                regex("topic", "$topic"),
                eq("type", "success"),
            ],
            window="30s",
        ).op("/", "2"),
        legend="",
    )

    pulsar_published_dml_schema_fail = graph(
        "Pulsar Published DML Schema Fail",
    ).add_query(
        expr_increase(
            "ticdc_pulsar_published_DML_schema_table_count",
            scope="none",
            selectors=[
                regex("changefeed", "$changefeed"),
                regex("schema", "$schema"),
                regex("topic", "$topic"),
                eq("type", "fail"),
            ],
            window="30s",
        ).op("/", "2"),
        legend="",
    )

    pulsar_client_bytes_published = graph(
        "Pulsar Client Bytes Published",
        unit="bytes",
    ).add_query(
        expr_sum(
            "pulsar_client_bytes_published",
            by_labels=["changefeed", "instance"],
            scope="none",
            selectors=[regex("changefeed", "$changefeed")],
        ),
    )

    pulsar_client_connections_opened = graph(
        "Pulsar Client Connections Opened",
        unit="none",
    ).add_range_query(
        expr_sum(
            "pulsar_client_connections_opened",
            by_labels=["changefeed", "instance"],
            scope="none",
            selectors=[regex("changefeed", "$changefeed")],
        ),
    )

    pulsar_client_rpc_count = graph(
        "Pulsar Client RPC Count",
        unit="none",
    ).add_range_query(
        expr_sum(
            "pulsar_client_rpc_count",
            by_labels=["changefeed", "instance"],
            scope="none",
            selectors=[regex("changefeed", "$changefeed")],
        ),
    )

    pulsar_client_producer_latency = (
        graph(
            "Pulsar Client Producer Latency",
            unit="s",
        )
        .add_range_query(
            expr_histogram_quantile(
                0.999,
                "pulsar_client_producer_latency_seconds",
                by_labels=["changefeed", "instance"],
                scope="none",
                selectors=[regex("changefeed", "$changefeed")],
            ),
            legend="{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            expr_histogram_avg(
                "pulsar_client_producer_latency_seconds",
                by_labels=["changefeed", "instance"],
                scope="none",
                selectors=[regex("changefeed", "$changefeed")],
            ),
            legend="",
        )
    )

    pulsar_client_producer_rpc_latency = graph(
        "Pulsar Client Producer RPC Latency",
        unit="s",
    ).add_range_query(
        expr_histogram_quantile(
            0.999,
            "pulsar_client_producer_rpc_latency_seconds",
            by_labels=["changefeed", "instance"],
            scope="none",
            selectors=[regex("changefeed", "$changefeed")],
        ),
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_producer_pending_messages = graph(
        "Pulsar Client Producer Pending Messages",
        unit="none",
        key="producer_pending_messages",
    ).add_range_query(
        expr_sum(
            "pulsar_client_producer_pending_messages",
            by_labels=["changefeed", "instance"],
            scope="none",
            selectors=[regex("changefeed", "$changefeed")],
        ),
    )

    pulsar_client_resolved_message_count = graph(
        "Pulsar Client Producer Pending Messages",
        unit="none",
        key="resolved_message_count",
    ).add_range_query(
        expr_sum(
            "published_message_type_resolved_count",
            by_labels=["changefeed"],
            scope="none",
            selectors=[
                regex("changefeed", "$changefeed"),
                regex("schema", "$schema"),
                regex("topic", "$topic"),
            ],
        ),
        legend="{{changefeed}}-{{topic}}",
    )

    row_builder.add_panels(
        pulsar_published_ddl_schema_count,
        pulsar_published_ddl_schema_success,
    )

    row_builder.add_panels(
        pulsar_published_ddl_schema_fail,
        pulsar_published_dml_schema_count,
    )

    row_builder.add_panels(
        pulsar_published_dml_schema_success,
        pulsar_published_dml_schema_fail,
    )

    row_builder.add_panels(
        pulsar_client_bytes_published,
        pulsar_client_connections_opened,
    )

    row_builder.add_panels(
        pulsar_client_rpc_count,
        pulsar_client_producer_latency,
    )

    row_builder.add_panels(
        pulsar_client_producer_rpc_latency,
        pulsar_client_producer_pending_messages,
    )

    row_builder.add_panel(
        pulsar_client_resolved_message_count,
        layout=LineLayouts.HALVES_RIGHT,
    )

    return row_builder.build()
