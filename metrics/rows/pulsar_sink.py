# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_pulsar_sink_row() -> RowSpec:
    row_builder = row("Pulsar Sink")

    pulsar_published_ddl_schema_count = (
        graph(
            "Pulsar Published DDL Schema Count",
        )
        .add_query(
            'increase(ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="count"}[30s]) / 2',
            legend="",
        )
        .add_query(
            'ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="count"}',
            legend="",
            ref="A",
        )
    )

    pulsar_published_ddl_schema_success = (
        graph(
            "Pulsar Published DDL Schema Success",
        )
        .add_query(
            'increase(ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="success"}[30s]) / 2',
            legend="",
        )
        .add_query(
            'ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="success"}',
            legend="",
            ref="A",
        )
    )

    pulsar_published_ddl_schema_fail = (
        graph(
            "Pulsar Published DDL Schema Fail",
        )
        .add_query(
            'increase(ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="fail"}[30s]) / 2',
            legend="",
        )
        .add_query(
            'ticdc_pulsar_published_DDL_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="fail"}',
            legend="",
            ref="A",
        )
    )

    pulsar_published_dml_schema_count = graph(
        "Pulsar Published DML Schema Count",
    ).add_query(
        'increase(ticdc_pulsar_published_DML_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="count"}[30s]) / 2',
        legend="",
    )

    pulsar_published_dml_schema_success = graph(
        "Pulsar Published DML Schema Success",
    ).add_query(
        'increase(ticdc_pulsar_published_DML_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="success"}[30s]) / 2',
        legend="",
    )

    pulsar_published_dml_schema_fail = graph(
        "Pulsar Published DML Schema Fail",
    ).add_query(
        'increase(ticdc_pulsar_published_DML_schema_table_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic", type="fail"}[30s]) / 2',
        legend="",
    )

    pulsar_client_bytes_published = graph(
        "Pulsar Client Bytes Published",
        unit="bytes",
    ).add_query(
        'sum(pulsar_client_bytes_published{changefeed=~"$changefeed"}) by (changefeed, instance)',
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_connections_opened = graph(
        "Pulsar Client Connections Opened",
        unit="none",
    ).add_range_query(
        'sum(pulsar_client_connections_opened{changefeed=~"$changefeed"}) by (changefeed, instance)',
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_rpc_count = graph(
        "Pulsar Client RPC Count",
        unit="none",
    ).add_range_query(
        'sum(pulsar_client_rpc_count{changefeed=~"$changefeed"}) by (changefeed, instance)',
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_producer_latency = (
        graph(
            "Pulsar Client Producer Latency",
            unit="s",
        )
        .add_range_query(
            'histogram_quantile(0.999, sum(rate(pulsar_client_producer_latency_seconds_bucket{changefeed=~"$changefeed"}[1m])) by (le, changefeed, instance))',
            legend="{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(pulsar_client_producer_latency_seconds_sum{changefeed=~"$changefeed"}[1m])) by (changefeed, instance) / sum(rate(pulsar_client_producer_latency_seconds_count{changefeed=~"$changefeed"}[1m])) by (changefeed, instance)',
            legend="",
        )
    )

    pulsar_client_producer_rpc_latency = graph(
        "Pulsar Client Producer RPC Latency",
        unit="s",
    ).add_range_query(
        'histogram_quantile(0.999, sum(rate(pulsar_client_producer_rpc_latency_seconds_bucket{changefeed=~"$changefeed"}[1m])) by (le, changefeed, instance))',
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_producer_pending_messages = graph(
        "Pulsar Client Producer Pending Messages",
        description="",
        unit="none",
        key="producer_pending_messages",
    ).add_range_query(
        'sum(pulsar_client_producer_pending_messages{changefeed=~"$changefeed"}) by (changefeed, instance)',
        legend="{{changefeed}}-{{instance}}",
    )

    pulsar_client_producer_pending_messages_2 = graph(
        "Pulsar Client Producer Pending Messages",
        description="",
        unit="none",
        key="resolved_message_count",
    ).add_range_query(
        'sum(published_message_type_resolved_count{changefeed=~"$changefeed", schema=~"$schema", topic=~"$topic"}) by (changefeed)',
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

    row_builder.add_right_half_panel(pulsar_client_producer_pending_messages_2)

    return row_builder.build()
