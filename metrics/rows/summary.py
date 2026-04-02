# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_summary_row() -> RowSpec:
    row_builder = row("Summary")

    changefeed_checkpoint_lag = graph(
        "Changefeed Checkpoint Lag",
        description="The lag between changefeed coordinator checkpoint ts and the tso of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        'max(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    changefeed_resolved_ts_lag = graph(
        "Changefeed Resolved Ts Lag",
        description="The lag between changefeed coordinator resolved ts and the tso of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        'max(ticdc_owner_resolved_ts_lag{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    upstream_write_bytes_s = graph(
        "Upstream Write Bytes / s",
        description="Represents the total amount of data written by the upstream cluster of TiCDC, not the total amount of data that CDC needs to pull.",
        unit="binBps",
        min="0",
    ).add_auto_query(
        'sum(rate(tidb_tikvclient_txn_write_size_bytes_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", scope=~"general"}[30s]))',
        legend="sum",
    )

    ticdc_input_bytes_s = graph(
        "TiCDC Input Bytes / s",
        description="The number of bytes written into the event store pebble storage",
        unit="binBps",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_event_store_write_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance)',
        legend="{{instance}}",
    )

    sink_event_row_count_s = graph(
        "Sink Event Row Count / s",
        description="The number of dml events that sink flushes to downstream per second.",
        min="0",
        decimals=0,
    ).add_query(
        'sum(rate(ticdc_sink_dml_event_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (namespace, changefeed, instance)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}",
    )

    sink_write_bytes_s = (
        graph(
            "Sink Write Bytes / s",
            unit="binBps",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_write_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (instance, type, changefeed)',
            legend="{{instance}}-{{changefeed}}-{{type}}",
        )
        .add_auto_query(
            'avg_over_time(sum(rate(ticdc_sink_write_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (instance, type, changefeed)[1m:])',
            legend="{{instance}}-{{changefeed}}-AVG",
            hide=True,
        )
    )

    the_status_of_changefeeds = graph(
        "The Status of Changefeeds",
        description="The status of each changefeed.\n\n0: Normal\n\n1: Pending\n\n2: Failed\n\n3: Stopped\n\n4: Finished\n\n5: Removed\n\n6: Warning\n\n7: Uninitialized\n\n-1: Unknown",
        min="0",
    ).add_range_query(
        'max(ticdc_owner_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    table_dispatcher_count = graph(
        "Table Dispatcher Count",
        description="",
        min="0",
        decimals=0,
    ).add_query(
        'sum(ticdc_dispatchermanager_table_dispatcher_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (instance, changefeed, event_type)',
        legend="{{instance}}-{{changefeed}}-{{event_type}}",
    )

    memory_quota = graph(
        "Memory Quota",
        description="Changefeed memory quota",
        unit="bytes",
        min="0",
    ).add_auto_query(
        'sum(ticdc_dynamic_stream_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", area=~"$changefeed", instance=~"$ticdc_instance", module=~"event-collector"}) by (namespace, area, instance, module, type)',
        legend="{{namespace}}-{{area}}-{{instance}}-{{module}}-{{type}}",
    )

    table_count = graph(
        "Table Count",
        description="The total number of tables",
    ).add_query(
        'sum(ticdc_scheduler_table_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, mode)',
        legend="{{namespace}}-{{changefeed}}-{{mode}}",
    )

    row_builder.add_panels(
        changefeed_checkpoint_lag,
        changefeed_resolved_ts_lag,
    )

    row_builder.add_panels(
        upstream_write_bytes_s,
        ticdc_input_bytes_s,
    )

    row_builder.add_panels(
        sink_event_row_count_s,
        sink_write_bytes_s,
    )

    row_builder.add_panels(
        the_status_of_changefeeds,
        table_dispatcher_count,
    )

    row_builder.add_panels(
        memory_quota,
        table_count,
    )

    return row_builder.build()
