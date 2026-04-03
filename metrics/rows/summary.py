# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import expr_max, expr_sum, expr_sum_rate, regex


def build_summary_row() -> RowSpec:
    row_builder = row("Summary")

    changefeed_checkpoint_lag = graph(
        "Changefeed Checkpoint Lag",
        description="The lag between changefeed coordinator checkpoint ts and the tso of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    changefeed_resolved_ts_lag = graph(
        "Changefeed Resolved Ts Lag",
        description="The lag between changefeed coordinator resolved ts and the tso of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_resolved_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    upstream_write_bytes_s = graph(
        "Upstream Write Bytes / s",
        description="Represents the total amount of data written by the upstream cluster of TiCDC, not the total amount of data that CDC needs to pull.",
        unit="binBps",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "tidb_tikvclient_txn_write_size_bytes_sum",
            scope="cluster",
            selectors=[regex("scope", "general")],
            window="30s",
        ),
        legend="sum",
    )

    ticdc_input_bytes_s = graph(
        "TiCDC Input Bytes / s",
        description="The number of bytes written into the event store pebble storage",
        unit="binBps",
        min="0",
    ).add_auto_query(
        expr_sum_rate(
            "ticdc_event_store_write_bytes",
            by_labels=["instance"],
            scope="instance",
        ),
    )

    sink_event_row_count_s = graph(
        "Sink Event Row Count / s",
        description="The number of dml events that sink flushes to downstream per second.",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum_rate(
            "ticdc_sink_dml_event_count",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
    )

    sink_write_bytes_s = (
        graph(
            "Sink Write Bytes / s",
            unit="binBps",
            min="0",
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_sink_write_bytes_total",
                by_labels=["instance", "type", "changefeed"],
                scope="changefeed",
            ),
            legend="{{instance}}-{{changefeed}}-{{type}}",
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_sink_write_bytes_total",
                by_labels=["instance", "type", "changefeed"],
                scope="changefeed",
            ).call("avg_over_time", range_selector="1m:"),
            legend="{{instance}}-{{changefeed}}-AVG",
            hide=True,
        )
    )

    the_status_of_changefeeds = graph(
        "The Status of Changefeeds",
        description="The status of each changefeed.\n\n0: Normal\n\n1: Pending\n\n2: Failed\n\n3: Stopped\n\n4: Finished\n\n5: Removed\n\n6: Warning\n\n7: Uninitialized\n\n-1: Unknown",
        min="0",
    ).add_range_query(
        expr_max(
            "ticdc_owner_status",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=[
                regex("namespace", "$namespace"),
                regex("changefeed", "$changefeed"),
            ],
        ),
    )

    table_dispatcher_count = graph(
        "Table Dispatcher Count",
        description="",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_dispatchermanager_table_dispatcher_count",
            by_labels=["instance", "changefeed", "event_type"],
            scope="changefeed",
        ),
    )

    memory_quota = graph(
        "Memory Quota",
        description="Changefeed memory quota",
        unit="bytes",
        min="0",
    ).add_auto_query(
        expr_sum(
            "ticdc_dynamic_stream_memory_usage",
            by_labels=["namespace", "area", "instance", "module", "type"],
            scope="instance",
            selectors=[
                regex("namespace", "$namespace"),
                regex("area", "$changefeed"),
                regex("module", "event-collector"),
            ],
        ),
        legend="{{namespace}}-{{area}}-{{instance}}-{{module}}-{{type}}",
    )

    table_count = graph(
        "Table Count",
        description="The total number of tables",
    ).add_query(
        expr_sum(
            "ticdc_scheduler_table_count",
            by_labels=["namespace", "changefeed", "mode"],
            scope="changefeed",
        ),
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
