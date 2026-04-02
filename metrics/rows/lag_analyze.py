# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_avg,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_delta,
    expr_sum_increase,
    expr_sum_rate,
    not_regex,
    regex,
)


def build_lag_analyze_row() -> RowSpec:
    row_builder = row("Lag analyze")
    namespace_changefeed_selectors = [
        regex("namespace", "$namespace"),
        regex("changefeed", "$changefeed"),
    ]
    pd_cluster_tso = expr_max("pd_cluster_tso", scope="cluster")
    min_resolved_ts_seconds = expr_avg(
        f"{expr_simple('tikv_cdc_min_resolved_ts', scope='tikv_instance')} / 1000",
        by_labels=["instance"],
        scope="none",
    )
    resolved_ts_gap_rate = expr_sum_rate(
        "tikv_cdc_resolved_ts_gap_seconds_count",
        by_labels=["instance"],
        scope="tikv_instance",
    )
    resolved_region_count = expr_sum(
        "tikv_cdc_region_resolve_status",
        by_labels=["instance"],
        scope="tikv_instance",
        selectors=[eq("status", "resolved")],
    )

    changefeed_checkpoint_lag = graph(
        "Changefeed Checkpoint Lag",
        description="The lag between changefeed checkpoint ts and PD TSO of upstream TiDB.",
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_checkpoint_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=namespace_changefeed_selectors,
        )
    )

    changefeed_resolved_ts_lag = graph(
        "Changefeed Resolved Ts Lag",
        description=("The lag between changefeed resolved ts and PD TSO of upstream TiDB."),
        unit="s",
        min="0",
    ).add_query(
        expr_max(
            "ticdc_owner_resolved_ts_lag",
            by_labels=["namespace", "changefeed"],
            scope="cluster",
            selectors=namespace_changefeed_selectors,
        ),
        legend="{{namespace}}-{{changefeed}}-resolvedts",
    )

    row_builder.add_panels(
        changefeed_checkpoint_lag,
        changefeed_resolved_ts_lag,
    )

    eventfeed_error_m = graph(
        "Eventfeed Error / m",
        description="The number of errors that interrupt Eventfeed RPC",
        unit="short",
        min="0",
    ).add_query(
        expr_sum_increase(
            "ticdc_kvclient_event_feed_error_count", by_labels=["type"], scope="instance"
        )
    )

    pd_operator_m = graph(
        "PD Operator / m",
        description="The number of PD scheduling operator.",
        unit="short",
        min="0",
    ).add_query(
        expr_sum_increase(
            "pd_schedule_operators_count",
            by_labels=["type"],
            scope="cluster",
            selectors=[eq("event", "create")],
        )
    )

    row_builder.add_panels(eventfeed_error_m, pd_operator_m)

    tidb_query_duration = graph(
        "TiDB Query Duration", description="99.9% of TiDB query durations.", unit="s", min="0"
    ).add_query(
        expr_histogram_quantile(
            0.999,
            "tidb_server_handle_query_duration_seconds",
            by_labels=["sql_type"],
            scope="cluster",
            selectors=[not_regex("sql_type", "internal|Use|Show")],
        ),
        ref="B",
    )

    tikv_min_resolved_ts_lag = (
        graph(
            "TiKV Min Resolved Ts Lag",
            description="The min resolved ts lag of each TiKV",
            unit="s",
            min="0",
        )
        .add_range_query(
            f"scalar({pd_cluster_tso})/1000 - {min_resolved_ts_seconds} > 0",
            legend="{{instance}}-min-resolved-lag",
        )
        .add_query(
            expr_simple("tikv_cdc_resolved_ts_advance_method", scope="cluster"),
            legend="{{instance}}-resolved_ts_advance_method",
            ref="B",
        )
    )

    row_builder.add_panels(tidb_query_duration, tikv_min_resolved_ts_lag)

    sink_write_rows_s = graph(
        "Sink Write Rows / s",
        description=("The number of changed rows that are written to downstream per second"),
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum_rate(
            "ticdc_sink_batch_row_count_sum",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        legend="{{namespace}}-{{changefeed}}",
    )

    sink_write_duration = graph(
        "Sink Write Duration",
        description="99.9% of TiCDC sink write durations.",
        unit="s",
        min="0",
    ).add_query(
        expr_histogram_avg(
            "ticdc_sink_txn_worker_flush_duration",
            by_labels=["namespace", "changefeed", "instance"],
            scope="changefeed",
        ),
        legend="{{namespace}}-{{changefeed}}-avg",
        ref="B",
    )

    row_builder.add_panels(sink_write_rows_s, sink_write_duration)

    tikv_scan_tasks_m = graph(
        "TiKV Scan Tasks / m",
        description="The number of incremental scan tasks per minute",
        unit="none",
        min="0",
    ).add_query(
        expr_sum_increase(
            "tikv_cdc_scan_duration_seconds_sum",
            by_labels=["type", "instance"],
            scope="tikv_instance",
        ),
        legend="{{instance}}",
    )

    tikv_scan_region_time_m = graph(
        "TiKV Scan Region Time / m",
        description="The total time of incremental scan region takes per minute",
        unit="s",
        min="0",
    ).add_query(
        expr_sum_increase(
            "tikv_cdc_scan_duration_seconds_sum", by_labels=["instance"], scope="tikv_instance"
        ),
        ref="B",
    )

    row_builder.add_panels(tikv_scan_tasks_m, tikv_scan_region_time_m)

    tikv_leader_change = graph(
        "TiKV Leader Change",
        description="The number of leaders on each TiKV instance",
        unit="short",
        min="0",
    ).add_query(
        expr_sum_delta(
            "tikv_raftstore_region_count",
            scope="tikv_instance",
            selectors=[eq("type", "leader")],
            window="30s",
        ),
        legend="changed",
    )

    tikv_admin_apply_s = graph(
        "TiKV Admin Apply / s",
        description="The number of the processed TiKV admin command",
        unit="none",
        min="0",
    ).add_query(
        expr_sum_rate(
            "tikv_raftstore_admin_cmd_total",
            by_labels=["type"],
            scope="tikv_instance",
            selectors=[eq("status", "success"), not_regex("type", "compact")],
        ),
        ref="B",
    )

    row_builder.add_panels(tikv_leader_change, tikv_admin_apply_s)

    tikv_advance_resolved_ts_s = graph(
        "TiKV Advance Resolved Ts / s",
        description="The rate of TiKV advancing resolved ts.",
        unit="none",
        min="0",
    ).add_query(
        f"{resolved_ts_gap_rate} / {resolved_region_count}",
        legend="{{instance}}",
    )

    tikv_unresolved_region_count = graph(
        "TiKV Unresolved Region Count",
        description="The number of unresolved region per TiKV",
        unit="none",
        min="0",
    ).add_query(
        expr_sum(
            "tikv_cdc_region_resolve_status",
            by_labels=["status"],
            scope="tikv_instance",
            selectors=[eq("status", "unresolved")],
        ),
        ref="B",
    )

    row_builder.add_panels(
        tikv_advance_resolved_ts_s,
        tikv_unresolved_region_count,
    )

    tikv_check_leader_region_count_percentile = graph(
        "TiKV Check Leader Region Count Percentile",
        description="99.99% of the number of regions that TiKV checks. ",
        unit="none",
        min="0",
    ).add_query(
        expr_histogram_quantile(
            0.9999,
            "tikv_check_leader_request_item_count",
            by_labels=["instance"],
            scope="tikv_instance",
        )
    )

    tikv_advance_resolved_ts_fail_m = graph(
        "TiKV Advance Resolved Ts Fail / m",
        description="The number of failed count of advancing resolved ts.",
        unit="none",
        min="0",
    ).add_query(
        expr_sum_increase(
            "tikv_resolved_ts_fail_advance_count",
            by_labels=["reason", "instance"],
            scope="tikv_instance",
        )
    )

    row_builder.add_panels(
        tikv_check_leader_region_count_percentile,
        tikv_advance_resolved_ts_fail_m,
    )

    tikv_check_leader_duration_percentile = (
        graph(
            "TiKV Check Leader Duration Percentile",
            description="99.99% of the duration that TiKV check leader takes. ",
            unit="s",
            min="0",
        )
        .add_query(
            expr_histogram_quantile(
                0.9999, "tikv_resolved_ts_tikv_client_init_duration_seconds", scope="tikv_instance"
            ),
            legend="new-client",
        )
        .add_query(
            expr_histogram_quantile(
                0.9999,
                "tikv_resolved_ts_check_leader_duration_seconds",
                by_labels=["type"],
                scope="tikv_instance",
            ),
            ref="B",
        )
    )

    tikv_cdc_incremental_scan_long_duration_region_count = graph(
        "TiKV CDC Incremental Scan Long Duration Region Count",
        description=("The number of regions that take a long time (more than 60s) to scan"),
        unit="none",
        min="0",
    ).add_query(
        expr_simple("tikv_cdc_scan_long_duration_region", scope="tikv_instance"),
        legend="{{instance}}",
        ref="B",
    )

    row_builder.add_panels(
        tikv_check_leader_duration_percentile,
        tikv_cdc_incremental_scan_long_duration_region_count,
    )

    return row_builder.build()
