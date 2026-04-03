# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_sum,
    legend_for,
    neq,
    regex,
)


def build_scheduler_row() -> RowSpec:
    row_builder = row("Scheduler")

    table_replication_state = graph(
        "Table Replication State",
        description="The total number of tables in different replication states.\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_scheduler_table_replication_state",
            by_labels=["namespace", "changefeed", "state"],
            scope="changefeed",
        ),
    )

    schedule_tasks = graph(
        "Schedule Tasks",
        description="The total number of different schedule tasks.",
        unit="none",
        min="0",
    ).add_query(
        expr_sum(
            "ticdc_scheduler_task",
            by_labels=["namespace", "changefeed", "scheduler", "task", "mode"],
            scope="changefeed",
        ),
    )

    span_count = graph(
        "Span Count",
        description="The total number of spans",
    ).add_query(
        expr_sum(
            "ticdc_scheduler_span_count",
            by_labels=["namespace", "changefeed", "mode"],
            scope="changefeed",
        ),
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

    operator_count = (
        graph(
            "Operator Count",
            description="The number of current operator count",
        )
        .add_query(
            expr_sum(
                "ticdc_maintainer_created_count",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
                selectors=[eq("type", "add")],
            ),
            legend=legend_for("namespace", "changefeed", "mode", prefix="add-operator"),
        )
        .add_auto_query(
            expr_sum(
                "ticdc_maintainer_created_count",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
                selectors=[eq("type", "move")],
            ),
            legend=legend_for("namespace", "changefeed", "mode", prefix="move-operator"),
        )
        .add_auto_query(
            expr_sum(
                "ticdc_maintainer_created_count",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
                selectors=[eq("type", "split")],
            ),
            legend=legend_for("namespace", "changefeed", "mode", prefix="split-operator"),
        )
        .add_auto_query(
            expr_sum(
                "ticdc_maintainer_created_count",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
                selectors=[eq("type", "merge")],
            ),
            legend=legend_for("namespace", "changefeed", "mode", prefix="merge-operator"),
        )
    )

    total_operator_count = graph(
        "Total Operator Count",
        description="The number of total operator count ",
    ).add_query(
        expr_sum(
            "ticdc_maintainer_total_operator_count",
            by_labels=["namespace", "changefeed", "type", "mode"],
            scope="changefeed",
            selectors=[neq("type", "occupy")],
        ),
        legend=legend_for("type", "namespace", "changefeed", "mode"),
    )

    split_span_check_duration = (
        graph(
            "Split Span Check Duration",
            description="duration for split span do once check",
            unit="s",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_maintainer_split_span_check_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_maintainer_split_span_check_duration",
                by_labels=["namespace", "changefeed", "instance"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
        )
    )

    operator_cost_duration = (
        graph(
            "Operator Cost Duration",
            description="duration for each operator",
            unit="s",
        )
        .add_auto_query(
            expr_histogram_quantile(
                0.999,
                "ticdc_maintainer_finish_operators_duration_seconds",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
            ),
            legend="99.9-{{namespace}}-{{changefeed}}-{{mode}}",
        )
        .add_auto_query(
            expr_histogram_avg(
                "ticdc_maintainer_finish_operators_duration_seconds",
                by_labels=["namespace", "changefeed", "mode"],
                scope="changefeed",
            ),
            legend="avg-{{namespace}}-{{changefeed}}-{{mode}}",
        )
    )

    slowest_table_checkpoint = (
        graph(
            "Slowest Table Checkpoint",
            description="The checkpoint ts of the slowest table.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"),
            legend="approximate current time (s)",
        )
        .add_query(
            expr_max(
                "ticdc_scheduler_slow_table_checkpoint_ts",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
        )
    )

    slowest_table_id = graph(
        "Slowest Table ID",
        description="The ID of the slowest table",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_scheduler_slow_table_id",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
    )

    slowest_table_replication_state = graph(
        "Slowest Table Replication State",
        description="The state of the slowest table.\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum(
            "ticdc_scheduler_slow_table_replication_state",
            by_labels=["namespace", "changefeed"],
            scope="changefeed",
        ),
    )

    slowest_table_resolved_ts = (
        graph(
            "Slowest Table Resolved Ts",
            description="The resolved ts of the slowest table.",
            unit="dateTimeAsIso",
        )
        .add_query(
            expr_max("pd_cluster_tso", scope="cluster"),
            legend="approximate current time (s)",
        )
        .add_query(
            expr_max(
                "ticdc_scheduler_slow_table_resolved_ts",
                by_labels=["namespace", "changefeed"],
                scope="cluster",
                selectors=[
                    regex("namespace", "$namespace"),
                    regex("changefeed", "$changefeed"),
                ],
            ),
        )
    )

    row_builder.add_panels(
        table_replication_state,
        schedule_tasks,
    )

    row_builder.add_panels(
        span_count,
        table_count,
    )

    row_builder.add_panels(
        operator_count,
        total_operator_count,
    )

    row_builder.add_panels(
        split_span_check_duration,
        operator_cost_duration,
    )

    row_builder.add_panels(
        slowest_table_checkpoint,
        slowest_table_id,
    )

    row_builder.add_panels(
        slowest_table_replication_state,
        slowest_table_resolved_ts,
    )

    return row_builder.build()
