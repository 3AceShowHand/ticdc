# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_scheduler_row() -> RowSpec:
    row_builder = row("Scheduler")

    table_replication_state = graph(
        "Table Replication State",
        description="The total number of tables in different replication states.\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n",
        min="0",
    ).add_query(
        'sum(ticdc_scheduler_table_replication_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, state)',
        legend="{{namespace}}-{{changefeed}}-{{state}}",
    )

    schedule_tasks = graph(
        "Schedule Tasks",
        description="The total number of different schedule tasks.",
        unit="none",
        min="0",
    ).add_query(
        'sum(ticdc_scheduler_task{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, scheduler, task, mode)',
        legend="{{namespace}}-{{changefeed}}-{{scheduler}}-{{task}}-{{mode}}",
    )

    span_count = graph(
        "Span Count",
        description="The total number of spans",
    ).add_query(
        'sum(ticdc_scheduler_span_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, mode)',
        legend="{{namespace}}-{{changefeed}}-{{mode}}",
    )

    table_count = graph(
        "Table Count",
        description="The total number of tables",
    ).add_query(
        'sum(ticdc_scheduler_table_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, mode)',
        legend="{{namespace}}-{{changefeed}}-{{mode}}",
    )

    operator_count = (
        graph(
            "Operator Count",
            description="The number of current operator count",
        )
        .add_query(
            'sum(ticdc_maintainer_created_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed", type="add"}) by (namespace, changefeed, mode)',
            legend="add-operator-{{namespace}}-{{changefeed}}-{{mode}}",
        )
        .add_auto_query(
            'sum(ticdc_maintainer_created_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed", type="move"}) by (namespace, changefeed, mode)',
            legend="move-operator-{{namespace}}-{{changefeed}}-{{mode}}",
        )
        .add_auto_query(
            'sum(ticdc_maintainer_created_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed", type="split"}) by (namespace, changefeed, mode)',
            legend="split-operator-{{namespace}}-{{changefeed}}-{{mode}}",
        )
        .add_auto_query(
            'sum(ticdc_maintainer_created_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed", type="merge"}) by (namespace, changefeed, mode)',
            legend="merge-operator-{{namespace}}-{{changefeed}}-{{mode}}",
        )
    )

    total_operator_count = graph(
        "Total Operator Count",
        description="The number of total operator count ",
    ).add_query(
        'sum(ticdc_maintainer_total_operator_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance", type!="occupy"}) by (namespace, changefeed, type, mode)',
        legend="{{type}}-{{namespace}}-{{changefeed}}-{{mode}}",
    )

    split_span_check_duration = graph(
        "Split Span Check Duration",
        description="duration for split span do once check",
        unit="s",
    ).add_histogram(
        "ticdc_maintainer_split_span_check_duration",
        by_labels=["namespace", "changefeed", "instance"],
        scope="changefeed",
        quantile=0.999,
        quantile_legend="99.9-{{namespace}}-{{changefeed}}-{{instance}}",
        average_legend="avg-{{namespace}}-{{changefeed}}-{{instance}}",
    )

    operator_cost_duration = graph(
        "Operator Cost Duration",
        description="duration for each operator",
        unit="s",
    ).add_histogram(
        "ticdc_maintainer_finish_operators_duration_seconds",
        by_labels=["namespace", "changefeed", "mode"],
        scope="changefeed",
        quantile=0.999,
        quantile_legend="99.9-{{namespace}}-{{changefeed}}-{{mode}}",
        average_legend="avg-{{namespace}}-{{changefeed}}-{{mode}}",
    )

    slowest_table_checkpoint = (
        graph(
            "Slowest Table Checkpoint",
            description="The checkpoint ts of the slowest table.",
            unit="dateTimeAsIso",
        )
        .add_query(
            'max(pd_cluster_tso{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"})',
            legend="approximate current time (s)",
        )
        .add_query(
            'max(ticdc_scheduler_slow_table_checkpoint_ts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
            legend="{{namespace}}-{{changefeed}}",
        )
    )

    slowest_table_id = graph(
        "Slowest Table ID",
        description="The ID of the slowest table",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        'sum(ticdc_scheduler_slow_table_id{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    slowest_table_replication_state = graph(
        "Slowest Table Replication State",
        description="The state of the slowest table.\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        'sum(ticdc_scheduler_slow_table_replication_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
        legend="{{namespace}}-{{changefeed}}",
    )

    slowest_table_resolved_ts = (
        graph(
            "Slowest Table Resolved Ts",
            description="The resolved ts of the slowest table.",
            unit="dateTimeAsIso",
        )
        .add_query(
            'max(pd_cluster_tso{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"})',
            legend="approximate current time (s)",
        )
        .add_query(
            'max(ticdc_scheduler_slow_table_resolved_ts{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed)',
            legend="{{namespace}}-{{changefeed}}",
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
