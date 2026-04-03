# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_coordinator_row() -> RowSpec:
    row_builder = row("Coordinator")

    changefeed_status = graph(
        "Changefeed Status",
        description="The number of changefeed in different status\n\n0: ReplicationSetStateUnknown means the replication state is unknown, it should not happen.\n\n1: ReplicationSetStateAbsent means there is no one replicates or prepares it.\n\n2: ReplicationSetStatePrepare means it needs to add a secondary.\n\n3: ReplicationSetStateCommit means it needs to promote secondary to primary.\n\n4: ReplicationSetStateReplicating means there is exactly one capture that is replicating the table.\n\n5: ReplicationSetStateRemoving means all captures need to stop replication eventually.\n\n",
        min="0",
    ).add_query(
        'sum(ticdc_coordinator_changefeed_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, state)',
        legend="{{namespace}}-{{changefeed}}-{{state}}",
    )

    coordinator_operator_cost_duration = graph(
        "Coordinator Operator Cost Duration",
        description="duration for each operator for changefeed",
        unit="s",
    ).add_histogram(
        "ticdc_coordinator_finish_operators_duration_seconds",
        by_labels=["namespace", "changefeed", "mode"],
        scope="changefeed",
        quantile=0.999,
        quantile_legend="99.9-{{namespace}}-{{changefeed}}-{{mode}}",
        average_legend="avg-{{namespace}}-{{changefeed}}-{{mode}}",
    )

    coordinator_history = graph(
        "Coordinator History",
        description="The history of TiCDC cluster coordinator. The owner node has a value greater than 0.",
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        'sum(rate(ticdc_owner_ownership_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[240s])) by (instance) > BOOL 0.5',
        legend="{{instance}}",
    )

    row_builder.add_panels(
        changefeed_status,
        coordinator_operator_cost_duration,
    )

    row_builder.add_half_panel(coordinator_history)

    return row_builder.build()
