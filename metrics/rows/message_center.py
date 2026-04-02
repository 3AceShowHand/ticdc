# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_message_center_row() -> RowSpec:
    row_builder = row("Message Center")

    sent_message_count_per_second = (
        graph(
            "Sent Message Count Per Second",
            min="0",
        )
        .add_auto_query(
            'sum(rate(ticdc_messaging_error_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, target, type)',
            legend="err-{{instance}}-{{target}}-{{type}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_messaging_drop_message_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, target, type)',
            legend="drop-{{instance}}-{{target}}-{{type}}",
        )
        .add_auto_query(
            'sum(rate(ticdc_messaging_send_message_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}[1m])) by (instance, target, type)',
            legend="send-{{instance}}-{{target}}-{{type}}",
        )
    )

    slow_message_count = graph(
        "Slow Message Count",
        description="the count of slow message Count",
    ).add_query(
        'sum(ticdc_messaging_slow_handle_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance"}) by (instance, type)',
        legend="{{instance}}-{{type}}",
    )

    row_builder.add_panels(
        sent_message_count_per_second,
        slow_message_count,
    )

    return row_builder.build()
