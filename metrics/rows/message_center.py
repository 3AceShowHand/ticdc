# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import expr_sum, expr_sum_rate, legend_for


def build_message_center_row() -> RowSpec:
    row_builder = row("Message Center")

    sent_message_count_per_second = (
        graph(
            "Sent Message Count Per Second",
            min="0",
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_messaging_error_counter",
                by_labels=["instance", "target", "type"],
                scope="instance",
            ),
            legend=legend_for("instance", "target", "type", prefix="err"),
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_messaging_drop_message_counter",
                by_labels=["instance", "target", "type"],
                scope="instance",
            ),
            legend=legend_for("instance", "target", "type", prefix="drop"),
        )
        .add_auto_query(
            expr_sum_rate(
                "ticdc_messaging_send_message_counter",
                by_labels=["instance", "target", "type"],
                scope="instance",
            ),
            legend=legend_for("instance", "target", "type", prefix="send"),
        )
    )

    slow_message_count = graph(
        "Slow Message Count",
        description="the count of slow message Count",
    ).add_query(
        expr_sum(
            "ticdc_messaging_slow_handle_counter",
            by_labels=["instance", "type"],
            scope="instance",
        ),
    )

    row_builder.add_panels(
        sent_message_count_per_second,
        slow_message_count,
    )

    return row_builder.build()
