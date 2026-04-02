# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    expr_sum,
    expr_sum_rate,
)


def build_active_active_row() -> RowSpec:
    row_builder = row("Active Active")

    conflict_skip_rows_s = (
        graph(
            "Conflict Skip Rows / s",
            description=(
                "Rows skipped due to last-write-wins conflict resolution during "
                "TiDB active-active replication."
            ),
            unit="none",
            min="0",
        )
        .add_query(
            expr_sum_rate(
                "ticdc_sink_active_active_conflict_skip_rows_total",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-rate",
            ref="B",
        )
        .add_query(
            expr_sum(
                "ticdc_sink_active_active_conflict_skip_rows_total",
                by_labels=["namespace", "changefeed"],
                scope="changefeed",
            ),
            legend="{{namespace}}-{{changefeed}}-sum",
            ref="C",
            hide=True,
        )
    )

    row_builder.add_half_panel(conflict_skip_rows_s)

    return row_builder.build()
