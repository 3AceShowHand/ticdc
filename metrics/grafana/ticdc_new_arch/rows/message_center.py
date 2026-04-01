# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    expr_sum,
    expr_sum_rate,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_message_center_row() -> RowSpec:
    row_builder = row('Message Center', default_height=8, default_span=12)

    sent_message_count_per_second = graph('Sent Message Count Per Second', unit='short', min='0')

    sent_message_count_per_second.add_query(
        expr_sum_rate(
            'ticdc_messaging_error_counter',
            by_labels=['instance', 'target', 'type'],
            scope='instance',
        ),
        legend='err-{{instance}}-{{target}}-{{type}}',
        format=None,
    ).add_query(
        expr_sum_rate(
            'ticdc_messaging_drop_message_counter',
            by_labels=['instance', 'target', 'type'],
            scope='instance',
        ),
        legend='drop-{{instance}}-{{target}}-{{type}}',
        format=None,
    ).add_query(
        expr_sum_rate(
            'ticdc_messaging_send_message_counter',
            by_labels=['instance', 'target', 'type'],
            scope='instance',
        ),
        legend='send-{{instance}}-{{target}}-{{type}}',
        format=None,
    )

    row_builder.add_graph(sent_message_count_per_second)

    slow_message_count = graph(
        'Slow Message Count',
        description='the count of slow message Count',
        unit='short',
    )

    slow_message_count.add_query(
        expr_sum(
            'ticdc_messaging_slow_handle_counter',
            by_labels=['instance', 'type'],
            scope='instance',
        ),
        legend='{{instance}}-{{type}}',
    )

    row_builder.add_graph(slow_message_count)

    return row_builder.build()
