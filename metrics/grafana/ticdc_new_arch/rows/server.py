# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    eq,
    expr_max,
    expr_rate,
    expr_simple,
    expr_sum_rate,
    regex,
    transformation,
)
from metrics.grafana.ticdc_new_arch.builders import graph, table, row

def build_server_row() -> RowSpec:
    row_builder = row('Server', default_height=9, default_span=12)

    uptime = graph(
        'Uptime',
        description='Uptime of TiCDC and TiKV',
        unit='dtdurations',
    )

    uptime.add_query(
        '(time() - process_start_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*ticdc.*", instance=~"$ticdc_instance"})',
        legend='TiCDC-{{instance}}',
        format='time_series',
    ).add_query(
        '(time() - process_start_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tikv.*"})',
        legend='TiKV-{{instance}}',
        ref='B',
        format='time_series',
    ).add_query(
        '(time() - process_start_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*pd.*"})',
        legend='PD-{{instance}}',
        ref='C',
        format='time_series',
    )

    row_builder.add_graph(uptime)

    cpu_usage = graph('CPU Usage', description='CPU usage of TiCDC', unit='percentunit')

    cpu_usage.add_query(
        expr_rate(
            'process_cpu_seconds_total',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}',
        format='time_series',
    ).add_query(
        expr_simple(
            'ticdc_server_go_max_procs',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='quota-{{instance}}',
        ref='B',
        format='time_series',
    )

    row_builder.add_graph(cpu_usage)

    goroutine_count = graph(
        'Goroutine Count',
        description='Goroutine count of TiCDC',
        unit='short',
    )

    goroutine_count.add_query(
        expr_simple(
            'go_goroutines',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}',
        format='time_series',
    ).add_query(
        expr_simple(
            'go_threads',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='threads-{{instance}}',
        ref='B',
        hide=True,
        format='time_series',
    )

    row_builder.add_graph(goroutine_count)

    memory_usage = graph('Memory Usage', description='Memory usage of TiCDC', unit='bytes')

    memory_usage.add_query(
        expr_simple(
            'process_resident_memory_bytes',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='process-{{instance}}',
        format='time_series',
    ).add_query(
        expr_simple(
            'go_memstats_heap_alloc_bytes',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='heap-{{instance}}',
        ref='B',
        format='time_series',
    )

    row_builder.add_graph(memory_usage)

    open_fd_count = graph(
        'Open FD Count',
        description='The count of open FD count of TiCDC',
        unit='short',
    )

    open_fd_count.add_query(
        expr_simple(
            'process_open_fds',
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}',
        format='time_series',
    )

    row_builder.add_graph(open_fd_count)

    ownership_history = graph(
        'Ownership History',
        description='The history of TiCDC cluster ownership, owner node has a value that is great than 0',
        unit='none',
        min='0',
    )

    ownership_history.add_query(
        expr_sum_rate(
            'ticdc_owner_ownership_counter',
            by_labels=['instance'],
            scope='instance',
            window='240s',
        ).op('> BOOL', '0.5'),
        legend='{{instance}}',
        format='time_series',
    )

    row_builder.add_graph(ownership_history)

    pd_leader_history = graph(
        'PD Leader History',
        description='The history of PD cluster leadership, leader node has a value that is great than 0',
        unit='none',
        min='0',
    )

    pd_leader_history.add_query(
        expr_simple(
            'pd_tso_role',
            scope='cluster',
            selectors=[eq('dc', 'global')],
        ).op('> BOOL', '0.5'),
        legend='PD-{{instance}}',
        format='time_series',
    )

    row_builder.add_graph(pd_leader_history)

    build_info = table(
        'Build Info',
        description='Build metadata of each TiCDC server instance.',
    )

    build_info.add_query(
        expr_max(
            'ticdc_server_build_info',
            by_labels=[
                'instance',
                'kernel_type',
                'git_hash',
                'release_version',
                'utc_build_time',
            ],
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        format='time_series',
        instant=True,
    )

    build_info.add_transformation(
        transformation('labelsToFields'),
    ).add_transformation(
        transformation(
            'organize',
            {
                'excludeByName': {'Metric': True, 'Time': True, 'Value': True, '__name__': True},
                'indexByName': {
                    'git_hash': 2,
                    'instance': 0,
                    'kernel_type': 1,
                    'release_version': 3,
                    'utc_build_time': 4,
                },
                'renameByName': {},
            },
        ),
    )

    row_builder.add_table(build_info)

    log_write_speed = graph(
        'Log Write Speed',
        description='Log write speed of each TiCDC instance.',
        unit='Bps',
    )

    log_write_speed.add_query(
        expr_sum_rate(
            'ticdc_logger_write_bytes_total',
            by_labels=['instance'],
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}',
        format='time_series',
    )

    row_builder.add_graph(log_write_speed)

    log_size_disk_usage = graph(
        'Log Size & Disk Usage',
        description='Log size and disk usage of the filesystem containing each TiCDC log file directory.',
        unit='bytes',
    )

    log_size_disk_usage.add_query(
        expr_max(
            'ticdc_logger_total_size_bytes',
            by_labels=['instance'],
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}-log_total',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_logger_disk_used_bytes',
            by_labels=['instance'],
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}-disk_used',
        ref='B',
        format='time_series',
    ).add_query(
        expr_max(
            'ticdc_logger_disk_total_bytes',
            by_labels=['instance'],
            scope='cluster',
            selectors=[regex('job', '.*ticdc.*'), regex('instance', '$ticdc_instance')],
        ),
        legend='{{instance}}-disk_total',
        ref='C',
        format='time_series',
    )

    row_builder.add_graph(log_size_disk_usage)

    return row_builder.build()
