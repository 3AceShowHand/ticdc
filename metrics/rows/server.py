# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row, table
from metrics.dsl.specs import RowSpec
from metrics.queries import (
    eq,
    expr_max,
    expr_rate,
    expr_simple,
    expr_sum_rate,
    regex,
)


def build_server_row() -> RowSpec:
    row_builder = row("Server")
    ticdc_job_selectors = [
        regex("job", ".*ticdc.*"),
        regex("instance", "$ticdc_instance"),
    ]
    ticdc_start_time = expr_simple(
        "process_start_time_seconds",
        scope="cluster",
        selectors=ticdc_job_selectors,
    )
    tikv_start_time = expr_simple(
        "process_start_time_seconds",
        scope="cluster",
        selectors=[regex("job", ".*tikv.*")],
    )
    pd_start_time = expr_simple(
        "process_start_time_seconds",
        scope="cluster",
        selectors=[regex("job", ".*pd.*")],
    )

    uptime = (
        graph(
            "Uptime",
            description="Uptime of TiCDC and TiKV",
            unit="dtdurations",
        )
        .add_query(
            f"time() - {ticdc_start_time}",
            legend="TiCDC-{{instance}}",
        )
        .add_query(
            f"time() - {tikv_start_time}",
            legend="TiKV-{{instance}}",
            ref="B",
        )
        .add_query(
            f"time() - {pd_start_time}",
            legend="PD-{{instance}}",
            ref="C",
        )
    )

    cpu_usage = (
        graph("CPU Usage", description="CPU usage of TiCDC", unit="percentunit")
        .add_query(
            expr_rate(
                "process_cpu_seconds_total",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="{{instance}}",
        )
        .add_query(
            expr_simple(
                "ticdc_server_go_max_procs",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="quota-{{instance}}",
            ref="B",
        )
    )

    row_builder.add_panels(uptime, cpu_usage)

    goroutine_count = (
        graph(
            "Goroutine Count",
            description="Goroutine count of TiCDC",
            unit="short",
        )
        .add_query(
            expr_simple(
                "go_goroutines",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="{{instance}}",
        )
        .add_query(
            expr_simple(
                "go_threads",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="threads-{{instance}}",
            ref="B",
            hide=True,
        )
    )

    memory_usage = (
        graph(
            "Memory Usage",
            description="Memory usage of TiCDC",
            unit="bytes",
        )
        .add_query(
            expr_simple(
                "process_resident_memory_bytes",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="process-{{instance}}",
        )
        .add_query(
            expr_simple(
                "go_memstats_heap_alloc_bytes",
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="heap-{{instance}}",
            ref="B",
        )
    )

    row_builder.add_panels(goroutine_count, memory_usage)

    open_fd_count = graph(
        "Open FD Count",
        description="The count of open FD count of TiCDC",
        unit="short",
    ).add_query(
        expr_simple(
            "process_open_fds",
            scope="cluster",
            selectors=ticdc_job_selectors,
        ),
        legend="{{instance}}",
    )

    ownership_history = graph(
        "Ownership History",
        description=(
            "The history of TiCDC cluster ownership. The owner node has a value greater than 0."
        ),
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_sum_rate(
            "ticdc_owner_ownership_counter",
            by_labels=["instance"],
            scope="instance",
            window="240s",
        ).op("> BOOL", "0.5"),
        legend="{{instance}}",
    )

    row_builder.add_panels(open_fd_count, ownership_history)

    pd_leader_history = graph(
        "PD Leader History",
        description=(
            "The history of PD cluster leadership. The leader node has a value greater than 0."
        ),
        unit="none",
        min="0",
        decimals=0,
    ).add_query(
        expr_simple(
            "pd_tso_role",
            scope="cluster",
            selectors=[eq("dc", "global")],
        ).op("> BOOL", "0.5"),
        legend="PD-{{instance}}",
    )

    build_info = table(
        "Build Info",
        description="Build metadata of each TiCDC server instance.",
    ).add_label_query(
        expr_max(
            "ticdc_server_build_info",
            by_labels=[
                "instance",
                "kernel_type",
                "git_hash",
                "release_version",
                "utc_build_time",
            ],
            scope="cluster",
            selectors=ticdc_job_selectors,
        ),
        columns=[
            "instance",
            "kernel_type",
            "git_hash",
            "release_version",
            "utc_build_time",
        ],
    )

    row_builder.add_panels(pd_leader_history, build_info)

    log_write_speed = graph(
        "Log Write Speed", description="Log write speed of each TiCDC instance.", unit="Bps"
    ).add_query(
        expr_sum_rate(
            "ticdc_logger_write_bytes_total",
            by_labels=["instance"],
            scope="cluster",
            selectors=ticdc_job_selectors,
        )
    )

    log_size_disk_usage = (
        graph(
            "Log Size & Disk Usage",
            description=(
                "Log size and disk usage of the filesystem containing each "
                "TiCDC log file directory."
            ),
            unit="bytes",
        )
        .add_query(
            expr_max(
                "ticdc_logger_total_size_bytes",
                by_labels=["instance"],
                scope="cluster",
                selectors=ticdc_job_selectors,
            ),
            legend="{{instance}}-log_total",
        )
        .add_query(
            expr_max(
                "ticdc_logger_disk_used_bytes",
                by_labels=["instance"],
                scope="cluster",
                selectors=[
                    regex("job", ".*ticdc.*"),
                    regex("instance", "$ticdc_instance"),
                ],
            ),
            legend="{{instance}}-disk_used",
            ref="B",
        )
        .add_query(
            expr_max(
                "ticdc_logger_disk_total_bytes",
                by_labels=["instance"],
                scope="cluster",
                selectors=[
                    regex("job", ".*ticdc.*"),
                    regex("instance", "$ticdc_instance"),
                ],
            ),
            legend="{{instance}}-disk_total",
            ref="C",
        )
    )

    row_builder.add_panels(log_write_speed, log_size_disk_usage)

    return row_builder.build()
