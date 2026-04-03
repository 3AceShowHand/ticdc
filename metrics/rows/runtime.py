# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec
from metrics.queries import Expr, eq, expr_simple, not_regex


def build_runtime_row() -> RowSpec:
    row_builder = row("Runtime $runtime_instance")
    next_gc_bytes = expr_simple("go_memstats_next_gc_bytes", scope="runtime_instance")
    heap_alloc_bytes = expr_simple("go_memstats_heap_alloc_bytes", scope="runtime_instance")
    gc_percent = expr_simple("ticdc_server_go_gc", scope="runtime_instance").op("/", "100")
    estimated_inuse = next_gc_bytes.op("/", f"(1 + {gc_percent})")
    last_gc_time = expr_simple("go_memstats_last_gc_time_seconds", scope="runtime_instance")
    alloc_total = expr_simple("go_memstats_alloc_bytes_total", scope="runtime_instance")

    memory_usage = (
        graph(
            "Memory Usage",
            description="TiCDC process rss memory usage. TiCDC heap memory size in use ",
            unit="bytes",
            min="0",
        )
        .add_query(
            expr_simple("process_resident_memory_bytes", scope="runtime_instance"),
            legend="alloc-from-os",
        )
        .add_query(
            estimated_inuse,
            legend="estimate-inuse",
            ref="H",
        )
        .add_query(
            heap_alloc_bytes.op("-", estimated_inuse),
            legend="estimate-garbage",
            ref="C",
        )
        .add_query(
            expr_simple("go_memstats_heap_idle_bytes", scope="runtime_instance")
            .op("-", expr_simple("go_memstats_heap_released_bytes", scope="runtime_instance"))
            .op("+", expr_simple("go_memstats_heap_inuse_bytes", scope="runtime_instance"))
            .op("-", heap_alloc_bytes),
            legend="reserved-by-go",
        )
        .add_query(
            expr_simple("go_memstats_stack_sys_bytes", scope="runtime_instance")
            .op("+", expr_simple("go_memstats_mspan_sys_bytes", scope="runtime_instance"))
            .op("+", expr_simple("go_memstats_mcache_sys_bytes", scope="runtime_instance"))
            .op("+", expr_simple("go_memstats_buck_hash_sys_bytes", scope="runtime_instance"))
            .op("+", expr_simple("go_memstats_gc_sys_bytes", scope="runtime_instance"))
            .op("+", expr_simple("go_memstats_other_sys_bytes", scope="runtime_instance")),
            legend="used-by-go",
        )
        .add_query(
            next_gc_bytes,
            legend="gc-threshold",
        )
        .add_query(
            Expr(
                f"(clamp_max({last_gc_time.call('idelta', range_selector='1m')}, 1) * "
                f"{next_gc_bytes}) > 0"
            ),
            legend="gc",
        )
    )

    estimated_live_objects = graph(
        "Estimated Live Objects",
        description="Count of live objects.",
        min="0",
    ).add_query(
        expr_simple("go_memstats_heap_objects", scope="runtime_instance"),
        legend="objects",
    )

    gc_stw_duration_last_256_gc_cycles = (
        graph(
            "GC STW Duration (last 256 GC cycles)",
            description="TiCDC process Go garbage collection STW pause duration",
            unit="s",
            min="0",
        )
        .add_range_query(
            expr_simple(
                "go_gc_duration_seconds",
                scope="runtime_instance",
                selectors=[eq("quantile", "0")],
            ),
            legend="min",
        )
        .add_range_query(
            expr_simple(
                "go_gc_duration_seconds",
                scope="runtime_instance",
                selectors=[not_regex("quantile", "0|1")],
            ),
            legend="{{quantile}}",
        )
        .add_range_query(
            expr_simple(
                "go_gc_duration_seconds",
                scope="runtime_instance",
                selectors=[eq("quantile", "1")],
            ),
            legend="max",
        )
    )

    allocator_throughput = (
        graph(
            "Allocator Throughput",
            description="The throughput of Go's memory allocator.",
            unit="Bps",
        )
        .add_query(
            alloc_total.call("irate", range_selector="30s"),
            legend="alloc",
        )
        .add_query(
            Expr(f"({alloc_total} - {heap_alloc_bytes})").call(
                "irate",
                range_selector="30s:",
            ),
            legend="sweep",
        )
        .add_query(
            expr_simple("go_memstats_mallocs_total", scope="runtime_instance").call(
                "irate",
                range_selector="30s",
            ),
            legend="alloc-ops",
        )
        .add_query(
            expr_simple("go_memstats_frees_total", scope="runtime_instance").call(
                "irate",
                range_selector="30s",
            ),
            legend="swepp-ops",
        )
    )

    row_builder.add_panels(
        memory_usage,
        estimated_live_objects,
    )

    row_builder.add_panels(
        gc_stw_duration_last_256_gc_cycles,
        allocator_throughput,
    )

    return row_builder.build()
