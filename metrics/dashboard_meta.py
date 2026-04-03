# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Dashboard-wide metadata shared by assembly, rendering, and annotations."""

from __future__ import annotations

from typing import Final

BASE_DASHBOARD_TITLE: Final = "test-cluster-TiCDC-New-Arch"
BASE_DASHBOARD_UID: Final = "YiGL8hBZ0aac"
# Keep this monotonic for the stable dashboard UID.
DASHBOARD_VERSION: Final = 41
DATASOURCE_INPUT_NAME: Final = "DS_TEST-CLUSTER"
DATASOURCE: Final = f"${{{DATASOURCE_INPUT_NAME}}}"
DATASOURCE_INPUT: Final[dict[str, object]] = {
    "name": DATASOURCE_INPUT_NAME,
    "label": DATASOURCE,
    "type": "datasource",
    "pluginId": "prometheus",
    "pluginName": "Prometheus",
}
