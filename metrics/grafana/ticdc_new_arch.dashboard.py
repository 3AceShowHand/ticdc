# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from ticdc_new_arch.dashboard import build_dashboard, build_dashboard_spec
