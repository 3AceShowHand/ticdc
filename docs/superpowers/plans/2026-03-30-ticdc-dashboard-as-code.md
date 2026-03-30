# TiCDC Dashboard-as-Code Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert TiCDC dashboard maintenance to a compatibility-first dashboard-as-code workflow with Python as the source of truth for the base dashboard and checked-in generated artifacts for all three dashboards.

**Architecture:** Add a stdlib-only Python source model for the base dashboard, generate the base JSON from `metrics/grafana/ticdc_new_arch.dashboard.py`, keep the current next-gen `sed`/`jq` derivation semantics, and enforce freshness through regeneration plus `git diff` while using Python-based checksum and structural validation for all generated dashboards.

**Tech Stack:** Python 3 stdlib (`json`, `hashlib`, `runpy`, `pathlib`, `unittest`), existing shell scripts, `jq`, GNU/portable `sed`, Makefile wiring.

---

### Task 1: Add Python Dashboard Source Model And Failing Tool Tests

**Files:**
- Create: `metrics/grafana/common.py`
- Create: `metrics/grafana/ticdc_new_arch.dashboard.py`
- Create: `scripts/tests/test_ticdc_dashboard_tools.py`
- Test: `scripts/tests/test_ticdc_dashboard_tools.py`

- [ ] **Step 1: Write failing Python unit tests for generator/checker helpers**

Create `scripts/tests/test_ticdc_dashboard_tools.py` with focused stdlib `unittest`
coverage for the behavior we need from the new toolchain:

```python
import importlib.util
import json
import pathlib
import tempfile
import unittest


def load_module(path: pathlib.Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DashboardToolsTest(unittest.TestCase):
    def test_build_dashboard_contract_returns_dict(self):
        module = load_module(
            pathlib.Path("metrics/grafana/ticdc_new_arch.dashboard.py"),
            "ticdc_dashboard",
        )
        dashboard = module.build_dashboard()
        self.assertIsInstance(dashboard, dict)
        self.assertEqual("test-cluster-TiCDC-New-Arch", dashboard["title"])

    def test_checker_detects_checksum_mismatch(self):
        checker = load_module(
            pathlib.Path("scripts/check-ticdc-dashboard.py"),
            "dashboard_checker",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            data = root / "dashboard.json"
            sha = root / "dashboard.json.sha256"
            data.write_text("{\"title\": \"x\"}\n", encoding="utf-8")
            sha.write_text(
                "0" * 64 + "  dashboard.json\n",
                encoding="utf-8",
            )
            messages = checker.check_checksums(root, [sha])
            self.assertTrue(any("checksum mismatch" in msg.lower() for msg in messages))
```

- [ ] **Step 2: Run the new test file to verify it fails for the right reason**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
```

Expected:

- FAIL because `metrics/grafana/ticdc_new_arch.dashboard.py` does not exist yet
- or FAIL because the checker does not expose checksum helpers yet

- [ ] **Step 3: Add the minimal Python source model for the base dashboard**

Implement:

- `metrics/grafana/common.py` with minimal helpers/constants only
- `metrics/grafana/ticdc_new_arch.dashboard.py` exposing `build_dashboard() -> dict`

Use a compatibility-first structure:

```python
from pathlib import Path
import json


def load_existing_dashboard() -> dict:
    path = Path(__file__).with_name("ticdc_new_arch.json")
    return json.loads(path.read_text(encoding="utf-8"))


def build_dashboard() -> dict:
    dashboard = load_existing_dashboard()
    return dashboard
```

Notes for implementation:

- Keep the first implementation minimal and green. It is acceptable to load the
  current JSON as the first source-model step so tests can pass before the
  generator is introduced.
- Do not redesign the dashboard structure in this task.

- [ ] **Step 4: Re-run the unit tests to verify the new Python contract works**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
```

Expected:

- the contract test passes
- the checksum-mismatch test still fails until checker support is added in Task 2

- [ ] **Step 5: Commit the source-model and test scaffold**

```bash
git add metrics/grafana/common.py metrics/grafana/ticdc_new_arch.dashboard.py scripts/tests/test_ticdc_dashboard_tools.py
git commit -m "metrics: add TiCDC dashboard source model scaffold"
```

### Task 2: Add Unified Generator, Deterministic JSON Output, And Checksum Support

**Files:**
- Create: `scripts/gen-ticdc-dashboards`
- Modify: `scripts/check-ticdc-dashboard.py`
- Modify: `scripts/generate-next-gen-metrics.sh`
- Create: `metrics/grafana/ticdc_new_arch.json.sha256`
- Create: `metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256`
- Create: `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256`
- Modify: `metrics/grafana/ticdc_new_arch.json`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_next_gen.json`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json`
- Test: `scripts/tests/test_ticdc_dashboard_tools.py`

- [ ] **Step 1: Extend the test file with failing generator/checksum tests**

Add tests for:

- deterministic JSON writing with newline
- checksum file generation in `<hex><two spaces><relative path>` format
- repo-root-relative checksum verification

Example additions:

```python
    def test_checksum_line_uses_repo_relative_path(self):
        generator = load_module(
            pathlib.Path("scripts/gen-ticdc-dashboards"),
            "dashboard_generator",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            dashboard = root / "metrics/grafana/ticdc_new_arch.json"
            dashboard.parent.mkdir(parents=True, exist_ok=True)
            dashboard.write_text("{\"title\": \"x\"}\n", encoding="utf-8")
            line = generator.make_checksum_line(root, dashboard)
            self.assertTrue(line.endswith("  metrics/grafana/ticdc_new_arch.json"))
```

- [ ] **Step 2: Run the tests to verify the missing generator/checksum behavior fails**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
```

Expected:

- FAIL due to missing `scripts/gen-ticdc-dashboards`
- or FAIL due to missing checksum helper functions in the checker/generator

- [ ] **Step 3: Implement the unified generator and Python checksum helpers**

Implement `scripts/gen-ticdc-dashboards` as a Python executable script with:

- repo-root discovery from `__file__`
- Python version preflight (`>= 3.8`)
- `runpy.run_path()` loading of `metrics/grafana/ticdc_new_arch.dashboard.py`
- deterministic `json.dump(..., indent=2, ensure_ascii=False, sort_keys=False)`
- trailing newline writes
- invocation of `scripts/generate-next-gen-metrics.sh`
- checksum generation with `hashlib.sha256`

Implement checker support in `scripts/check-ticdc-dashboard.py`:

- add `check_checksums(repo_root, checksum_files) -> list[str]`
- keep duplicate-ID and overlap checks
- support checking all three dashboard JSON files in one run

Restrict `scripts/generate-next-gen-metrics.sh` changes to:

- repo-root discovery
- caller-CWD independence
- invocation/path compatibility with the new generator

Do not change its `sed`/`jq` transformation semantics.

- [ ] **Step 4: Run the tool tests and the generator to verify green behavior**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
python3 scripts/gen-ticdc-dashboards
python3 scripts/check-ticdc-dashboard.py
```

Expected:

- unit tests pass
- generator rewrites all three JSON files and three `.sha256` files
- checker exits cleanly when artifacts match

- [ ] **Step 5: Commit generator/checksum implementation and generated artifacts**

```bash
git add scripts/gen-ticdc-dashboards scripts/check-ticdc-dashboard.py scripts/generate-next-gen-metrics.sh metrics/grafana/ticdc_new_arch.json metrics/grafana/ticdc_new_arch.json.sha256 metrics/nextgengrafana/ticdc_new_arch_next_gen.json metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256 metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256 scripts/tests/test_ticdc_dashboard_tools.py
git commit -m "metrics: add TiCDC dashboard generator"
```

### Task 3: Wire README, Shell Entry Point, And Makefile To The New Flow

**Files:**
- Create: `metrics/grafana/README.md`
- Modify: `scripts/check-ticdc-dashboard.sh`
- Modify: `Makefile`
- Test: `scripts/tests/test_ticdc_dashboard_tools.py`

- [ ] **Step 1: Add failing tests for repo-root-safe checker invocation if needed**

If the current unit test file does not already cover it, add a small failing
test for shell-wrapper expectations by asserting the Python checker can be run
from a non-repo CWD using repo-root-relative checksum paths.

- [ ] **Step 2: Run the targeted test to verify the wrapper/CLI expectation fails**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
```

Expected:

- FAIL if repo-root or multi-file checker invocation still assumes the caller is
  in the repository root

- [ ] **Step 3: Implement README and CI/dev wiring**

Implement:

- `metrics/grafana/README.md` documenting:
  - Python is the source of truth
  - JSON files are generated artifacts
  - run `python3 scripts/gen-ticdc-dashboards`
  - do not manually edit generated JSON or `.sha256`
- `scripts/check-ticdc-dashboard.sh` as a thin wrapper that computes repo root
  and invokes the Python checker
- Makefile wiring:
  - `generate-next-gen-grafana` should call `python3 scripts/gen-ticdc-dashboards`
  - `check-ticdc-dashboard` should call the shell wrapper
  - `make check` should still regenerate artifacts and then fail on
    `git diff --exit-code`

- [ ] **Step 4: Run the targeted checks and Makefile entry points**

Run:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
./scripts/check-ticdc-dashboard.sh
make generate-next-gen-grafana
```

Expected:

- tests pass
- shell checker succeeds from repo root
- Makefile generation target succeeds and leaves no unexpected diff on a second run

- [ ] **Step 5: Commit workflow wiring and docs**

```bash
git add metrics/grafana/README.md scripts/check-ticdc-dashboard.sh Makefile scripts/tests/test_ticdc_dashboard_tools.py
git commit -m "docs: wire TiCDC dashboard workflow"
```

### Task 4: Full Verification And Artifact Freshness Gate

**Files:**
- Modify: `metrics/grafana/ticdc_new_arch.json`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_next_gen.json`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json`
- Modify: `metrics/grafana/ticdc_new_arch.json.sha256`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256`
- Modify: `metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256`
- Test: `scripts/tests/test_ticdc_dashboard_tools.py`

- [ ] **Step 1: Run the full failing/passing verification matrix**

Run these commands in order:

```bash
python3 -m unittest scripts.tests.test_ticdc_dashboard_tools -v
python3 scripts/gen-ticdc-dashboards
python3 scripts/check-ticdc-dashboard.py
./scripts/check-ticdc-dashboard.sh
make check-ticdc-dashboard
make generate-next-gen-grafana
git diff --exit-code -- metrics/grafana metrics/nextgengrafana scripts/check-ticdc-dashboard.sh scripts/check-ticdc-dashboard.py scripts/generate-next-gen-metrics.sh scripts/gen-ticdc-dashboards Makefile metrics/grafana/README.md scripts/tests/test_ticdc_dashboard_tools.py
```

Expected:

- unit tests pass
- generator succeeds
- Python checker succeeds
- shell checker succeeds
- generation target succeeds
- final `git diff --exit-code` returns success after regeneration

- [ ] **Step 2: Run full repository `make check`**

Run:

```bash
make check
```

Expected:

- exit code 0
- no dashboard-related diff remains afterward

- [ ] **Step 3: Inspect generated dashboard invariants**

Manually verify:

- base title remains `test-cluster-TiCDC-New-Arch`
- base UID remains `YiGL8hBZ0aac`
- templating variables remain:
  - `k8s_cluster`
  - `tidb_cluster`
  - `namespace`
  - `changefeed`
  - `ticdc_instance`
  - `tikv_instance`
  - `spike_threshold`
  - `runtime_instance`
- userscope/dashboard UID behavior remains compatible with the current
  derivation script

- [ ] **Step 4: Commit the final verified state**

```bash
git add metrics/grafana/ticdc_new_arch.json metrics/grafana/ticdc_new_arch.json.sha256 metrics/nextgengrafana/ticdc_new_arch_next_gen.json metrics/nextgengrafana/ticdc_new_arch_next_gen.json.sha256 metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json metrics/nextgengrafana/ticdc_new_arch_with_keyspace_name.json.sha256
git commit -m "metrics: finish TiCDC dashboard as code migration"
```

- [ ] **Step 5: Final review checkpoint**

Dispatch:

- spec-compliance review against
  `docs/superpowers/specs/2026-03-30-ticdc-dashboard-as-code-design.md`
- code-quality review for the implementation changes

Only mark the implementation complete after both reviews approve and the fresh
verification commands above still pass.
