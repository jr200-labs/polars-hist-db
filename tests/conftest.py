"""Pytest collection guard for integration-marked test directories.

Several test modules import the package's DB / NATS layers at module
load, which fails on a vanilla runner (no MySQL, no NATS server). The
`pytest.mark.integration` marker filters those tests out of selection
but doesn't prevent the file from being collected — and a collection
error stops the whole run before the marker filter applies.

When the active mark expression excludes integration (the default
`addopts = "-m 'not integration'"`), skip collection of the
infra-only directories entirely. Run them via
`pytest -m integration` in a workflow that sets up the infra.
"""

INTEGRATION_DIRS = ("sql", "dataset", "nats", "audit")


def pytest_ignore_collect(collection_path, config):
    markexpr = config.getoption("-m", default="") or ""
    if "not integration" not in markexpr:
        return None
    p = str(collection_path)
    for d in INTEGRATION_DIRS:
        if f"/tests/{d}/" in p or p.endswith(f"/tests/{d}"):
            return True
    return None
