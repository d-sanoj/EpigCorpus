"""Enforce the dependency rule between the layers of the epigcorpus package.

    api           ->  services  ->  core, models  (and adapters, only through ports)
    adapters      ->  core, models
    every layer   ->  config

Services never import adapters: they receive them, typed by the Protocols in
epigcorpus/services/ports.py, from a composition root. There are two:
epigcorpus/api/server.py wires real adapters into services for a served run,
and epigcorpus/export/ builds the same Catalog to write the corpus out as
files. Nothing else outside the adapter layer may import adapters. See
docs/adr/0001-layered-app-layout.md.
"""

import ast
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[1] / "src" / "epigcorpus"

ALLOWED = {
    "config": {"config"},
    "models": {"config", "models"},
    "core": {"config", "models", "core"},
    "adapters": {"config", "models", "core", "adapters"},
    "services": {"config", "models", "core", "services"},
    "api": {"config", "models", "services", "api"},
}

COMPOSITION_ROOTS = {
    "api/server.py": ALLOWED["api"] | {"adapters"},
    "api/workers.py": ALLOWED["api"] | {"adapters"},
    # The static exporter wires adapters into a Catalog and reuses the API's
    # own names and serialisers, so it reads the API's allowance plus core.
    "export/__init__.py": set(),
    "export/static_site.py": ALLOWED["api"] | {"adapters", "core"},
}

# Third-party packages that belong to one layer only.
FRAMEWORK_ONLY = {
    "streamlit": set(),  # removed; the UI is frontend/, a client of the API
    "fastapi": {"api"},
    "starlette": {"api"},
    "pydantic": {"api"},
    "uvicorn": set(),  # started from outside: uvicorn epigcorpus.api.server:app
}

NOT_CHECKED: set[str] = set()


def _modules():
    for path in sorted(APP.rglob("*.py")):
        rel = path.relative_to(APP)
        if rel.parts[0] in NOT_CHECKED:
            continue
        yield rel, path


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module == "epigcorpus":
                for alias in node.names:
                    yield f"epigcorpus.{alias.name}"
            else:
                yield node.module


def _layer(rel: Path) -> str:
    return rel.parts[0] if len(rel.parts) > 1 else rel.as_posix()


CASES = [(rel.as_posix(), path) for rel, path in _modules()]


@pytest.mark.parametrize("rel,path", CASES, ids=[c[0] for c in CASES])
def test_module_respects_the_dependency_rule(rel, path):
    rel_path = Path(rel)
    layer = _layer(rel_path)
    if rel in COMPOSITION_ROOTS:
        allowed = COMPOSITION_ROOTS[rel]
    elif layer == "__init__.py":
        allowed = set()
    else:
        assert layer in ALLOWED, f"{rel}: unknown layer {layer!r}; add it to ALLOWED"
        allowed = ALLOWED[layer]

    violations = []
    for name in _imports(path):
        parts = name.split(".")
        if parts[0] == "epigcorpus":
            if len(parts) < 2:
                violations.append(name)
                continue
            target = parts[1]
            if target not in allowed:
                violations.append(name)
        elif parts[0] in FRAMEWORK_ONLY and layer not in FRAMEWORK_ONLY[parts[0]]:
            violations.append(name)
    assert not violations, f"{rel} ({layer}) may not import: {violations}"


def test_every_layer_is_present():
    present = {rel.parts[0] for rel, _ in _modules() if len(rel.parts) > 1}
    for layer in ("config", "models", "core", "adapters", "services", "api"):
        assert layer in present
