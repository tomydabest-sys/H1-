"""Scaffold sanity checks: repo structure and guard-rail files exist as expected."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

MODULES = ["ingest", "normalize", "calibration", "fairvalue", "gate"]
DATA_DIRS = ["raw", "interim", "curated"]


def test_src_modules_exist():
    for mod in MODULES:
        assert (ROOT / "src" / mod / "__init__.py").is_file(), f"missing src/{mod}"


def test_data_dirs_exist():
    for d in DATA_DIRS:
        assert (ROOT / "data" / d).is_dir(), f"missing data/{d}"


def test_guardrail_docs_exist():
    for name in ["CLAUDE.md", "PROPOSALS.md", "PREREGISTRATION.md", "README.md"]:
        assert (ROOT / name).is_file(), f"missing {name}"


def test_preregistration_not_yet_authored():
    """Until the pre-registration tag exists, the file must self-identify as a placeholder.

    This test is deleted in the same commit that authors PREREGISTRATION.md for real,
    before Phase 2 test code is written.
    """
    text = (ROOT / "PREREGISTRATION.md").read_text()
    assert "NOT YET AUTHORED" in text
