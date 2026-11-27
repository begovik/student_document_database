from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _load_extract_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "extract_from_catalog.py"
    spec = spec_from_file_location("extract_from_catalog", path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_dedupe_documents_preserves_first_occurrence():
    module = _load_extract_module()
    docs = [
        {"id": 1, "title": "A"},
        {"id": 2, "title": "B"},
        {"id": 1, "title": "A2"},
        {"id": 3, "title": "C"},
        {"id": 2, "title": "B2"},
    ]

    unique, dropped = module.dedupe_documents(docs)

    assert dropped == 2
    assert [d["id"] for d in unique] == [1, 2, 3]
    assert unique[0]["title"] == "A"
    assert unique[1]["title"] == "B"
