from harvester.curator.verifier import _catalog_validation_error
from harvester.extract.engine import _has_meaningful_quotations, _has_meaningful_summary


def test_empty_extraction_is_not_meaningful():
    assert _has_meaningful_quotations([]) is False
    assert _has_meaningful_summary(None) is False


def test_partial_extraction_is_meaningful():
    assert _has_meaningful_quotations([{"page": 1, "text": "Твердження", "type": "fact"}]) is True
    assert _has_meaningful_summary({"sections": [{"page": 1, "title": "Вступ"}]}) is True


def test_catalog_validation_flags_missing_extraction_data():
    doc = {"id": 10, "title": "Документ", "quotations": [], "summary": None}
    assert _catalog_validation_error(doc) == "відсутні quotations і summary"


def test_catalog_validation_accepts_document_with_summary():
    doc = {
        "id": 11,
        "title": "Документ",
        "quotations": [],
        "summary": {"sections": [{"page": 1, "title": "Вступ"}]},
    }
    assert _catalog_validation_error(doc) is None
