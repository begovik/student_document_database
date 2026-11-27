from pathlib import Path

from harvester.config import load_config
from harvester.curator.preparer import is_document_complete
from harvester.curator.selector import parse_selection_response
from harvester.net.guards import is_private_ip


def _complete_document(**overrides):
    document = {
        "status": "verified",
        "title": "Повний науковий документ про методику",
        "authors": ["Автор Іваненко"],
        "language": "uk",
        "canonical_url": "https://example.org/document.pdf",
        "page_count": 8,
        "has_text_layer": 1,
        "doc_type": "article",
        "extra": {
            "text_length": 16000,
            "structure": {
                "has_references": True,
                "has_introduction": True,
                "has_conclusion": True,
                "toc_ratio": 0.02,
            },
        },
    }
    document.update(overrides)
    return document


def test_selection_parser_does_not_use_greedy_json_regex():
    result = parse_selection_response(
        'Ось відповідь: {"selected_ids": [4, "5", 4, -1, true], '
        '"suggested_count": 25, "reasoning": "JSON {всередині тексту}"} trailing'
    )

    assert result is not None
    assert result.selected_ids == [4, 5]
    assert result.suggested_count == 25


def test_strict_document_requires_full_structure():
    passed, reason = is_document_complete(_complete_document())

    assert passed is True
    assert reason is None


def test_unknown_language_is_not_complete():
    passed, reason = is_document_complete(_complete_document(language="unknown"))

    assert passed is False
    assert reason == "мова не визначена"


def test_thesis_is_not_a_target_source_in_strict_profile():
    passed, reason = is_document_complete(_complete_document(doc_type="thesis"))

    assert passed is False
    assert "дисертації" in reason


def test_ssrf_blocks_carrier_grade_nat_and_public_ip_is_allowed():
    assert is_private_ip("100.64.0.1") is True
    assert is_private_ip("192.0.2.1") is True
    assert is_private_ip("8.8.8.8") is False


def test_legacy_db_path_environment_has_priority(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "contact:\n  email: test@example.org\n"
        "database:\n  local_db_path: yaml.db\n"
        "paths:\n  db_path: legacy.yaml.db\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HARVESTER_DATABASE__LOCAL_DB_PATH", raising=False)
    monkeypatch.setenv("HARVESTER_PATHS__DB_PATH", str(tmp_path / "env.db"))

    settings = load_config(config_path)

    assert settings.database.local_db_path == str(tmp_path / "env.db")
    assert settings.db_path == tmp_path / "env.db"
