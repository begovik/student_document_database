"""Імена файлів і класифікація відповідей для вивантаження джерел.

Каталог живе роками і відкривається у файловому менеджері, тому
імена мають бути читабельними, а не лише валідними. Кожне правило
тут з'явилося через конкретний артефакт у пулі.
"""


import importlib.util
import pathlib
import sys

import pytest

_SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "fetch_card_sources.py"
_spec = importlib.util.spec_from_file_location("fetch_card_sources", _SCRIPT)
fetch = importlib.util.module_from_spec(_spec)
sys.modules["fetch_card_sources"] = fetch
_spec.loader.exec_module(fetch)


def test_filename_keeps_readable_cyrillic():
    name = fetch.filename(7, "2026", "Договір про сплату аліментів на дитину")
    assert name == "07_2026_договір-про-сплату-аліментів-на-дитину.pdf"


def test_filename_does_not_glue_words_at_slash():
    """Роздільник має стати пробілом, інакше «малолітніх/неповнолітніх»
    склеїться в нерозбірливе «малолітніхнеповнолітніх»."""
    name = fetch.safe_name("за участі малолітніх/неповнолітніх")
    assert name == "за-участі-малолітніх-неповнолітніх"


def test_filename_strips_quotes_and_colons():
    assert "«" not in fetch.safe_name("Поняття «ДИТИНА»: контекст")
    assert ":" not in fetch.safe_name("Поняття «ДИТИНА»: контекст")


def test_filename_fits_filesystem_limit():
    """ext4 — 255 байт на компонент. Кирилиця по 2 байти, тому рахуємо
    саме байти, а не символи."""
    name = fetch.filename(1, "2026", "Д" * 900)
    assert len(name.encode()) <= 255


def test_filename_survives_empty_title():
    assert fetch.filename(1, "", "").endswith("_без-року_dzherelo.pdf")


def test_filename_prefixed_with_number_and_year():
    name = fetch.filename(42, "2015", "Умови та порядок")
    assert name.startswith("42_2015_")


def test_parse_rows_fills_url_from_doi():
    # поля: tier rej udc year authors title publisher doi url landing extra
    raw = "T0\x1f\x1f347.1\x1f2026\x1f[]\x1fНазва\x1f\x1f10.1/x\x1f\x1f\x1f{}"
    rows = fetch.parse_rows(raw)
    assert len(rows) == 1
    assert rows[0]["doi"] == "10.1/x"
    assert rows[0]["url"] == "https://doi.org/10.1/x"


def test_parse_rows_reads_authors_through_shared_helpers():
    """Автори проходять той самий розбір, що й у списку джерел —
    інакше каталог і список покажуть різних авторів."""
    raw = (
        "T0\x1f\x1f347.1\x1f2026\x1f"
        + '["\\u0421. \\u0421. \\u0420\\u043e\\u0437\\u0441\\u043e\\u0445\\u0430"]'
        + "\x1fНазва\x1f\x1f\x1f\x1fhttps://e.org/a.pdf\x1f{}"
    )
    rows = fetch.parse_rows(raw)
    assert rows[0]["authors_out"] == "С. С. Розсоха"


def test_parse_rows_skips_garbage_lines():
    rows = fetch.parse_rows("psql: помилка\n\\set m1 'x'\nкороткий\n")
    assert rows == []


@pytest.mark.parametrize(
    "prefix",
    ["HTTP ", "ТАЙМАУТ", "МЕРЕЖА", "НЕ ФАЙЛ", "ЗАМАЛИЙ", "ЗАБАГАТО ВЕЛИКИЙ",
     "ЗАБЛОКОВАНО", "НЕМАЄ URL"],
)
def test_manifest_groups_cover_every_skipped_kind(prefix):
    """Кожен можливий відмів має бути пояснений у README, інакше
    власник не зрозуміє, чому файлу немає."""
    src = pathlib.Path(_SCRIPT).read_text(encoding="utf-8")
    assert f'("{prefix}' in src


def test_hash_sql_sets_psql_format():
    """Без pset-директив psql екранує 0x1F, запит повертає 0 рядків
    МОВЧКИ, і звірка хешів виглядає як «усе сходиться» — але не
    перевіряє нічого."""
    sql = fetch.build_hash_sql(
        [{"url": "https://e.org/a.pdf", "doi": "", "status": "збережено"}]
    )
    assert "\\pset format unaligned" in sql
    assert "\\pset tuples_only on" in sql
    assert "\\pset fieldsep" in sql


def test_hash_sql_looks_up_both_url_and_doi():
    """Частина джерел у базі має DOI, а не прямий URL."""
    sql = fetch.build_hash_sql(
        [{"url": "https://doi.org/10.1/x", "doi": "10.1/x", "status": "уже є"}]
    )
    assert "'10.1/x'" in sql
    assert "canonical_url IN" in sql


def test_hash_sql_escapes_quotes():
    sql = fetch.build_hash_sql(
        [{"url": "https://e.org/it's.pdf", "doi": "", "status": "збережено"}]
    )
    assert "it''s.pdf" in sql


def test_db_hashes_short_circuits_without_downloads():
    rows = [{"url": "https://e.org/a.pdf", "doi": "", "status": "НЕ ФАЙЛ (HTML)"}]
    assert fetch.db_hashes(rows) == {}


def test_db_hashes_indexes_by_url_and_doi(monkeypatch):
    """Один рядок із БД має відкриватися і за URL, і за DOI."""
    line = "https://e.org/a.pdf\x1f10.1/x\x1f" + "f" * 64
    monkeypatch.setattr(fetch, "run_psql", lambda sql: line + "\n")
    rows = [{"url": "https://e.org/a.pdf", "doi": "10.1/x", "status": "збережено"}]
    got = fetch.db_hashes(rows)
    assert got["https://e.org/a.pdf"] == "f" * 64
    assert got["10.1/x"] == "f" * 64


def test_file_sha256_matches_disk(tmp_path):
    import hashlib

    f = tmp_path / "a.pdf"
    f.write_bytes(b"%PDF-1.7 content")
    assert fetch.file_sha256(f) == hashlib.sha256(b"%PDF-1.7 content").hexdigest()


def test_record_has_every_manifest_column(tmp_path):
    """Запис і маніфест мають бути одного складу — інакше CSV розсиплеться.

    Перевіряється на шляху, де запит не йде в мережу: guard зупиняє
    локальну адресу ще до завантаження.
    """
    import asyncio

    row = {
        "title": "Назва", "authors": "[]", "year": "2026", "udc": "347.1",
        "doi": "", "url": "http://127.0.0.1/secret.pdf", "authors_out": "",
    }
    record = asyncio.run(fetch.fetch_one(None, row, 1, tmp_path))

    assert record["status"].startswith("ЗАБЛОКОВАНО")
    for field in ("n", "file", "title", "authors", "year", "udc", "doi", "url",
                  "status", "bytes", "sha256"):
        assert field in record