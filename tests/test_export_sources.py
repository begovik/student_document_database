"""Тести вивантаження списку джерел.

Скрипт `scripts/export_sources.py` не має доступу до бази, але його
перетворення метаданих — чиста логіка, яка вирішує, чи читабельний
список джерел для курсової. Кожне правило тут з'явилося через
конкретний артефакт у пулі (див. коментарі в самому скрипті), тому
закріплено тестом.
"""

import importlib.util
import json
import pathlib
import sys

import pytest

_SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "export_sources.py"
_spec = importlib.util.spec_from_file_location("export_sources", _SCRIPT)
export = importlib.util.module_from_spec(_spec)
sys.modules["export_sources"] = export
_spec.loader.exec_module(export)


def test_authors_unescaped_legacy_unicode():
    """Спадок 2026-08…09: authors писався з ensure_ascii=True, тому в базі
    лежить «[\\"\\u0421. \\u0421. \\u0420\\u043e\\u0437\\u0441\\u043e\\u0445\\u0430\\"]»."""
    raw = json.dumps(["С. С. Розсоха"])
    assert export.parse_authors(raw) == ["С. С. Розсоха"]


def test_authors_control_characters_dropped():
    """У PDF-метаданих трапляється NUL прямо посеред імені."""
    raw = json.dumps(["Кошева Т.М. Паспорт\x00"])
    assert export.parse_authors(raw) == ["Кошева Т.М. Паспорт"]


def test_authors_merged_reversed_order():
    """Злиття джерел дає ту саму людину прямою та реверсованою формою."""
    raw = json.dumps(["М.Н. Шалько", "Шалько М.Н.", "І.В. Ковальчук"])
    got = export.parse_authors(raw)
    assert len(got) == 2, "Шалько має бути один, а не два"
    assert set(got) == {"М.Н. Шалько", "І.В. Ковальчук"}


def test_authors_merged_transliteration_prefers_cyrillic():
    """Crossref дає «Khryapchenko», локальна транскрипція — «Хряпченко».

    Ініціали при цьому не збігаються («T. Y.» vs «Т.Й.»), тому вони
    з ключа виключені, а схожість визначається за прізвищем.
    """
    raw = json.dumps(["V. P. Khryapchenko", "Хряпченко В.П."])
    assert export.parse_authors(raw) == ["Хряпченко В.П."]


def test_authors_distinct_people_not_merged():
    raw = json.dumps(["Безула Е.М.", "Білоус Т.Й.", "Левченко О.О."])
    assert len(export.parse_authors(raw)) == 3


def test_authors_garbage_does_not_crash():
    assert export.parse_authors("не json") == []
    assert export.parse_authors(json.dumps([None, 42, ""])) == []


_TAIL_1 = (
    "УЧАСТЬ ОРГАНІВ ОПІКИ І ПІКЛУВАННЯ ПРИ РОЗГЛЯДІ СУДОМ СПОРІВ "
    "ПРО УЧАСТЬ ОДНОГО З БАТЬКІВ У СПІЛКУВАННІ ТА ВИХОВАННІ ДИТИНИ"
)
_TAIL_2 = "ДОГОВІР ПРО ПЕРЕРОЗПОДІЛ СПАДЩИНИ В СИСТЕМІ ЦИВІЛЬНО-ПРАВОВИХ ДОГОВОРІВ УКРАЇНИ"


@pytest.mark.parametrize(
    "raw,expected",
    [
        (_TAIL_1 + " Костюк Віктор Ігорович,", _TAIL_1),
        (_TAIL_2 + " Лукасевич-Крутник І.С.,", _TAIL_2),
    ],
)
def test_title_metadata_tail_removed(raw, expected):
    assert export.clean_title(raw, []) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # Звичайний реченний регістр — хвоста немає, нічого не ріжемо.
        "Виконання обов’язку батьків здійслювати виховання дитини",
        # Назва повністю голосна, змішаного хвоста немає.
        "МАЛОЛІТНЬОЇ ДИТИНИ ТА СУМІЖНІ ПРАВОВІ ПОНЯТТЯ",
        # Коротка назва: різати нічого, навіть якщо є змішаний хвіст.
        "ПРАВО ДИТИНИ Костюк Віктор",
    ],
)
def test_title_untouched(raw):
    assert export.clean_title(raw, []) == raw


def test_title_tail_matching_author_removed():
    authors = export.parse_authors(json.dumps(["Ш. Р. Тодуа"]))
    raw = "КРИТЕРІЇ СПІВВІДНОШЕННЯ ЗАБЕЗПЕЧЕННЯ НАЙКРАЩИХ ІНТЕРЕСІВ ДИТИНИ Ш. Р. Тодуа,"
    assert export.clean_title(raw, authors) == (
        "КРИТЕРІЇ СПІВВІДНОШЕННЯ ЗАБЕЗПЕЧЕННЯ НАЙКРАЩИХ ІНТЕРЕСІВ ДИТИНИ"
    )


def test_journal_prefers_container_title():
    extra = json.dumps({"container_title": "Правова думка"}, ensure_ascii=False)
    assert export.journal_from(extra, "https://example.org/a.pdf") == "Правова думка"


def test_journal_falls_back_to_host_not_doi_resolver():
    """doi.org — резолвер, а не назва видання."""
    out = export.journal_from(
        "{}", "https://doi.org/10.1234/x", "https://visnyk-pravo.uzhnu.edu.ua/a/1"
    )
    assert out == "visnyk-pravo.uzhnu.edu.ua"


def test_journal_empty_when_only_doi_resolver():
    assert export.journal_from("{}", "https://doi.org/10.1234/x") == ""