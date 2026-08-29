"""Модуль вилучення бібліографічних посилань з PDF-документів."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class BibliographyEntry:
    """Один запис у списку літератури."""
    raw_text: str
    authors: list[str] = field(default_factory=list)
    title: str = ""
    year: str = ""
    source: str = ""  # журнал, видавництво, URL
    pages: str = ""
    doi: str = ""
    url: str = ""
    language: str = ""
    entry_type: str = "unknown"  # article, book, thesis, online, conference
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_text": self.raw_text,
            "authors": self.authors,
            "title": self.title,
            "year": self.year,
            "source": self.source,
            "pages": self.pages,
            "doi": self.doi,
            "url": self.url,
            "language": self.language,
            "entry_type": self.entry_type,
        }


# Патерни для розпізнавання записів літератури
REFERENCE_START_PATTERNS = [
    # Нумеровані посилання: [1], [2], 1., 2.
    r"^\s*\[\d+\]",
    r"^\s*\d+\.\s",
    # Буквенні посилання: a), b), а), б)
    r"^\s*[a-яіїєґ]\)\s",
    # Пряме посилання на автора (починається з великого або латиниці)
    r"^\s*[A-ZА-ЯІЇЄҐ][a-zа-яіїєґ]+\s+[A-ZА-ЯІЇЄҐ]",
]

# Патерни для визначення типу запису
BOOK_PATTERNS = [
    r":\s*(?:Вид-во|Видавництво|Издательство|Publishing|Press)",
    r"(?:навч\.\s*(?:посібник|підручник))",
    r"(?:підручник|посібник|монографія)",
    r"\d+\s*с\.",
    r"(?:К\.)\s*[:\"]",
]

ARTICLE_PATTERNS = [
    r"(?:журнал|часопис|збірник|вісник)",
    r"(?:Journal|Magazine|Review|Bulletin)",
    r"Vol\.\s*\d+",
    r"№\s*\d+",
    r"pp\.\s*\d+",
    r"с\.\s*\d+",
]

THESIS_PATTERNS = [
    r"(?:дисертація|дисертаційна|thesis)",
    r"(?:автореферат|дисертації)",
    r"(?:магістерська|бакалаврська|кандидатська)",
]

CONFERENCE_PATTERNS = [
    r"(?:конференці|конференц|conference|congress)",
    r"(?:тези|theses| materials)",
    r"(?:міжнародна|всеукраїнська|всеросійська)",
]

ONLINE_PATTERNS = [
    r"https?://",
    r"www\.",
    r"(?:онлайн|online|електронний|электронный)",
    r"(?:доступно|доступний|доступно на|доступен по)",
]


def parse_reference_entry(text: str) -> BibliographyEntry | None:
    """Розібрати один запис у списку літератури."""
    text = text.strip()
    if not text or len(text) < 10:
        return None
    
    entry = BibliographyEntry(raw_text=text)
    
    # Визначення URL
    url_match = re.search(r"https?://[^\s,;]+", text)
    if url_match:
        entry.url = url_match.group(0).rstrip(".")
        entry.entry_type = "online"
    
    # Визначення DOI
    doi_match = re.search(r"(?:doi:|DOI:)\s*(10\.\d{4,}/[^\s,;]+)", text)
    if doi_match:
        entry.doi = doi_match.group(1)
    
    # Визначення року
    year_match = re.search(r"[—–-]\s*(?:19|20)\d{2}\b", text)
    if year_match:
        entry.year = year_match.group(0).strip("—–- ")
    else:
        year_match = re.search(r"\b(?:19|20)\d{2}\b", text)
        if year_match:
            entry.year = year_match.group(0)
    
    # Визначення сторінок
    pages_match = re.search(r"(?:pp?\.|с\.)\s*(\d+[-–]\d+)", text)
    if pages_match:
        entry.pages = pages_match.group(1).replace("–", "-")
    
    # Визначення мови
    if re.search(r"[а-яіїєґА-ЯІЇЄҐ]{3,}", text):
        entry.language = "uk"
    elif re.search(r"[a-zA-Z]{10,}", text):
        entry.language = "en"
    
    # Визначення типу
    if any(re.search(p, text, re.IGNORECASE) for p in BOOK_PATTERNS):
        entry.entry_type = "book"
    elif any(re.search(p, text, re.IGNORECASE) for p in ARTICLE_PATTERNS):
        entry.entry_type = "article"
    elif any(re.search(p, text, re.IGNORECASE) for p in THESIS_PATTERNS):
        entry.entry_type = "thesis"
    elif any(re.search(p, text, re.IGNORECASE) for p in CONFERENCE_PATTERNS):
        entry.entry_type = "conference"
    elif entry.url:
        entry.entry_type = "online"
    
    # Витяг авторів (спрощений)
    # Шукаємо патерн: Прізвище І.Б. або Прізвище І.Б., Прізвище І.Б.
    author_matches = re.findall(
        r"([А-ЯІЇЄҐ][а-яіїєґ]+)\s+([А-ЯІЇЄҐ])\.\s*([А-ЯІЇЄҐ])?\.",
        text
    )
    if author_matches:
        entry.authors = [f"{m[0]} {m[1]}. {m[2]}." if m[2] else f"{m[0]} {m[1]}." for m in author_matches]
    
    # Якщо не знайшли українських авторів — шукаємо латинських
    if not entry.authors:
        author_matches_en = re.findall(
            r"([A-Z][a-z]+)\s+([A-Z])\.\s*([A-Z])?\.",
            text
        )
        if author_matches_en:
            entry.authors = [f"{m[0]} {m[1]}. {m[2]}." if m[2] else f"{m[0]} {m[1]}." for m in author_matches_en]
    
    # Витяг назви (спрощений)
    # Назва зазвичай після авторів і перед джерелом
    title_match = re.search(
        r"[—–-]\s*[«\"]?(.{10,100})[»\"]?\s*[—–-]",
        text
    )
    if title_match:
        entry.title = title_match.group(1).strip()
    
    # Витяг джерела (журнал, видавництво)
    source_match = re.search(
        r"(?:Журнал|Journal|Вид-во|Издательство| Publishing|Press)[^.]*",
        text,
        re.IGNORECASE
    )
    if source_match:
        entry.source = source_match.group(0).strip()
    
    return entry


def extract_references_from_text(text: str) -> list[BibliographyEntry]:
    """Витягти всі посилання на літературу з тексту."""
    references = []
    
    # Шукаємо розділ "Література" або "References"
    ref_section_patterns = [
        r"(?:ЛІТЕРАТУРА|ЛІТЕРАТУРА:|REFERENCES|REFERENCES:|BIBLIOGRAPHY|СПИСОК ВИКОРИСТАНИХ ДЖЕРЕЛ)",
    ]
    
    # Знаходимо початок розділу літератури
    ref_start = -1
    for pattern in ref_section_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            ref_start = match.end()
            break
    
    if ref_start == -1:
        # Якщо не знайшли розділ — шукаємо по патернах записів
        lines = text.split("\n")
        current_ref = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                if current_ref:
                    entry = parse_reference_entry(current_ref)
                    if entry and (entry.authors or entry.title):
                        references.append(entry)
                    current_ref = ""
                continue
            
            # Перевіряємо чи це початок нового запису
            is_new_ref = any(re.search(p, line) for p in REFERENCE_START_PATTERNS)
            
            if is_new_ref:
                if current_ref:
                    entry = parse_reference_entry(current_ref)
                    if entry and (entry.authors or entry.title):
                        references.append(entry)
                current_ref = line
            else:
                current_ref += " " + line
    else:
        # Обробляємо розділ літератури
        ref_text = text[ref_start:]
        lines = ref_text.split("\n")
        current_ref = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                if current_ref:
                    entry = parse_reference_entry(current_ref)
                    if entry and (entry.authors or entry.title):
                        references.append(entry)
                    current_ref = ""
                continue
            
            # Перевіряємо чи це початок нового запису
            is_new_ref = any(re.search(p, line) for p in REFERENCE_START_PATTERNS)
            
            if is_new_ref:
                if current_ref:
                    entry = parse_reference_entry(current_ref)
                    if entry and (entry.authors or entry.title):
                        references.append(entry)
                current_ref = line
            else:
                current_ref += " " + line
        
        # Останній запис
        if current_ref:
            entry = parse_reference_entry(current_ref)
            if entry and (entry.authors or entry.title):
                references.append(entry)
    
    return references


def deduplicate_references(references: list[BibliographyEntry]) -> list[BibliographyEntry]:
    """Дедуплікація записів літератури."""
    seen = set()
    unique = []
    
    for ref in references:
        # Створюємо ключ для дедуплікації
        key = (
            tuple(sorted(ref.authors[:2])) if ref.authors else (),
            ref.title.lower().strip()[:50] if ref.title else "",
            ref.year,
        )
        
        if key not in seen:
            seen.add(key)
            unique.append(ref)
    
    return unique


RUSSIAN_TLDS = (".ru", ".su", ".рф", ".xn--p1ai")
RUSSIAN_PUBLISHERS_HINTS = [
    "издательство",
    "наука",
    "высшая школа",
    "просвещение",
    "москва",
    "санкт-петербург",
    "россия",
    "российский",
    "ленинград",
]
SOVIET_PUBLISHERS_RU_HINTS = [
    "наука",
    "мысль",
    "высшая школа",
    "просвещение",
    "мир",
    "политиздат",
]


def is_russian_entry(entry: BibliographyEntry) -> tuple[bool, str | None]:
    """Перевірити чи запис літератури є російським/радянським і підлягає фільтрації."""
    text = f"{entry.raw_text} {entry.source} {entry.url}".lower()

    # 1. Перевірка TLD у URL
    if entry.url:
        try:
            import tldextract

            ext = tldextract.extract(entry.url)
            suffix = f".{ext.suffix}".lower() if ext.suffix else ""
            domain = f"{ext.domain}.{ext.suffix}".lower() if ext.domain and ext.suffix else entry.url.lower()
            if suffix in RUSSIAN_TLDS or domain.endswith((".ru", ".su")) or ".рф" in entry.url.lower():
                return True, f"російський домен TLD {suffix or domain}"
        except Exception:  # noqa: BLE001
            if any(tld in entry.url.lower() for tld in RUSSIAN_TLDS):
                return True, "російський домен у URL"

    # 2. Перевірка видавництв/міст СРСР у джерелі
    for hint in RUSSIAN_PUBLISHERS_HINTS:
        if hint in text and (
            re.search(r"\b(19[0-8]\d|1990)\b", entry.raw_text) or "издательство" in text
        ):
            return True, f"радянське/російське видавництво '{hint}'"

    # 3. Мовна перевірка: кирилиця без українських літер + наявність російських літер
    # Українські специфічні: і, ї, є, ґ
    has_ukr = any(c in entry.raw_text for c in "іїєґІЇЄҐ")
    has_ru_specific = any(c in entry.raw_text for c in "ыэъёЫЭЪЁ")
    # Якщо є кирилиця, немає українських, є російські специфічні - ймовірно російська
    if re.search(r"[а-яА-Я]{10,}", entry.raw_text) and not has_ukr and has_ru_specific:
        return True, "російська мова (відсутні іїєґ, є ыэъё)"

    # 4. Евристика: текст російською якщо містить "Издательство" або "г. Москва"
    if re.search(r"\bИздательство\b|\bг\.\s*Москва\b|\bг\.\s*СПб\b", entry.raw_text):
        return True, "російське видавництво/місто у бібліографії"

    return False, None


def filter_russian_entries(
    references: list[BibliographyEntry],
) -> tuple[list[BibliographyEntry], list[tuple[BibliographyEntry, str]]]:
    """Відфільтрувати російські джерела. Повертає (залишені, відфільтровані з причиною)."""
    kept: list[BibliographyEntry] = []
    filtered: list[tuple[BibliographyEntry, str]] = []
    for entry in references:
        is_ru, reason = is_russian_entry(entry)
        if is_ru:
            filtered.append((entry, reason or "російське джерело"))
        else:
            kept.append(entry)
    return kept, filtered


def format_references_list(references: list[BibliographyEntry]) -> str:
    """Сформатувати список літератури у текстовий вигляд."""
    lines = []
    for i, ref in enumerate(references, 1):
        parts = []
        
        # Автори
        if ref.authors:
            parts.append(", ".join(ref.authors))
        
        # Назва
        if ref.title:
            parts.append(f"// {ref.title}")
        
        # Джерело
        if ref.source:
            parts.append(ref.source)
        
        # Рік
        if ref.year:
            parts.append(ref.year)
        
        # Сторінки
        if ref.pages:
            parts.append(f"с. {ref.pages}")
        
        # URL
        if ref.url:
            parts.append(f"[Електронний ресурс]. URL: {ref.url}")
        
        # DOI
        if ref.doi:
            parts.append(f"DOI: {ref.doi}")
        
        line = f"{i}. {' '.join(parts)}"
        lines.append(line)
    
    return "\n\n".join(lines)
