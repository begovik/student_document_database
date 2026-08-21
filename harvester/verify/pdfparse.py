import asyncio
from dataclasses import dataclass
from pathlib import Path

import structlog

logger = structlog.get_logger()


@dataclass
class PDFMetadata:
    title: str | None = None
    author: str | None = None
    subject: str | None = None
    creator: str | None = None
    producer: str | None = None
    creation_date: str | None = None
    modification_date: str | None = None


@dataclass
class PDFParseResult:
    page_count: int
    metadata: PDFMetadata
    text: str
    has_text_layer: bool
    is_encrypted: bool = False
    is_corrupt: bool = False
    error: str | None = None


async def parse_pdf(file_path: Path, max_pages: int = 3) -> PDFParseResult:
    try:
        result = await asyncio.to_thread(_parse_pdf_sync, file_path, max_pages)
        return result
    except Exception as e:
        logger.error("pdf_parse_error", path=str(file_path), error=str(e))
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error=str(e),
        )


def _parse_pdf_sync(file_path: Path, max_pages: int) -> PDFParseResult:
    try:
        import fitz
    except ImportError:
        logger.error("pymupdf_not_installed")
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error="PyMuPDF not installed",
        )

    try:
        doc = fitz.open(str(file_path))

        if doc.needs_pass:
            doc.close()
            return PDFParseResult(
                page_count=0,
                metadata=PDFMetadata(),
                text="",
                has_text_layer=False,
                is_encrypted=True,
            )

        page_count = len(doc)
        metadata_dict = doc.metadata or {}

        metadata = PDFMetadata(
            title=metadata_dict.get("title"),
            author=metadata_dict.get("author"),
            subject=metadata_dict.get("subject"),
            creator=metadata_dict.get("creator"),
            producer=metadata_dict.get("producer"),
            creation_date=metadata_dict.get("creationDate"),
            modification_date=metadata_dict.get("modDate"),
        )

        text_parts = []
        pages_to_read = min(page_count, max_pages)
        for page_num in range(pages_to_read):
            page = doc[page_num]
            text = page.get_text()
            if text:
                text_parts.append(text)

        doc.close()

        full_text = "\n".join(text_parts)
        has_text_layer = len(full_text.strip()) > 50

        return PDFParseResult(
            page_count=page_count,
            metadata=metadata,
            text=full_text,
            has_text_layer=has_text_layer,
        )

    except Exception as e:
        logger.error("pymupdf_parse_error", error=str(e))
        return PDFParseResult(
            page_count=0,
            metadata=PDFMetadata(),
            text="",
            has_text_layer=False,
            is_corrupt=True,
            error=str(e),
        )


def extract_title_from_text(text: str) -> str | None:
    """Витягує заголовок статті з перших рядків тексту PDF.

    Евристика: заголовок зазвичай йде після заголовка журналу (ISSN, DOI,
    УДК, номер сторінки) і перед авторами. Часто набраний ВЕЛИКИМИ літерами
    або Title Case.
    """
    import re

    if not text:
        return None

    lines = text.split("\n")
    n = len(lines)

    skip_patterns = [
        r"^\s*$",
        r"^\s*\d+\s*$",
        r"ISSN",
        r"DOI\s*:",
        r"https?://",
        r"УДК\s*[\d]",
        r"UDC\s*[\d]",
        r"OPEN\s+ACCESS",
        r"^\s*©",
        r"Доступно",
        r"Рецензент",
        r"Редакційна\s+колегія",
        r"Журнал",
        r"науковий\s+журнал",
        r"Електронне\s+видання",
        r"Online\s+issue",
        r"Print\s+issue",
        r"Vol\.",
        r"Вип\.",
        r"Випуск",
        r"том",
        r"^\s*No\s*\d|^\s*№\s*\d",
        r"^\s*pp\.\s*\d|^\s*с\.\s*\d",
        r"journal\s+homepage",
        r"editorial\s+e-mail",
        r"^\s*\d{4}\s*$",
        r"JEL\s",
        r"p?ISSN",
        r"series",
        r"серія",
        r"фінансово",
        r"CC\s+BY",
        r"\(CC",
        r"\d+-\d+",
        r"ГОСТ",
        r"РЕФЕРАТ",
        r"АБСТРАКТ",
        r"КЛЮЧЕВІ\s+СЛОВА",
        r"КЛЮЧЕВЫЕ\s+СЛОВА",
        r"REFERENCES",
        r"Література",
        r"Literature",
        r"Вступ",
        r"Introduction",
        r"ВИСНОВКИ",
        r"CONCLUSIONS",
        r"ВИСНОВОК",
        r"РЕЗЮМЕ",
        r"REZUME",
        r"ВИЗНАЧЕННЯ",
        r"ВИЗНАЧЕНИЕ",
        r"МЕТОДИКА",
        r"МЕТОДИ",
        r"ДИСКУСІЯ",
        r"ДИСКУССІЯ",
        r"ЗВ'ЯЗОК",
        r"ЗВЯЗОК",
        r"СЕКЦІЯ",
        r"СЕКЦЯ",
        r"ТЕЗИ",
        r"ДОКЛАД",
        r"ДОКУМЕНТ",
        r"ТЕРМІНИ",
        r"ВИЗНАЧЕННЯ",
        r"УМОВИ\s+ВИКОНАННЯ",
        r"ЗАВДАННЯ",
        r"ВИМОГИ",
        r"ОСНОВНІ",
        r"ОСНОВНЕ",
    ]
    skip_re = [re.compile(p, re.IGNORECASE) for p in skip_patterns]

    journal_patterns = [
        r"journal\b", r"записки\b", r"вісник\b", r"вістник\b", r"наукові\s+праці",
        r"scientific\s+notes", r"proceedings", r"transactions",
        r"herald", r"bulletin", r"review", r"annals",
        r"interdisciplinary\s+studies", r"complex\s+systems",
        r"chemistry\s+and\s+technologies",
        r"економіка\s+та\s+суспільство", r"економічний\s+часопис",
        r"financial.*credit.*activity", r"фінансово.*кредитна",
        r"stateuniversity", r"visnyk", r"horizons",
        r"східноєвропейський", r"вісник\s+напн",
        r"науковий\s+часопис", r"науково-практичний\s+журнал",
        r"журнал\s+англійською", r"журнал\s+українською",
        r"міжнародний\s+науковий\s+журнал",
    ]
    journal_re = [re.compile(p, re.IGNORECASE) for p in journal_patterns]

    author_patterns = [
        r"кандидат\s", r"доктор\s", r"професор\s", r"доцент\s",
        r"ORCID", r"orcid\.org", r"@.*\.", r"старший\s+викладач",
        r"асистент\s", r"викладач\s", r"завідувач\s",
        r"PhD\s", r"Ph\.D\.", r"Dr\.\s", r"Prof\.\s",
        r"^([А-ЯІЇЄҐA-Z][а-яіїєґa-z]+\s+[А-ЯІЇЄҐA-Z]\.)",
        r"^[A-Z][a-z]+\s+[A-Z]\.",
        r"Валерія\s+Міляєва", r"Світлана\s+Калашнікова", r"Вікторія\s+Шаповалова",
        r"[A-Z][a-z]+\d{1,2}",  # Savenkova1, Test12 (uppercase start name + digits)
        r"[А-ЯІЇЄҐ][а-яіїєґ]+\d{1,2}",  # Ukrainian names with digits
        r",\s*[A-Z][a-z]+\d{1,2}",  # , Savenkova1 (comma + name + digits)
        r"^\s*\d+\s*$",  # page number on its own line
    ]
    author_re = [re.compile(p, re.IGNORECASE) for p in author_patterns]

    key_word_patterns = [
        r"КЛЮЧЕВІ\s+СЛОВА",
        r"КЛЮЧЕВЫЕ\s+СЛОВА",
        r"КЛЮЧІВІ\s+СЛОВА",
        r"Референcing",
        r"REFERENCES",
        r"Література",
        r"Літeратура",
        r"Літературe",
        r"Literature",
        r"Анотація",
        r"ABSTRACT",
        r"Annotation",
        r"Вступ",
        r"Introduction",
        r"ЗАГАЛЬШЕ",
        r"Нарешті",
        r"ВИСНОВКИ",
        r"CONCLUSIONS",
        r"ВИСНОВОК",
        r"РЕЗЮМЕ",
        r"REZUME",
        r"ВИЗНАЧЕННЯ",
        r"ВИЗНАЧЕНИЕ",
        r"МЕТОДИКА",
        r"МЕТОДИ",
        r"ДИСКУСІЯ",
        r"ДИСКУССІЯ",
        r"ЗВ'ЯЗОК",
        r"ЗВЯЗОК",
        r"СЕКЦІЯ",
        r"СЕКЦЯ",
        r"ТЕЗИ",
        r"ДОКЛАД",
        r"ДОКУМЕНТ",
        r"ТЕРМІНИ",
        r"ВИЗНАЧЕННЯ",
        r"УМОВИ\s+ВИКОНАННЯ",
        r"ЗАВДАННЯ",
        r"ВИМОГИ",
        r"ОСНОВНІ",
        r"ОСНОВНЕ",
    ]
    key_word_re = [re.compile(p, re.IGNORECASE) for p in key_word_patterns]

    title_lines: list[str] = []
    found_start = False
    i = 0

    while i < n:
        line = lines[i]
        stripped = line.strip()
        i += 1

        if not stripped:
            if found_start and title_lines:
                break
            continue

        if any(p.search(stripped) for p in skip_re):
            if found_start and title_lines:
                break
            continue

        if any(p.search(stripped) for p in author_re):
            if found_start and title_lines:
                break
            continue

        if any(p.search(stripped) for p in key_word_re):
            if found_start and title_lines:
                break
            continue

        if any(p.search(stripped) for p in journal_re):
            continue

        upper_count = sum(1 for c in stripped if c.isupper() and c.isalpha())
        alpha_count = sum(1 for c in stripped if c.isalpha())
        upper_ratio = upper_count / max(alpha_count, 1)
        is_mostly_upper = upper_ratio > 0.5 and alpha_count > 3
        is_title_case = stripped[0].isupper() if stripped else False
        has_min_length = len(stripped) > 8

        if not has_min_length:
            if found_start and title_lines:
                break
            continue

        if is_mostly_upper:
            found_start = True
            title_lines.append(stripped)
        elif is_title_case and not found_start:
            found_start = True
            title_lines.append(stripped)
        elif found_start:
            if any(p.search(stripped) for p in skip_re):
                break
            if any(p.search(stripped) for p in author_re):
                break
            if any(p.search(stripped) for p in key_word_re):
                break
            title_lines.append(stripped)

    if not title_lines:
        return None

    title = " ".join(title_lines)
    title = re.sub(r"\s+", " ", title).strip()

    if len(title) < 10 or len(title) > 500:
        return None

    return title


def extract_udc_from_text(text: str) -> str | None:
    import re

    udc_pattern = r"УДК\s*([\d.():+\-]+)"
    match = re.search(udc_pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    udc_pattern_en = r"UDC\s*([\d.():+\-]+)"
    match = re.search(udc_pattern_en, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None
