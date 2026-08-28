#!/usr/bin/env python3
"""
Якісний аналіз PDF документів — визначення типу, структури, якості.
Покращена версія: виправлено Greek→українські, додано типи документів.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PDFQualityResult:
    """Результат якісного аналізу PDF."""
    page_count: int = 0
    total_text_chars: int = 0
    chars_per_page: float = 0.0
    num_images: int = 0
    has_text_layer: bool = False
    
    title_from_meta: str = ""
    author_from_meta: str = ""
    producer: str = ""
    subject_from_meta: str = ""
    
    doc_type: str = "unknown"
    confidence: float = 0.0
    
    quality_score: float = 0.0
    quality_label: str = "unknown"
    
    has_toc: bool = False
    has_references: bool = False
    has_abstract: bool = False
    has_keywords_section: bool = False
    has_sections: int = 0
    has_conclusion: bool = False
    has_authors_list: bool = False
    has_titled_page: bool = False
    has_udc: bool = False
    
    notes: list[str] = field(default_factory=list)
    rejected_reasons: list[str] = field(default_factory=list)
    
    text_density: str = "unknown"
    page_count_category: str = "unknown"


def _parse_pdf(pdf_path: Path) -> dict:
    """Отримати базові дані про PDF."""
    try:
        import fitz
    except ImportError:
        return {"error": "pymupdf не встановлено"}
    
    try:
        doc = fitz.open(str(pdf_path))
        page_count = len(doc)
        
        total_text = ""
        img_count = 0
        for page in doc:
            total_text += page.get_text()
            img_count += len(page.get_images())
        
        doc.close()
        
        has_text_layer = len(total_text.strip()) > 100
        
        meta = doc.metadata or {}
        producer = meta.get("producer", "")
        title = meta.get("title", "")
        author = meta.get("author", "")
        subject = meta.get("subject", "")
        
        return {
            "page_count": page_count,
            "total_text": total_text,
            "total_text_chars": len(total_text),
            "chars_per_page": len(total_text) / max(page_count, 1),
            "num_images": img_count,
            "has_text_layer": has_text_layer,
            "title_from_meta": title or "",
            "author_from_meta": author or "",
            "producer": producer,
            "subject_from_meta": subject or "",
        }
    except Exception as e:
        return {"error": str(e)}


def _check_section_presence(text: str) -> dict:
    """Перевірити наявність структурних розділів."""
    text_upper = text.upper()
    
    # ВИПРАВЛЕНО: замінив Greek letters на Ukrainian patterns
    result = {
        "has_toc": bool(
            re.search(
                r"ЗМІСТ|ОГЛАВЛ|ОГЛАВЛЕН|СОДЕРЖ|СОДЕРЖАН",
                text_upper,
            )
        ),
        "has_references": bool(
            re.search(
                r"ЛІТЕРАТУР|ЛИТЕРАТУРА|БІБЛІОГРАФ|БІБЛІОГРІФ|REFERENCES|REF\.|LITERATURE",
                text_upper,
            )
        ),
        "has_abstract": bool(
            re.search(
                r"АБСТРАКТ|РЕФЕРАТ|РЕЗЮМЕ|АННОТ|АБСТ|ABSTRACT|RESUM|ANNOT",
                text_upper,
            )
        ),
        "has_keywords_section": bool(
            re.search(
                r"КЛЮЧЕВІ\s*СЛОВА|КЛЮЧЕВЫЕ\s*СЛОВА|КЛЮЧ.*СЛОВ|KEY\s*WORDS|KEYWORDS",
                text_upper,
            )
        ),
        "has_conclusion": bool(
            re.search(
                r"ВИСНОВК|ВИСНОВОК|ВЫВОД|CONCLUSIONS?|ЗАКЛЮЧ|РЕЗУЛЬТАТ",
                text_upper,
            )
        ),
        "has_authors_list": bool(
            re.search(
                r"автор|author|кандидат\s+наук|доктор\s+наук|наук(\s*)?.керівник|керівник|реценз",
                text_upper,
            )
        ),
        "has_titled_page": False,
        "keywords_found": [],
    }
    
    # Пошук ключових заголовків розділів (українські патерни)
    section_headers = [
        "Вступ|ВВЕДЕННИЯ|ВВЕДЕНИЕ",
        r"1\.\s+[А-ЯІЇЄҐA-Z]",
        r"[А-ЯІЇЄҐ][А-ЯІЇЄҐ]+\s+[А-ЯІЇЄҐ][А-ЯІЇЄҐ]+",
        r"Розділ\s*\d|РОЗДІЛ",
        r"Глава\s*\d|ГЛАВ",
    ]
    for pattern in section_headers:
        result["keywords_found"].extend(re.findall(pattern, text[:50000], re.IGNORECASE | re.UNICODE))
    
    result["num_section_indicators"] = len(result["keywords_found"])
    
    return result


def _detect_document_type(
    page_count: int,
    chars_per_page: float,
    num_images: int,
    total_text_chars: int,
    has_toc: bool,
    has_references: bool,
    has_abstract: bool,
    has_keywords_section: bool,
    has_conclusion: bool,
    has_authors_list: bool,
    has_udc: bool,
    producer: str,
    title_meta: str,
    text: str,
) -> tuple[str, float, list[str]]:
    """Визначити тип документа та впевненість."""
    notes = []
    confidence = 0.5
    
    text_upper = text.upper()
    is_ppt = "POWERPOINT" in producer.upper() if producer else False
    
    # ВИПРАВЛЕНО: детекція презентацій українською мовою
    is_slides = bool(
        re.search(
            r"СРЕДА|ПРОБОЛ|ПРОБОЛ|ЕЛЕНХОС|АНАКОІНОСІС|ПАРОУСІАСІ|СРЄД|ПРОВОЛ|СРІАК|СРІДА",
            text_upper,
        )
    )
    
    # ВИПРАВЛЕНО: детекція PowerPoint за producer
    if is_ppt and page_count >= 5:
        notes.append("Презентація PowerPoint — низький пріоритет")
        return "presentation", 0.8, notes
    
    # ВИПРАВЛЕНО: детекція презентацій за структурою
    if is_slides and page_count >= 5:
        notes.append("Презентація (слайди) — низький пріоритет")
        return "presentation", 0.7, notes
    
    # ВИПРАВЛЕНО: правило для 2-сторінкових статей з повною структурою
    if page_count <= 2 and has_udc and has_toc and has_references and has_conclusion:
        notes.append("Коротка стаття з повною структурою (УДК, зміст, література, висновки) — прийнятна")
        return "scientific_article", 0.6, notes
    
    if page_count <= 2 and has_references and has_conclusion and has_udc:
        notes.append("2-сторінкова стаття з науковою структурою — прийнятна")
        return "scientific_article", 0.5, notes
    
    if page_count <= 2 and has_references and has_conclusion and chars_per_page >= 800:
        notes.append("Коротка стаття з літературою та висновками — прийнятна")
        return "short_article", 0.4, notes
    
    # ВИПРАВЛЕНО: уривки — однієї сторінки з мало тексту
    if page_count == 1 and chars_per_page < 800:
        notes.append("Одна сторінка, мала щільність — уривок")
        return "fragment", 0.8, notes
    
    if page_count == 1 and total_text_chars < 1500:
        notes.append("Одна сторінка, дуже мало тексту — уривок")
        return "fragment", 0.8, notes
    
    # ВИПРАВЛЕНО: детекція навчальних програм (українські патерни)
    is_pedagogical = bool(
        re.search(
            r"навчальна\s*програма|РОБОЧА\s*ПРОГРАМА|РОБОЧАЯ\s*ПРОГРАММА|СИЛАБУС|силабус|ДИСЦИПЛІНА|дисциплін|ДИСЦИПЛИНА|НАВЧАЛЬНИЙ\s*ПЛАН|НАВЧАЛЬНИЙ\s*КУРС|НАВЧАЛЬНА\s*ПРОГРАММА",
            text_upper,
        )
    )
    if is_pedagogical:
        notes.append("Навчально-педагогічний документ (програма, силабус)")
        return "pedagogical", 0.6, notes
    
    # ВИПРАВЛЕНО: детекція пояснювальних записок / дипломів
    is_student_thesis = bool(
        re.search(
            r"пояснювальна\s*записка|дипломна\s*робота|магістерська\s*робота|бакалаврська\s*робота|науковий\s*керівник|науковий\s*керівник|керівник|магістерського|бакалаврського",
            text_upper,
        )
    )
    if is_student_thesis and page_count >= 10:
        notes.append("Дипломна/навчальна робота — нижчий пріоритет")
        return "thesis", 0.5, notes
    
    # ВИПРАВЛЕНО: детекція практичних/методичних документів
    is_practical = bool(
        re.search(
            r"методична\s*розробка|практична\s*робота|лабораторна\s*робота|розкрій|крій|закрій|конструювання\s*одягу|пошиття|шв",
            text_upper,
        )
    )
    if is_practical:
        notes.append("Практичний/методичний документ")
        return "practical_guide", 0.5, notes
    
    # ВИПРАВЛЕНО: детекція технічних звітів
    is_technical = bool(
        re.search(
            r"технічний\s*звіт|звіт\s*про|технічний\s*документ|проект|дослідження|експеримент|технічний\s*опис",
            text_upper,
        )
    )
    if is_technical and page_count >= 4:
        notes.append("Технічний звіт/дослідження — прийнятний")
        return "technical_report", 0.4, notes
    
    # ВИПРАВЛЕНО: детекція есе (короткий текст без літератури)
    if page_count <= 4 and not has_references:
        notes.append("Короткий текст без літератури — есе")
        return "essay", 0.4, notes
    
    # ВИПРАВЛЕНО: детекція статті за типовою структурою
    if has_references or has_conclusion or has_abstract:
        notes.append("Має ознаки статті/звіту")
        confidence = 0.5
        return "scientific_article", confidence, notes
    
    notes.append("Тип не зрозумів")
    return "unknown", 0.1, notes


def analyze_pdf_quality(pdf_path: Path) -> PDFQualityResult:
    """Основна функція — проаналізувати PDF на якість."""
    pdf_path = Path(pdf_path)
    raw = _parse_pdf(pdf_path)
    
    if raw.get("error"):
        return PDFQualityResult(
            quality_label="error",
            notes=[f"Помилка: {raw['error']}"],
            rejected_reasons=[raw["error"]],
        )
    
    page_count = raw["page_count"]
    total_text = raw["total_text"]
    total_text_chars = raw["total_text_chars"]
    chars_per_page = raw["chars_per_page"]
    num_images = raw["num_images"]
    has_text_layer = raw["has_text_layer"]
    producer = raw["producer"]
    title_meta = raw["title_from_meta"]
    author_meta = raw["author_from_meta"]
    subject_meta = raw["subject_from_meta"]
    
    result = PDFQualityResult()
    result.page_count = page_count
    result.total_text_chars = total_text_chars
    result.chars_per_page = chars_per_page
    result.num_images = num_images
    result.has_text_layer = has_text_layer
    result.title_from_meta = title_meta
    result.author_from_meta = author_meta
    result.producer = producer
    result.subject_from_meta = subject_meta
    
    # Категорії сторінок
    if page_count <= 1:
        result.page_count_category = "single_page"
    elif page_count <= 3:
        result.page_count_category = "few_pages"
    elif page_count <= 8:
        result.page_count_category = "dozen"
    elif page_count <= 20:
        result.page_count_category = "many"
    else:
        result.page_count_category = "very_many"
    
    # Щільність тексту
    if chars_per_page < 300:
        result.text_density = "sparse"
    elif chars_per_page < 1000:
        result.text_density = "normal"
    elif chars_per_page < 2000:
        result.text_density = "dense"
    else:
        result.text_density = "very_dense"
    
    # Перевірка секцій
    section_info = _check_section_presence(total_text)
    result.has_toc = section_info["has_toc"]
    result.has_references = section_info["has_references"]
    result.has_abstract = section_info["has_abstract"]
    result.has_keywords_section = section_info["has_keywords_section"]
    result.has_conclusion = section_info["has_conclusion"]
    result.has_authors_list = section_info["has_authors_list"]
    result.has_sections = section_info["num_section_indicators"] // 2
    
    # Перевірка УДК
    result.has_udc = bool(re.search(r"УДК\s*\d|UDC\s*\d|УДК:|UDC:", total_text[:5000], re.IGNORECASE | re.UNICODE))
    
    # Перевірка титульного аркуша
    first_300 = total_text[:300]
    result.has_titled_page = bool(
        re.search(r"УНІВЕРСИТЕТ|УНИВЕРСИТЕТ|НАЦІОНАЛЬНИЙ|МІНІСТЕРСТВО|НАЦІОНАЛЬНИЙ", 
                  first_300, re.IGNORECASE | re.UNICODE)
    ) or (len(first_300.strip()) > 50 and len(first_300.split()) > 3)
    
    # Визначення типу документа
    doc_type, confidence, type_notes = _detect_document_type(
        page_count,
        chars_per_page,
        num_images,
        total_text_chars,
        result.has_toc,
        result.has_references,
        result.has_abstract,
        result.has_keywords_section,
        result.has_conclusion,
        result.has_authors_list,
        result.has_udc,
        producer,
        title_meta,
        total_text,
    )
    result.doc_type = doc_type
    result.confidence = confidence
    result.notes.extend(type_notes)
    
    # Розрахунок якості
    quality = 0.0
    reasons = []
    
    # Кількість сторінок
    if page_count >= 10:
        quality += 0.20
        reasons.append("Багато сторінок — цілісність вища")
    elif page_count >= 5:
        quality += 0.10
        reasons.append("Достатньо сторінок")
    elif page_count >= 2:
        quality += 0.05
        reasons.append("Дві сторінки — мінімум для статті")
    else:
        quality -= 0.10
        reasons.append("Дуже мало сторінок")
    
    # Щільність тексту
    if chars_per_page >= 2000:
        quality += 0.15
        reasons.append("Висока щільність тексту — багато інформації")
    elif chars_per_page >= 1000:
        quality += 0.08
        reasons.append("Нормальна щільність тексту")
    elif chars_per_page >= 500:
        quality += 0.05
        reasons.append("Середня щільність")
    elif chars_per_page >= 200:
        quality += 0.02
        reasons.append("Низька щільність тексту")
    else:
        quality -= 0.10
        reasons.append("Дуже низька щільність — можливо фрагмент")
    
    # Текстовий шар
    if has_text_layer:
        quality += 0.10
        reasons.append("Є текстовий шар")
    else:
        quality -= 0.10
        reasons.append("Немає текстового шару — PDF складно читабельний")
    
    # Структура документа (для наукових/технічних/педагогічних типів)
    if doc_type in ("scientific_article", "technical_report", "pedagogical", "thesis", "short_article"):
        if result.has_toc:
            quality += 0.03
            reasons.append("Є зміст — добра структура")
        if result.has_references:
            quality += 0.10
            reasons.append("Є література — належний науковий стандарт")
        if result.has_conclusion:
            quality += 0.08
            reasons.append("Є висновки — завершеність")
        if result.has_titled_page:
            quality += 0.03
            reasons.append("Є титульний аркуш")
        if result.has_abstract:
            quality += 0.03
            reasons.append("Є анотація/реферат")
        if result.has_keywords_section:
            quality += 0.02
            reasons.append("Є ключові слова")
        if result.has_authors_list:
            quality += 0.02
            reasons.append("Є список авторів")
        if result.has_udc:
            quality += 0.02
            reasons.append("Є УДК — науковий стандарт")
    
    # Налаштування для презентацій
    if doc_type == "presentation":
        quality -= 0.20
        reasons.append("Презентація — низький пріоритет для бібліотеки")
    
    # Налаштування для есе
    elif doc_type == "essay":
        quality -= 0.10
        reasons.append("Есе/не-науковий — нижчий пріоритет")
    
    # Налаштування для невідомих типів
    elif doc_type == "unknown":
        quality -= 0.05
        reasons.append("Тип невідомий — ризик серйозного заваду")
    
    # Налаштування для фрагментів
    elif doc_type == "fragment":
        quality -= 0.20
        reasons.append("Фрагмент — непридатний для колекції")
    
    # Тематична прив'язка
    topic_keywords = [
        "пальто", "швейн", "конструювання одягу", "технологія швейного",
        "моделювання одягу", "текстиль", "швейне виробництво", "легка промисловість",
        "розкрій", "крій", "шиття", "пошиття", "одяг", "оверсайз", "силует",
        "конструювання", "моделювання", "технологічний процес",
    ]
    
    is_relevant = any(
        kw.lower() in subject_meta.lower() or
        kw.lower() in title_meta.lower() or
        kw.lower() in total_text[:2000].lower()
        for kw in topic_keywords
    )
    
    if is_relevant:
        quality += 0.05
        reasons.append("Тематична прив'язка до цілі — відповідність колекції")
    
    # Нормалізація
    quality = max(-0.5, min(1.0, quality))
    result.quality_score = round(quality, 2)
    
    # Мітки якості
    if quality >= 0.8:
        result.quality_label = "excellent"
    elif quality >= 0.6:
        result.quality_label = "good"
    elif quality >= 0.4:
        result.quality_label = "acceptable"
    elif quality >= 0.2:
        result.quality_label = "poor"
    else:
        result.quality_label = "reject"
        result.rejected_reasons.append("Замало змісту або непридатна структура для використання у колекції")
    
    # Додати причини до приміток
    for r in reasons:
        result.notes.append(r)
    
    # Додати причини відкидання
    result.rejected_reasons = [
        r for r in reasons
        if ("мало" in r.lower() or "замало" in r.lower() or
            "дуже мало" in r.lower() or "не придатний" in r.lower() or
            "невідомий" in r.lower() or "не впізнали" in r.lower())
    ]
    
    return result


def get_document_priority(quality: PDFQualityResult) -> tuple[int, str]:
    """Повертає (priority 0-1000, reason)."""
    label = quality.quality_label
    doc_type = quality.doc_type
    
    # Базовий пріоритет за якістю
    base = {
        "excellent": 800,
        "good": 600,
        "acceptable": 350,
        "poor": 150,
        "reject": 0,
    }.get(label, 0)
    
    # Коррекції за типом документа
    type_bonus = {
        "scientific_article": 100,
        "technical_report": 100,
        "pedagogical": 70,
        "thesis": 50,
        "short_article": 50,
        "practical_guide": 60,
        "essay": -30,
        "presentation": -150,
        "fragment": -200,
        "unknown": -50,
    }
    base += type_bonus.get(doc_type, 0)
    
    # Бонуси за структуру
    if quality.has_references:
        base += 30
    if quality.has_conclusion:
        base += 20
    if quality.has_udc:
        base += 15
    if quality.has_titled_page:
        base += 10
    if quality.has_toc:
        base += 10
    if quality.has_abstract:
        base += 5
    
    # Штрафи за недоліки
    if quality.page_count == 1 and quality.chars_per_page < 500:
        base = max(0, base - 150)
    if quality.page_count <= 2 and quality.chars_per_page < 600:
        base = max(0, base - 100)
    if quality.producer and "ppt" in quality.producer.lower():
        base = max(0, base - 100)
    
    # Тематичний бонус
    topic_keywords = [
        "пальто", "швейн", "конструювання одягу", "технологія швейного",
        "моделювання одягу", "текстиль", "швейне виробництво", "легка промисловість",
        "розкрій", "крій", "шиття", "пошиття", "одяг", "шв", "пошит",
    ]
    is_relevant = any(
        kw in quality.subject_from_meta.lower() or kw in quality.title_from_meta.lower()
        for kw in topic_keywords
    )
    if is_relevant:
        base += 30
        if base > 1000:
            base = 1000
    
    # Визначення причини
    if base >= 700:
        reason = "Високий пріоритет — повноцінний документ з повною структурою"
    elif base >= 500:
        reason = "Достатній пріоритет — прийнятна стаття/звіт/документ"
    elif base >= 250:
        reason = "Середній пріоритет — включити завдяки повноцінності документу"
    elif base >= 100:
        reason = "Низький пріоритет — документ сумнівний, враховувати якщо немає кращого"
    else:
        reason = "Мінімальний пріоритет — документ придатний лише за відсутності інших варіантів"
    
    return (base, reason)


def analyze_and_report(pdf_path: Path) -> dict:
    """Повертає структуроване звіт про якість документа."""
    quality = analyze_pdf_quality(pdf_path)
    priority, reason = get_document_priority(quality)
    
    entry = {
        "document_id": pdf_path.stem if pdf_path.stem.isdigit() else f"unknown_{pdf_path.stem}",
        "file": str(pdf_path.name),
        "path": str(pdf_path),
        "page_count": quality.page_count,
        "total_text_chars": quality.total_text_chars,
        "chars_per_page": round(quality.chars_per_page, 1),
        "num_images": quality.num_images,
        "has_text_layer": quality.has_text_layer,
        "producer": quality.producer,
        "title_meta": quality.title_from_meta,
        "author_meta": quality.author_from_meta,
        "doc_type": quality.doc_type,
        "type_confidence": round(quality.confidence, 2),
        "quality_score": round(quality.quality_score, 2),
        "quality_label": quality.quality_label,
        "has_toc": quality.has_toc,
        "has_references": quality.has_references,
        "has_abstract": quality.has_abstract,
        "has_keywords_section": quality.has_keywords_section,
        "has_conclusion": quality.has_conclusion,
        "has_udc": quality.has_udc,
        "has_authors_list": quality.has_authors_list,
        "has_titled_page": quality.has_titled_page,
        "has_sections": quality.has_sections,
        "text_density": quality.text_density,
        "page_count_category": quality.page_count_category,
        "priority": priority,
        "priority_reason": reason,
        "decision": "ACCEPT" if priority >= 200 else ("REVIEW" if priority >= 50 else "REJECT"),
        "notes": quality.notes[:8],
        "rejected_reasons": quality.rejected_reasons,
    }
    
    return entry


def analyze_catalog(catalog_dir: Path) -> list[dict]:
    """Проаналізувати всі документи в каталозі."""
    results = []
    
    for pdf_file in sorted(catalog_dir.glob("*.pdf")):
        try:
            report = analyze_and_report(pdf_file)
            results.append(report)
        except Exception as e:
            results.append({
                "file": pdf_file.name,
                "decision": "ERROR",
                "error": str(e),
            })
    
    return results


def main():
    import sys
    import json
    
    if len(sys.argv) > 1:
        pdf_dir = Path(sys.argv[1])
    else:
        pdf_dir = Path("/opt/harvester/catalogs/catalog_20260827_142217/resources")
    
    print(f"\n{'='*80}")
    print(f"📊 АНАЛІЗ PDF ЯКОСТІ — Каталог: {pdf_dir}")
    print(f"{'='*80}")
    
    results = analyze_catalog(pdf_dir)
    
    # Відсортовано по пріоритету (спадання)
    results_sorted = sorted(
        [r for r in results if r.get("priority") is not None],
        key=lambda x: x.get("priority", 0),
        reverse=True
    )
    
    # Підрахунок статистики
    accept = sum(1 for r in results if r.get("decision") == "ACCEPT")
    review = sum(1 for r in results if r.get("decision") == "REVIEW")
    reject = sum(1 for r in results if r.get("decision") == "REJECT")
    error = sum(1 for r in results if r.get("decision") == "ERROR")
    
    print(f"\n📋 ПІДСУМОК")
    print(f"{'='*40}")
    print(f"Всього документів: {len(results)}")
    print(f"  ✅ Прийняти (ACCEPT): {accept}")
    print(f"  ⚠️  Переглянути (REVIEW): {review}")
    print(f"  ❌ Відкинути (REJECT): {reject}")
    print(f"  💥 Помилка (ERROR): {error}")
    print()
    
    # Виведення документів по пріоритету
    print(f"{'Пріор.':>5} {'Рішення':^12} {'Стор.':>5} {'Знак./ств':>10} {'Якість':^10} {'Тип':^20} {'Заголовок'}")
    print(f"{'-'*5} {'-'*12} {'-'*5} {'-'*10} {'-'*10} {'-'*20} {'-'*50}")
    
    for r in results_sorted:
        priority = r.get("priority", 0)
        decision = r.get("decision", "UNKNOWN")
        pages = r.get("page_count", "?")
        cps = r.get("chars_per_page", "?")
        ql = r.get("quality_label", "?")
        dt = r.get("doc_type", "?")
        title = r.get("title_meta", r.get("file", ""))
        if len(title) > 45:
            title = title[:42] + "..."
        
        decision_display = {"ACCEPT": "✅✅", "REVIEW": "⚠️⚡", "REJECT": "❌❌", "ERROR": "💥💥"}.get(decision, "?")
        ql_display = {"excellent": "ВИСОКА", "good": "ДОБРА", "acceptable": "НОРМ", "poor": "ПОГАНА", "reject": "ВІДК", "error": "ПОМИЛКА"}.get(ql, "❓")
        
        print(f"{priority:>5} {decision_display:^12} {str(pages):>5} {str(cps):>10} {ql_display:^10} {str(dt):^20} {title}")
    
    print(f"\n{'='*80}")
    print(f"📋 ПІДРОЗУМОКИ")
    print(f"{'='*80}")
    
    # Відкинуті
    if reject > 0:
        print(f"\n--- Відкинуті документи ({reject}) ---")
        for r in results_sorted:
            if r.get("decision") == "REJECT":
                print(f"  📄 {r.get('file', 'unknown').ljust(20)} | {r.get('doc_type', '?').ljust(20)} | {r.get('priority_reason', '')}")
                if r.get("rejected_reasons"):
                    for reason in r["rejected_reasons"]:
                        print(f"      ⚠️ {reason}")
    
    # Перегляд
    if review > 0:
        print(f"\n--- На перегляд ({review}) ---")
        for r in results_sorted:
            if r.get("decision") == "REVIEW":
                print(f"  📄 {r.get('file', 'unknown').ljust(20)} | {r.get('doc_type', '?').ljust(20)} | Пріор. {r.get('priority')} | {r.get('priority_reason', '')}")
    
    # Прийняті
    if accept > 0:
        print(f"\n--- Прийняті ({accept}) ---")
        for r in results_sorted[:10]:
            if r.get("decision") == "ACCEPT":
                print(f"  📄 {r.get('file', 'unknown').ljust(20)} | {r.get('doc_type', '?').ljust(20)} | Пріор. {r.get('priority')} | Якість: {r.get('quality_label')}")
        if accept > 10:
            print(f"  ... ще {accept - 10} документів")
    
    print(f"\n{'='*80}")
    
    # Збереження звіту як файл
    import datetime
    report_path = pdf_dir.parent / f"{pdf_dir.stem}_quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({
            "catalog_dir": str(pdf_dir),
            "analysis_date": datetime.datetime.now().isoformat(),
            "summary": {
                "total": len(results),
                "accept": accept,
                "review": review,
                "reject": reject,
                "error": error,
            },
            "documents": results_sorted,
            "rejected": [r for r in results_sorted if r.get("decision") == "REJECT"],
            "for_review": [r for r in results_sorted if r.get("decision") == "REVIEW"],
            "accepted": [r for r in results_sorted if r.get("decision") == "ACCEPT"],
        }, f, ensure_ascii=False, indent=2)
    print(f"📄 Звіт збережено: {report_path}")


if __name__ == "__main__":
    main()
