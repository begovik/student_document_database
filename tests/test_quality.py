"""Тести аналізатора якості документів (DocumentQualityAnalyzer)."""

from harvester.verify.quality import DocumentQualityAnalyzer, QualityResult


def _good_doc(**overrides):
    doc = {
        "id": 1,
        "title": "Технологія пошиття чоловічого довгого пальта",
        "authors": ["Петренко І.А."],
        "year": 2020,
        "udc": "687.13",
        "doc_type": "textbook",
        "language": "uk",
        "page_count": 180,
        "has_text_layer": 1,
        "summary": {"text": "Повноцінний текст посібника з розділами. " * 60},
    }
    doc.update(overrides)
    return doc


class TestHardFilters:
    def test_good_document_passes(self):
        result = DocumentQualityAnalyzer().analyze(_good_doc())
        assert isinstance(result, QualityResult)
        assert result.passed is True
        assert result.score > 0

    def test_fragment_rejected_by_pages(self):
        result = DocumentQualityAnalyzer().analyze(_good_doc(page_count=2, doc_type="other"))
        assert result.passed is False
        assert any("page_count" in f for f in result.hard_failures)

    def test_fragment_rejected_by_text_layer(self):
        result = DocumentQualityAnalyzer().analyze(_good_doc(has_text_layer=0))
        assert result.passed is False
        assert any("has_text_layer" in f for f in result.hard_failures)

    def test_fragment_rejected_by_too_little_text(self):
        result = DocumentQualityAnalyzer().analyze(_good_doc(summary={"text": "ok"}))
        assert result.passed is False
        assert any("text_chars" in f for f in result.hard_failures)

    def test_russian_rejected(self):
        result = DocumentQualityAnalyzer().analyze(_good_doc(language="ru"))
        assert result.passed is False
        assert "russian_language" in result.hard_failures

    def test_powerpoint_rejected(self):
        result = DocumentQualityAnalyzer().analyze(
            _good_doc(title="NAME OF PRESENTATION", doc_type="report")
        )
        assert result.passed is False
        assert "presentation_powerpoint" in result.hard_failures

    def test_ppt_producer_rejected(self):
        result = DocumentQualityAnalyzer().analyze(
            _good_doc(extra={"producer": "Microsoft PowerPoint"})
        )
        assert result.passed is False
        assert "presentation_powerpoint" in result.hard_failures


class TestRank:
    def test_ranks_best_first_and_filters_bad(self):
        bad = _good_doc(id=2, page_count=2, doc_type="other")
        good = _good_doc(id=1)
        ranked = DocumentQualityAnalyzer().rank([bad, good])
        assert [d["id"] for d in ranked] == [1]

    def test_enriches_with_quality_fields(self):
        ranked = DocumentQualityAnalyzer().rank([_good_doc()])
        assert ranked[0]["_quality_score"] > 0
        assert ranked[0]["_quality"].passed is True
