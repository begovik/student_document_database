import json

import structlog

from harvester.classify.llm import LLMClient, LLMUnavailable, AllLimitsExhausted
from harvester.classify.taxonomy import load_topics
from harvester.config import get_settings
from harvester.db.connection import Database

logger = structlog.get_logger()

CLASSIFY_PROMPT = """Ти — класифікатор наукових документів. Проаналізуй метадані та фрагмент тексту документа.

ДОКУМЕНТ:
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Фрагмент тексту:
\"\"\"
{text_sample}
\"\"\"

ДОСТУПНІ ТЕМИ (код — назва):
{topics_list}

ЗАВДАННЯ. Відповідай ВИКЛЮЧНО валідним JSON без пояснень:
{{
  "topics": ["код1", "код2"],          // 1-3 коди зі списку вище, за релевантністю; [] якщо жодна не підходить
  "confidence": 0.0,                   // 0..1 впевненість у класифікації
  "doc_type": "article",               // article | book | textbook | methodical | thesis | dissertation | report | preprint | other
  "year": null,                        // рік видання (число) якщо видно у тексті, інакше null
  "publisher": null                    // видавництво якщо видно, інакше null
}}"""


class Classifier:
    """Зважені сигнали: S1 УДК (0.45), S3 ключові слова (0.15), S5 LLM (Gemini) — якщо доступний."""

    def __init__(self, db: Database):
        self.db = db
        self.settings = get_settings()
        self.llm = LLMClient()

    async def classify_document(self, doc: dict) -> dict:
        topics = await load_topics(self.db)
        scores: dict[int, float] = {}
        signals: dict[str, float] = {}

        # S1: УДК
        udc = doc.get("udc")
        if udc:
            for t in topics:
                for prefix in t["udc_prefixes"]:
                    if udc.startswith(prefix):
                        scores[t["id"]] = scores.get(t["id"], 0) + 0.45
                        signals[f"S1:{t['code']}"] = 0.45
                        break

        # S3: ключові слова
        haystack = " ".join(
            filter(None, [doc.get("title"), doc.get("title_hint"), (doc.get("text_sample") or "")[:2000]])
        ).lower()
        if haystack:
            for t in topics:
                hits = sum(1 for kw in t["keywords_uk"] + t["keywords_en"] if kw.lower() in haystack)
                if hits:
                    kw_score = min(0.15 + 0.05 * (hits - 1), 0.3)
                    scores[t["id"]] = scores.get(t["id"], 0) + kw_score
                    signals[f"S3:{t['code']}"] = round(kw_score, 3)

        # S5: LLM (Gemini 3.1 Flash → OpenRouter)
        llm_meta: dict = {}
        if self.llm.enabled and (doc.get("title") or doc.get("text_sample")):
            try:
                llm_meta = await self._classify_llm(doc, topics)
                for code in llm_meta.get("topics", [])[:3]:
                    match = next((t for t in topics if t["code"] == code), None)
                    if match:
                        conf = float(llm_meta.get("confidence") or 0.5)
                        scores[match["id"]] = scores.get(match["id"], 0) + 0.40 * conf
                        signals[f"S5:{code}"] = round(0.40 * conf, 3)
            except AllLimitsExhausted:
                raise
            except LLMUnavailable as e:
                logger.warning("llm_unavailable_fallback_rules", doc_id=doc.get("id"), error_msg=str(e))
            except Exception as e:
                logger.error("llm_classify_error", doc_id=doc.get("id"), error_msg=str(e))

        total = sum(scores.values())
        min_score = self.settings.classify.min_score
        max_topics = self.settings.classify.max_topics

        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        picked = [(tid, s) for tid, s in ranked if s >= min_score][:max_topics]

        return {
            "topics": [(tid, s / total if total > 0 else 0.0) for tid, s in picked],
            "signals": signals,
            "llm_meta": llm_meta,
        }

    async def _classify_llm(self, doc: dict, topics: list[dict]) -> dict:
        topics_list = "\n".join(f"- {t['code']} — {t['name_uk']} / {t['name_en']}" for t in topics)
        prompt = CLASSIFY_PROMPT.format(
            title=doc.get("title") or doc.get("title_hint") or "невідомо",
            authors=doc.get("authors") or "невідомі",
            language=doc.get("language") or "невідома",
            udc=doc.get("udc") or "—",
            text_sample=(doc.get("text_sample") or "")[:3000],
            topics_list=topics_list,
        )

        resp = await self.llm.complete(prompt)
        raw = resp.text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw
            raw = raw.rsplit("```", 1)[0]

        data = json.loads(raw)
        logger.info(
            "llm_classified",
            doc_id=doc.get("id"),
            provider=resp.provider,
            model=resp.model,
            topics=data.get("topics"),
            confidence=data.get("confidence"),
        )
        return data

    async def save_classification(self, doc_id: int, result: dict) -> None:
        await self.db.execute("DELETE FROM document_topics WHERE document_id = ?", (doc_id,))
        for topic_id, score in result["topics"]:
            await self.db.execute(
                """
                INSERT OR REPLACE INTO document_topics (document_id, topic_id, score, signals)
                VALUES (?, ?, ?, ?)
                """,
                (doc_id, topic_id, round(score, 4), json.dumps(result["signals"], ensure_ascii=False)),
            )

        llm_meta = result.get("llm_meta") or {}
        updates: list[str] = []
        params: list = []

        if llm_meta.get("doc_type") and llm_meta["doc_type"] != "other":
            updates.append("doc_type = ?")
            params.append(llm_meta["doc_type"])
        if isinstance(llm_meta.get("year"), int) and 1000 < llm_meta["year"] <= 2100:
            updates.append("year = COALESCE(year, ?)")
            params.append(llm_meta["year"])
        if llm_meta.get("publisher"):
            updates.append("publisher = COALESCE(publisher, ?)")
            params.append(llm_meta["publisher"])

        if updates:
            params.append(doc_id)
            await self.db.execute(f"UPDATE documents SET {', '.join(updates)} WHERE id = ?", tuple(params))
