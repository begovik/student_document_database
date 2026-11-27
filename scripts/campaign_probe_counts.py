"""Вимірює розмір вибірки OpenAlex для поточних запитів кампанії (read-only).

Дає змогу відкинути запити з нулем або з мільйонним хвостом ще до того,
як вони вичерпають квоту openalex-каналу.
"""

import asyncio

import httpx

from harvester.config import get_settings

QUERIES_UK = [
    "неповнолітній договір цивільний кодекс",
    "дієздатність неповнолітньої особи",
    "укладення договору неповнолітнім",
    "недійсність договору неповнолітній",
    "виконання договору неповнолітнім",
    "опіка піклування майно дитини розпорядження",
    "свобода договору межі добросовісність сторін",
    "законне представництво неповнолітньої особи",
    "цивільна дієздатність неповнолітнього",
    "добросовісний набувач майно дитини",
    "припинення договору досягненням 18 років",
    "договірна правосуб'єктність неповнолітніх",
    "правочинність договору за участю неповнолітньої особи",
    "цивільне право договірне право методичні вказівки",
    "цивільне право України договірні відносини практикум",
    "цивільне право України договірне право монографія",
    "захист прав дитини майно договір",
    "цивільний кодекс України зобов'язальне право неповнолітній",
]

QUERIES_INT = [
    "capacity minor contract",
    "Minderjährige Vertragsschluss",
    "acte juridique mineur capacité",
    "contracts with minors capacity comparative law",
    "minor consent parental contract validity law",
]


async def count(client: httpx.AsyncClient, label: str, search: str, filt: str) -> int:
    r = await client.get(
        "https://api.openalex.org/works",
        params={
            "search": search,
            "filter": filt,
            "per-page": 1,
            "mailto": get_settings().contact.email,
        },
    )
    if r.status_code != 200:
        print(f"  HTTP {r.status_code} | {search[:50]}")
        return -1
    return int(r.json().get("meta", {}).get("count", 0))


async def main() -> None:
    uk = "language:uk,open_access.is_oa:true"
    async with httpx.AsyncClient(timeout=45) as client:
        print("=== UA-укр (lang=uk, OA) ===")
        for q in QUERIES_UK:
            c = await count(client, "uk", q, uk)
            print(f"  {c:>7}  {q}")

        print("\n=== Міжнародні (без мовного фільтра, type:article) ===")
        art = "open_access.is_oa:true"
        for q in QUERIES_INT:
            c = await count(client, "int", q, art)
            print(f"  {c:>7}  {q}")


if __name__ == "__main__":
    asyncio.run(main())
