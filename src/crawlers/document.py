from dataclasses import dataclass


@dataclass
class Document:
    url: str
    title: str | None = None
    topic: str | None = None
    source: str | None = None
    doc_type: str | None = None
    min_pages: int | None = None
    benchmark: str | None = None
    search_query: str | None = None
    crawler_name: str | None = None
    snippet: str | None = None
    path: str | None = None
    pages: int | None = None
