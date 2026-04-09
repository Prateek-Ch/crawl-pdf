from .base_crawler import BaseCrawler
from .document import Document
from ddgs import DDGS

class DuckDuckGoCrawler(BaseCrawler):

    def __init__(self, topic, max_docs, doc_type=None, min_pages=None, search_query=None):
        super().__init__(topic, max_docs, search_query=search_query)
        self.doc_type = doc_type
        self.min_pages = min_pages

    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        query = f"{self.search_query} filetype:pdf"
        limit = max_results + start

        try:
            results = DDGS().text(query, max_results=limit)
        except Exception as exc:
            print(f"[DuckDuckGoCrawler] Search failed: {exc}")
            return []

        docs = []
        seen = set()

        for item in results[start:]:
            href = item.get("href")

            if not href:
                continue

            if ".pdf" in href.lower() and href not in seen:
                seen.add(href)

                docs.append(
                    Document(
                        url=href,
                        title=(item.get("title") or "").strip() or None,
                        topic=self.topic,
                        source="duckduckgo",
                        doc_type=self.doc_type,
                        min_pages=self.min_pages,
                        benchmark=getattr(self, "benchmark", None),
                        search_query=self.search_query,
                        crawler_name=self.__class__.__name__,
                        snippet=(item.get("body") or "").strip() or None,
                    )
                )

            if len(docs) >= max_results:
                break

        self.throttle()
        return docs
