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
        target_count = max_results + start
        queries = [self.search_query, *getattr(self, "query_variants", [])]
        docs = []
        seen_urls = set()

        for raw_query in queries:
            if len(docs) >= target_count:
                break

            query = f"{raw_query} filetype:pdf"
            limit = target_count

            try:
                results = DDGS().text(query, max_results=limit)
            except Exception as exc:
                print(f"[DuckDuckGoCrawler] Search failed for query '{raw_query}': {exc}")
                continue

            for item in results:
                href = item.get("href")

                if not href:
                    continue

                if ".pdf" in href.lower() and href not in seen_urls:
                    seen_urls.add(href)

                    docs.append(
                        Document(
                            url=href,
                            title=(item.get("title") or "").strip() or None,
                            topic=self.topic,
                            source="duckduckgo",
                            doc_type=self.doc_type,
                            min_pages=self.min_pages,
                            benchmark=getattr(self, "benchmark", None),
                            search_query=raw_query,
                            crawler_name=self.__class__.__name__,
                            snippet=(item.get("body") or "").strip() or None,
                        )
                    )

                if len(docs) >= target_count:
                    break

        self.throttle()
        return docs[start:start + max_results]
