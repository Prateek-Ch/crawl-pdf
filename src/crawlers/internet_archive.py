# Books

from .base_crawler import BaseCrawler
from .document import Document

class InternetArchiveCrawler(BaseCrawler):

    SEARCH_URL = "https://archive.org/advancedsearch.php"

    def __init__(self, topic, max_docs, doc_type="book", min_pages=None, search_query=None):
        super().__init__(topic, max_docs, search_query=search_query)
        self.doc_type = doc_type
        self.min_pages = min_pages
    
    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        params = {
            "q": f"{self.search_query} AND mediatype:texts",
            "fl[]": "identifier,title",
            "rows": max_results,
            "page": (start // max_results) + 1,
            "output": "json"
        }

        response = self.safe_request(self.SEARCH_URL, params)

        if response is None:
            return []

        try:
            data = response.json()
        except Exception:
            print(f"[{self.__class__.__name__}] JSON parse failed")
            return []

        docs = []

        for item in data.get("response", {}).get("docs", []):
            identifier = item.get("identifier")
            title = item.get("title")

            if not identifier:
                continue

            pdf_url = f"https://archive.org/download/{identifier}/{identifier}.pdf"

            docs.append(
                Document(
                    url=pdf_url,
                    title=title,
                    topic=self.topic,
                    source="internet_archive",
                    doc_type=self.doc_type,
                    min_pages=self.min_pages,
                    benchmark=getattr(self, "benchmark", None),
                    search_query=self.search_query,
                    crawler_name=self.__class__.__name__,
                )
            )

        self.throttle()
        return docs
