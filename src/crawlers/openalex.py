# research papers

from .base_crawler import BaseCrawler
from .document import Document

class OpenAlexCrawler(BaseCrawler):

    BASE_URL = "https://api.openalex.org/works"
    
    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        params = {
            "search": self.topic,
            "per-page": max_results,
            "page": (start // max_results) + 1
        }

        response = self.safe_request(self.BASE_URL, params)

        if response is None:
            return []

        try:
            data = response.json()
        except Exception:
            print(f"[{self.__class__.__name__}] JSON parse failed")
            return []

        docs = []

        for work in data.get("results", []):
            pdf_url = None

            if work.get("open_access"):
                pdf_url = work["open_access"].get("oa_url")

            if not pdf_url:
                continue

            docs.append(
                Document(
                    url=pdf_url,
                    title=work.get("title"),
                    topic=self.topic,
                    source="openalex",
                    doc_type="research_paper"
                )
            )

        self.throttle()
        return docs