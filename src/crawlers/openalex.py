# research papers

import requests
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

        response = requests.get(self.BASE_URL, params=params)

        if response.status_code != 200:
            return []

        data = response.json()
        docs = []

        for work in data.get("results", []):
            pdf_url = None

            # Try Open Access PDF
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

        return docs