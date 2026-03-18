# Books
import requests
from .base_crawler import BaseCrawler
from .document import Document

class InternetArchiveCrawler(BaseCrawler):

    SEARCH_URL = "https://archive.org/advancedsearch.php"
    
    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        params = {
            "q": f"{self.topic} AND mediatype:texts",
            "fl[]": "identifier,title",
            "rows": max_results,
            "page": (start // max_results) + 1,
            "output": "json"
        }

        response = requests.get(self.SEARCH_URL, params=params)

        if response.status_code != 200:
            return []

        data = response.json()
        docs = []

        for doc in data["response"]["docs"]:
            identifier = doc.get("identifier")
            title = doc.get("title")

            if not identifier:
                continue

            pdf_url = f"https://archive.org/download/{identifier}/{identifier}.pdf"

            docs.append(
                Document(
                    url=pdf_url,
                    title=title,
                    topic=self.topic,
                    source="internet_archive",
                    doc_type="book"
                )
            )

        return docs