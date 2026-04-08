from .base_crawler import BaseCrawler
from .document import Document
from bs4 import BeautifulSoup


class DuckDuckGoCrawler(BaseCrawler):

    SEARCH_URL = "https://lite.duckduckgo.com/lite/"

    def __init__(self, topic, max_docs, doc_type=None, min_pages=None, search_query=None):
        super().__init__(topic, max_docs, search_query=search_query)
        self.doc_type = doc_type
        self.min_pages = min_pages

    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        query = f"{self.search_query} filetype:pdf"

        data = {
            "q": query
        }

        response = self.session.post(
            self.SEARCH_URL,
            data=data,
            headers=self.headers,
            timeout=self.TIMEOUT
        )

        if not response or response.status_code != 200:
            print(f"[DuckDuckGoCrawler] Bad status: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        docs = []
        seen = set()

        for a in soup.find_all("a"):
            href = a.get("href")

            if not href:
                continue

            if ".pdf" in href.lower() and href not in seen:
                seen.add(href)

                docs.append(
                    Document(
                        url=href,
                        title=a.text.strip(),
                        topic=self.topic,
                        source="duckduckgo",
                        doc_type=self.doc_type,
                        min_pages=self.min_pages,
                        benchmark=getattr(self, "benchmark", None),
                        search_query=self.search_query,
                        crawler_name=self.__class__.__name__,
                    )
                )

            if len(docs) >= max_results:
                break

        self.throttle()
        return docs
