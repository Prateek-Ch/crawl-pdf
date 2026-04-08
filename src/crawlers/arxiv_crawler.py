import xml.etree.ElementTree as ET
from .base_crawler import BaseCrawler
from .document import Document


class ArxivCrawler(BaseCrawler):
    
    BASE_URL = "http://export.arxiv.org/api/query"

    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        params = {
            "search_query": f"cat:{self.search_query}",
            "max_results": max_results,
            "start": start
        }

        response = self.safe_request(self.BASE_URL, params)

        if response is None:
            return []

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            print("XML parse failed")
            return []

        docs = []
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            id_elem = entry.find("{http://www.w3.org/2005/Atom}id")
            title_elem = entry.find("{http://www.w3.org/2005/Atom}title")
            if id_elem is None or title_elem is None:
                continue
            
            pdf_link = None
            if id_elem.text is not None:
                pdf_link = id_elem.text.replace("abs", "pdf") + ".pdf"

            docs.append(
                Document(
                    url=pdf_link,
                    title=title_elem.text.strip() if title_elem is not None and title_elem.text else None,
                    topic=self.topic,
                    source="arxiv",
                    doc_type=getattr(self, "doc_type", "research_paper"),
                    min_pages=getattr(self, "min_pages", None),
                    benchmark=getattr(self, "benchmark", None),
                    search_query=self.search_query,
                    crawler_name=self.__class__.__name__,
                )
            )

        self.throttle()
        return docs
