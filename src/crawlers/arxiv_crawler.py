import requests
from .base_crawler import BaseCrawler
from .document import Document

import xml.etree.ElementTree as ET

class ArxivCrawler(BaseCrawler):

    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        url = f"http://export.arxiv.org/api/query?search_query=cat:{self.topic}&max_results={max_results}&start={start}"
        response = requests.get(url)

        root = ET.fromstring(response.content)

        docs = []
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            id_elem = entry.find("{http://www.w3.org/2005/Atom}id")
            title_elem = entry.find("{http://www.w3.org/2005/Atom}title")
            if id_elem is None or title_elem is None:
                continue
            
            pdf_link = None
            if id_elem is not None and id_elem.text is not None:
                pdf_link = id_elem.text.replace("abs", "pdf") + ".pdf"

            docs.append(
                Document(
                    url=pdf_link,
                    title=title_elem.text if title_elem is not None else None,
                    topic=self.topic,
                    source="arxiv",
                    doc_type="research_paper"
                )
            )

        return docs