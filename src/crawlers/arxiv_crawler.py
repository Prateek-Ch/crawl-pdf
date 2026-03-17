# src/crawlers/arxiv_crawler.py

import requests
from .base_crawler import BaseCrawler
import xml.etree.ElementTree as ET

class ArxivCrawler(BaseCrawler):

    def fetch_pdf_links(self):
        url = f"http://export.arxiv.org/api/query?search_query=cat:{self.topic}&max_results={self.max_docs}"
        response = requests.get(url)

        root = ET.fromstring(response.content)

        links = []
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            id_elem = entry.find("{http://www.w3.org/2005/Atom}id")
            if id_elem is not None and id_elem.text is not None:
                pdf_link = id_elem.text.replace("abs", "pdf") + ".pdf"
                links.append(pdf_link)

        return links