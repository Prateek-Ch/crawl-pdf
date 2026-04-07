from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse, unquote

from .base_crawler import BaseCrawler
from .document import Document


class _DuckDuckGoParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.results = []
        self._current_href = None
        self._capture_text = False
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs = dict(attrs)
            href = attrs.get("href") or ""

            if "uddg=" in href:
                self._current_href = href
                self._capture_text = True
                self._current_text = []

    def handle_data(self, data):
        if self._capture_text:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._current_href:
            parsed = urlparse(self._current_href)
            query = parse_qs(parsed.query)

            if "uddg" in query:
                real_url = unquote(query["uddg"][0])

                if ".pdf" in real_url.lower():
                    title = " ".join(self._current_text).strip()

                    self.results.append({
                        "url": real_url,
                        "title": title
                    })

            self._current_href = None
            self._capture_text = False
            self._current_text = []

class DuckDuckGoCrawler(BaseCrawler):

    SEARCH_URL = "https://lite.duckduckgo.com/lite/"

    def __init__(self, topic, max_docs, doc_type=None, min_pages=None):
        super().__init__(topic, max_docs)
        self.doc_type = doc_type
        self.min_pages = min_pages

    def fetch_pdf_links(self):
        return self.fetch_pdf_links_batch(self.max_docs, 0)

    def fetch_pdf_links_batch(self, max_results, start=0):
        query = f"{self.topic} filetype:pdf"

        params = {
            "q": query,
            "s": str(start)
        }

        response = self.safe_request(self.SEARCH_URL, params=params)

        if not response:
            return []

        parser = _DuckDuckGoParser()
        parser.feed(response.text)

        # deduplicate
        seen = set()
        docs = []

        for item in parser.results:
            url = item["url"]

            if url in seen:
                continue
            seen.add(url)

            doc = Document(
                url=url,
                title=item["title"],
                topic=self.topic,
                source="duckduckgo",
                doc_type=self.doc_type,
                min_pages=self.min_pages
            )

            docs.append(doc)

            if len(docs) >= max_results:
                break

        self.throttle()
        return docs