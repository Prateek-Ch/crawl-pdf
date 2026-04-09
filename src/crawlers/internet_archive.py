from urllib.parse import quote

from .base_crawler import BaseCrawler
from .document import Document


class InternetArchiveCrawler(BaseCrawler):

    SEARCH_URL = "https://archive.org/advancedsearch.php"
    METADATA_URL = "https://archive.org/metadata"

    def __init__(self, topic, max_docs, doc_type="book", min_pages=None, search_query=None):
        super().__init__(topic, max_docs, search_query=search_query)
        self.doc_type = doc_type
        self.min_pages = min_pages
        self._pdf_url_cache = {}
    
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

            pdf_url = self._resolve_pdf_url(identifier)
            if pdf_url is None:
                continue

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

    def _resolve_pdf_url(self, identifier):
        if identifier in self._pdf_url_cache:
            return self._pdf_url_cache[identifier]

        response = self.safe_request(f"{self.METADATA_URL}/{identifier}")
        if response is None:
            self._pdf_url_cache[identifier] = None
            return None

        try:
            data = response.json()
        except Exception:
            self._pdf_url_cache[identifier] = None
            return None

        metadata = data.get("metadata", {})
        collections = metadata.get("collection", [])
        if isinstance(collections, str):
            collections = [collections]

        if str(metadata.get("access-restricted-item") or "").lower() == "true":
            self._pdf_url_cache[identifier] = None
            return None

        if any(name in {"internetarchivebooks", "inlibrary", "printdisabled"} for name in collections):
            self._pdf_url_cache[identifier] = None
            return None

        files = data.get("files", [])
        best_name = None
        best_score = None

        for file_info in files:
            name = file_info.get("name")
            file_format = (file_info.get("format") or "").lower()
            is_private = str(file_info.get("private") or "").lower() == "true"

            if not name or is_private:
                continue

            lower_name = name.lower()
            if not lower_name.endswith(".pdf"):
                continue

            if "encrypted pdf" in file_format or lower_name.endswith(".lcpdf"):
                continue

            score = self._score_pdf_candidate(lower_name, file_format, file_info.get("source"))
            if best_score is None or score > best_score:
                best_name = name
                best_score = score

        if best_name is None:
            self._pdf_url_cache[identifier] = None
            return None

        pdf_url = f"https://archive.org/download/{identifier}/{quote(best_name)}"
        self._pdf_url_cache[identifier] = pdf_url
        return pdf_url

    def _score_pdf_candidate(self, name, file_format, source):
        score = 0

        if source == "original":
            score += 5
        if "pdf" in file_format:
            score += 3
        if "text pdf" in file_format:
            score += 2
        if name.endswith(".pdf"):
            score += 1

        return score
