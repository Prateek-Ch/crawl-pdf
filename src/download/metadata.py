import json
import os
from datetime import datetime, timezone


class MetadataStore:

    def __init__(self, path):
        self.path = path
        self.data = []
        self._seen_urls = set()
        self._seen_paths = set()
        self._load_existing()

    def _load_existing(self):
        if not os.path.exists(self.path):
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except (json.JSONDecodeError, OSError):
            loaded = []

        if not isinstance(loaded, list):
            loaded = []

        self.data = loaded

        for item in self.data:
            url = item.get("url")
            path = item.get("path")

            if url:
                self._seen_urls.add(url)
            if path:
                self._seen_paths.add(path)

    def has(self, doc):
        return doc.url in self._seen_urls or (doc.path is not None and doc.path in self._seen_paths)

    def add(self, doc):
        if self.has(doc):
            return False

        record = {
            "url": doc.url,
            "title": doc.title,
            "snippet": doc.snippet,
            "benchmark": doc.benchmark,
            "topic": doc.topic,
            "source": doc.source,
            "crawler": doc.crawler_name,
            "search_query": doc.search_query,
            "doc_type": doc.doc_type,
            "min_pages": doc.min_pages,
            "path": doc.path,
            "pages": doc.pages,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

        self.data.append(record)
        self._seen_urls.add(doc.url)
        if doc.path:
            self._seen_paths.add(doc.path)
        return True

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        sorted_data = sorted(
            self.data,
            key=lambda item: (
                item.get("benchmark") or "",
                item.get("doc_type") or "",
                item.get("source") or "",
                item.get("title") or "",
                item.get("url") or "",
            ),
        )
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(sorted_data, f, indent=2, ensure_ascii=False)
