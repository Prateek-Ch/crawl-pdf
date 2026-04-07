import json

class MetadataStore:

    def __init__(self, path):
        self.path = path
        self.data = []

    def add(self, doc):
        self.data.append({
            "url": doc.url,
            "title": doc.title,
            "topic": doc.topic,
            "source": doc.source,
            "doc_type": doc.doc_type,
            "min_pages": doc.min_pages,
            "path": doc.path,
            "pages": doc.pages
        })

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)