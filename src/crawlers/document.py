class Document:
    def __init__(self, url, title=None, topic=None, source=None, doc_type=None):
        self.url = url
        self.title = title
        self.topic = topic
        self.source = source
        self.doc_type = doc_type
        self.path = None
        self.pages = None