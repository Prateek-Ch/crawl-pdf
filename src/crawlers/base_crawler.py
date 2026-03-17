from abc import ABC, abstractmethod

class BaseCrawler(ABC):

    def __init__(self, topic, max_docs=100):
        self.topic = topic
        self.max_docs = max_docs

    @abstractmethod
    def fetch_pdf_links(self) -> list:
        pass