# src/pipeline/pipeline.py

import os

class PDFPipeline:

    def __init__(self, crawler, downloader, filters):
        self.crawler = crawler
        self.downloader = downloader
        self.filters = filters

    def run(self):
        links = self.crawler.fetch_pdf_links()

        for i, link in enumerate(links):
            filename = f"{self.crawler.topic}_{i}.pdf"

            success = self.downloader.download(link, filename)

            if not success:
                continue

            path = os.path.join(self.downloader.save_dir, filename)

            if not self.filters(path):
                os.remove(path)