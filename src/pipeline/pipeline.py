# src/pipeline/pipeline.py

import os

class PDFPipeline:

    def __init__(self, crawler, downloader, filters):
        self.crawler = crawler
        self.downloader = downloader
        self.filters = filters

    def run(self):
        batch_size = 10
        start = 0
        successful = 0

        while successful < self.crawler.max_docs:
            links = self.crawler.fetch_pdf_links_batch(batch_size, start)
            if not links:
                break
            start += len(links)

            for link in links:
                if successful >= self.crawler.max_docs:
                    break

                filename = f"{self.crawler.topic}_{successful}.pdf"
                success = self.downloader.download(link, filename)

                if not success:
                    continue

                path = os.path.join(self.downloader.save_dir, filename)

                if not self.filters(path):
                    os.remove(path)
                    continue

                successful += 1