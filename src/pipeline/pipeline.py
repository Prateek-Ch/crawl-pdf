import os

class PDFPipeline:

    def __init__(self, crawler, downloader, filters, metadata_store):
        self.crawler = crawler
        self.downloader = downloader
        self.filters = filters
        self.metadata_store = metadata_store

    def run(self):
        batch_size = 10
        start = 0
        successful = 0

        while successful < self.crawler.max_docs:
            docs = self.crawler.fetch_pdf_links_batch(batch_size, start)

            if not docs:
                break

            start += len(docs)

            for doc in docs:
                if successful >= self.crawler.max_docs:
                    break

                filename = f"{doc.source}_{successful}.pdf"
                success = self.downloader.download(doc.url, filename)

                if not success:
                    continue

                path = os.path.join(self.downloader.save_dir, filename)
                doc.path = path

                if not self.filters(doc):
                    os.remove(path)
                    continue
                self.metadata_store.add(doc)
                successful += 1
        self.metadata_store.save()