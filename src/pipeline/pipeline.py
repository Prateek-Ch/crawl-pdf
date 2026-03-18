import os

class PDFPipeline:

    def __init__(self, crawlers, downloader, filters, metadata_store):
        self.crawlers = crawlers
        self.downloader = downloader
        self.filters = filters
        self.metadata_store = metadata_store

    def run(self):
        for crawler in self.crawlers:
            print(f"\n=== Running crawler: {crawler.__class__.__name__} ({crawler.topic}) ===")
            batch_size = 10
            start = 0
            successful = 0

            while successful < crawler.max_docs:
                docs = crawler.fetch_pdf_links_batch(batch_size, start)
                
                if not docs: break
                
                start += len(docs)
                prev_successful = successful

                for doc in docs:
                    if successful >= crawler.max_docs: break

                    print(f"Downloading: {doc.url}")
                    filename = f"{doc.source}_{crawler.topic}_{successful}.pdf"
                    success = self.downloader.download(doc.url, filename, crawler.topic)

                    if not success:
                        print("Download failed")
                        continue

                    path = os.path.join(self.downloader.save_dir, crawler.topic, filename)
                    doc.path = path

                    if not os.path.exists(path): continue

                    if not self.filters(doc):
                        os.remove(path)
                        continue

                    print(f"Saved: {filename}")

                    self.metadata_store.add(doc)
                    successful += 1

                if successful == prev_successful:
                    print("No progress, stopping crawler.")
                    break
        self.metadata_store.save()