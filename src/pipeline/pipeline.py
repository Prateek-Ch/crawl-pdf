import os
import re


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

            no_progress_count = 0
            # max number of consecutive failed batches before stopping
            max_no_progress = 10
            attempts = 0
            # max total attempts to fetch documents before giving up on crawler
            max_attempts = 500

            while successful < crawler.max_docs:
                docs = crawler.fetch_pdf_links_batch(batch_size, start)
                if not docs:
                    print("No more documents from crawler.")
                    break

                start += len(docs)
                attempts += len(docs)

                if attempts >= max_attempts:
                    print("Reached max attempts. Stopping crawler.")
                    break

                prev_successful = successful

                for doc in docs:
                    if successful >= crawler.max_docs:
                        break

                    filename = self._build_filename(crawler, doc, successful)
                    doc.path = os.path.join(
                        self.downloader.save_dir,
                        self._safe_slug(crawler.topic),
                        filename
                    )
                    doc.benchmark = getattr(crawler, "benchmark", None)
                    doc.search_query = getattr(crawler, "search_query", None)
                    doc.crawler_name = crawler.__class__.__name__

                    if not doc.url or self.metadata_store.has(doc):
                        print("Skipping duplicate document")
                        continue

                    print(f"Downloading: {doc.url}")

                    success = self.downloader.download(
                        doc.url,
                        filename,
                        self._safe_slug(crawler.topic)
                    )

                    if not success:
                        print("Download failed")
                        continue

                    if not os.path.exists(doc.path):
                        print("File not found after download")
                        continue

                    if not self.filters(doc):
                        print("Filtered out")
                        os.remove(doc.path)
                        continue

                    print(f"Saved: {filename}")

                    if self.metadata_store.add(doc):
                        successful += 1

                if successful == prev_successful:
                    no_progress_count += 1
                    print(f"No progress ({no_progress_count}/{max_no_progress})")

                    if no_progress_count >= max_no_progress:
                        print("Stopping crawler after repeated failures.")
                        break
                else:
                    no_progress_count = 0

            print(f"Finished {crawler.topic}: {successful} documents collected.")

        self.metadata_store.save()
        print("Metadata saved.")

    def _build_filename(self, crawler, doc, index):
        topic_slug = self._safe_slug(crawler.topic)
        title_slug = self._safe_slug(doc.title or f"document_{index}")[:80]
        return f"{doc.source}_{topic_slug}_{index:04d}_{title_slug}.pdf"

    def _safe_slug(self, value):
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return slug.strip("._") or "untitled"
