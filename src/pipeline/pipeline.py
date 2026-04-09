import os
import re
import json
from datetime import datetime, timezone
from urllib.parse import urlparse


class PDFPipeline:

    def __init__(self, crawlers, downloader, filters, metadata_store, attempt_store=None, run_summary_dir=None):
        self.crawlers = crawlers
        self.downloader = downloader
        self.filters = filters
        self.metadata_store = metadata_store
        self.attempt_store = attempt_store
        self.run_summary_dir = run_summary_dir
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def run(self):
        run_summary = {
            "run_id": self.run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "crawlers": [],
        }

        for crawler in self.crawlers:
            print(f"\n=== Running crawler: {crawler.__class__.__name__} ({crawler.topic}) ===")
            batch_size = getattr(crawler, "batch_size", 10)
            start = 0
            successful = 0
            seen_urls = set()
            attempted_urls = set()
            failed_urls = set()
            filtered_urls = set()
            existing_urls = set()
            rejected_urls = set()
            empty_urls = 0
            skipped_urls = set()

            no_progress_count = 0
            # max number of consecutive failed batches before stopping
            max_no_progress = getattr(crawler, "max_no_progress", 10)
            # max total unique download attempts before giving up on crawler
            max_attempts = getattr(crawler, "max_attempts", None) or max(crawler.max_docs * 20, 200)

            while successful < crawler.max_docs:
                if len(attempted_urls) >= max_attempts:
                    print("Reached max unique attempts. Stopping crawler.")
                    break

                docs = crawler.fetch_pdf_links_batch(batch_size, start)
                if not docs:
                    print("No more documents from crawler.")
                    break

                start += len(docs)

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

                    if not doc.url:
                        empty_urls += 1
                        print("Skipping document with empty URL")
                        continue

                    if doc.url in seen_urls:
                        print("Skipping URL already seen in this run")
                        continue

                    seen_urls.add(doc.url)

                    if self.metadata_store.has(doc):
                        existing_urls.add(doc.url)
                        if self.attempt_store:
                            self.attempt_store.record(doc, "existing", "already_in_metadata", run_id=self.run_id)
                        print("Skipping document already present in metadata")
                        continue

                    if self.attempt_store:
                        should_skip, skip_reason = self.attempt_store.should_skip(doc.url)
                        if should_skip:
                            skipped_urls.add(doc.url)
                            print(f"Skipping document based on attempt history ({skip_reason})")
                            continue

                    is_match, reason = self._matches_candidate_rules(doc, crawler)
                    if not is_match:
                        rejected_urls.add(doc.url)
                        if self.attempt_store:
                            self.attempt_store.record(doc, "rejected", reason, run_id=self.run_id)
                        print(f"Rejected candidate ({reason})")
                        continue

                    if len(attempted_urls) >= max_attempts:
                        print("Reached max unique attempts. Stopping crawler.")
                        break

                    attempted_urls.add(doc.url)

                    print(f"Downloading: {doc.url}")

                    success, detail = self.downloader.download(
                        doc.url,
                        filename,
                        self._safe_slug(crawler.topic)
                    )

                    if not success:
                        failed_urls.add(doc.url)
                        if self.attempt_store:
                            self.attempt_store.record(doc, "download_failed", detail, run_id=self.run_id)
                        print(f"Download failed ({detail})")
                        continue

                    if not os.path.exists(doc.path):
                        if self.attempt_store:
                            self.attempt_store.record(doc, "download_failed", "missing_downloaded_file", run_id=self.run_id)
                        print("File not found after download")
                        continue

                    if not self.filters(doc):
                        filtered_urls.add(doc.url)
                        if self.attempt_store:
                            self.attempt_store.record(doc, "filtered", "failed_pdf_filter", run_id=self.run_id)
                        print("Filtered out")
                        os.remove(doc.path)
                        continue

                    print(f"Saved: {filename}")

                    if self.metadata_store.add(doc):
                        if self.attempt_store:
                            self.attempt_store.record(doc, "saved", "success", run_id=self.run_id)
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
            print(
                "Run stats: "
                f"seen={len(seen_urls)}, "
                f"attempted={len(attempted_urls)}, "
                f"skipped={len(skipped_urls)}, "
                f"failed={len(failed_urls)}, "
                f"filtered={len(filtered_urls)}, "
                f"rejected={len(rejected_urls)}, "
                f"existing={len(existing_urls)}, "
                f"empty_url={empty_urls}"
            )
            run_summary["crawlers"].append({
                "topic": crawler.topic,
                "benchmark": getattr(crawler, "benchmark", None),
                "doc_type": getattr(crawler, "doc_type", None),
                "source": crawler.__class__.__name__,
                "target_docs": crawler.max_docs,
                "collected_docs": successful,
                "seen": len(seen_urls),
                "attempted": len(attempted_urls),
                "skipped": len(skipped_urls),
                "failed": len(failed_urls),
                "filtered": len(filtered_urls),
                "rejected": len(rejected_urls),
                "existing": len(existing_urls),
                "empty_url": empty_urls,
            })

        self.metadata_store.save()
        if self.attempt_store:
            self.attempt_store.save()
        run_summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._save_run_summary(run_summary)
        print("Metadata saved.")

    def _build_filename(self, crawler, doc, index):
        topic_slug = self._safe_slug(crawler.topic)
        title_slug = self._safe_slug(doc.title or f"document_{index}")[:80]
        return f"{doc.source}_{topic_slug}_{index:04d}_{title_slug}.pdf"

    def _safe_slug(self, value):
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return slug.strip("._") or "untitled"

    def _matches_candidate_rules(self, doc, crawler):
        parsed_url = urlparse(doc.url)
        haystack = " ".join(
            part for part in [
                doc.title or "",
                getattr(doc, "snippet", "") or "",
                doc.url or "",
            ] if part
        ).lower()

        include_any = [term.lower() for term in getattr(crawler, "include_any", [])]
        exclude_any = [term.lower() for term in getattr(crawler, "exclude_any", [])]
        allowed_domains = [domain.lower() for domain in getattr(crawler, "allowed_domains", [])]
        blocked_domains = [domain.lower() for domain in getattr(crawler, "blocked_domains", [])]
        domain = (parsed_url.netloc or "").lower()
        path = (parsed_url.path or "").lower()

        if domain == "github.com" and "/blob/" in path:
            return False, "html_wrapper"

        if domain in {"github.com", "www.github.com"} and not path.endswith(".pdf"):
            return False, "html_wrapper"

        if blocked_domains and any(domain == blocked or domain.endswith(f".{blocked}") for blocked in blocked_domains):
            return False, "blocked_domain"

        if allowed_domains and not any(domain == allowed or domain.endswith(f".{allowed}") for allowed in allowed_domains):
            return False, "domain_not_allowed"

        if exclude_any and any(term in haystack for term in exclude_any):
            return False, "excluded_term"

        if include_any and not any(term in haystack for term in include_any):
            return False, "missing_required_term"

        return True, None

    def _save_run_summary(self, run_summary):
        if not self.run_summary_dir:
            return

        os.makedirs(self.run_summary_dir, exist_ok=True)
        latest_path = os.path.join(self.run_summary_dir, "latest.json")
        archived_path = os.path.join(self.run_summary_dir, f"{self.run_id}.json")

        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, ensure_ascii=False)

        with open(archived_path, "w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, ensure_ascii=False)
