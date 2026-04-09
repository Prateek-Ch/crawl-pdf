from pathlib import Path

import yaml

from src.crawlers import ArxivCrawler, DuckDuckGoCrawler, InternetArchiveCrawler
from src.download.attempt_store import AttemptStore
from src.download.metadata import MetadataStore
from src.download.pdf_download import PDFDownloader
from src.pipeline.pipeline import PDFPipeline
from src.processing.filter import is_valid_pdf


CONFIG_PATH = Path("config/pipeline_config.yaml")


def load_pipeline_config(path=CONFIG_PATH):
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    benchmarks = config.get("benchmarks")
    if not isinstance(benchmarks, list):
        raise ValueError("pipeline config must define a 'benchmarks' list")

    return benchmarks


def build_crawlers(config_path=CONFIG_PATH):
    crawlers = []

    for benchmark in load_pipeline_config(config_path):
        benchmark_name = benchmark["name"]

        for source in benchmark.get("sources", []):
            query = source["query"]
            topic = f"{benchmark_name} {source['doc_type']}"
            crawler_name = source["crawler"]

            if crawler_name == "arxiv":
                crawler = ArxivCrawler(
                    topic=topic,
                    max_docs=source["max_docs"],
                    search_query=query,
                )
                crawler.doc_type = source["doc_type"]
                crawler.min_pages = source["min_pages"]
            elif crawler_name == "internet_archive":
                crawler = InternetArchiveCrawler(
                    topic=topic,
                    max_docs=source["max_docs"],
                    doc_type=source["doc_type"],
                    min_pages=source["min_pages"],
                    search_query=query,
                )
            elif crawler_name == "duckduckgo":
                if DuckDuckGoCrawler is None:
                    raise ImportError("DuckDuckGoCrawler requires the optional 'ddgs' dependency")
                crawler = DuckDuckGoCrawler(
                    topic=topic,
                    max_docs=source["max_docs"],
                    doc_type=source["doc_type"],
                    min_pages=source["min_pages"],
                    search_query=query,
                )
            else:
                raise ValueError(f"unsupported crawler: {crawler_name}")

            crawler.benchmark = benchmark_name
            crawler.include_any = source.get("include_any", [])
            crawler.exclude_any = source.get("exclude_any", [])
            crawler.allowed_domains = source.get("allowed_domains", [])
            crawler.blocked_domains = source.get("blocked_domains", [])
            crawler.batch_size = source.get("batch_size", 10)
            crawler.max_attempts = source.get("max_attempts")
            crawler.max_no_progress = source.get("max_no_progress", 10)
            crawlers.append(crawler)

    return crawlers


def main():
    pipeline = PDFPipeline(
        crawlers=build_crawlers(),
        downloader=PDFDownloader(save_dir="data/raw/"),
        filters=is_valid_pdf,
        metadata_store=MetadataStore("data/metadata.json"),
        attempt_store=AttemptStore(
            state_path="data/attempt_state.json",
            events_path="data/attempt_events.jsonl",
        ),
        run_summary_dir="data/run_summaries",
    )
    pipeline.run()


if __name__ == "__main__":
    main()
