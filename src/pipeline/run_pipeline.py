from pathlib import Path

import yaml

from src.crawlers import ArxivCrawler, DuckDuckGoCrawler, InternetArchiveCrawler
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
            topic = f"{benchmark_name} {source['doc_type']} {query}"
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
            crawlers.append(crawler)

    return crawlers


def main():
    pipeline = PDFPipeline(
        crawlers=build_crawlers(),
        downloader=PDFDownloader(save_dir="data/raw/"),
        filters=is_valid_pdf,
        metadata_store=MetadataStore("data/metadata.json"),
    )
    pipeline.run()


if __name__ == "__main__":
    main()
