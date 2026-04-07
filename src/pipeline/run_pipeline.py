from src.crawlers.duckduckgo import DuckDuckGoCrawler
from src.download.pdf_download import PDFDownloader
from src.processing.filter import is_valid_pdf
from src.pipeline.pipeline import PDFPipeline
from src.download.metadata import MetadataStore
from src.crawlers.internet_archive import InternetArchiveCrawler


def build_crawlers():
    crawlers = [
        # InternetArchiveCrawler(
        #     "industrial safety handbook manual",
        #     max_docs=2,
        #     doc_type="user_manual_or_guide",
        #     min_pages=8,
        # ),
        DuckDuckGoCrawler(
            "industrial safety handbook OR guideline OR manual",
            max_docs=10,
            doc_type="user_manual_or_guide",
            min_pages=8,
        ),
    ]
    return crawlers


def main():

    crawlers = build_crawlers()
    
    downloader = PDFDownloader(save_dir=f"data/raw/")
    store = MetadataStore("data/metadata.json")

    pipeline = PDFPipeline(
        crawlers=crawlers,
        downloader=downloader,
        filters=is_valid_pdf,
        metadata_store=store
    )

    pipeline.run()

if __name__ == "__main__":
    main()
