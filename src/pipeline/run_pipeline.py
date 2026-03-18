from src.crawlers.arxiv_crawler import ArxivCrawler
from src.download.pdf_download import PDFDownloader
from src.processing.filter import is_valid_pdf
from src.pipeline.pipeline import PDFPipeline
from src.download.metadata import MetadataStore
from src.crawlers.openalex import OpenAlexCrawler
from src.crawlers.internet_archive import InternetArchiveCrawler

def main():

    crawlers = [
    ArxivCrawler("cs.AI", max_docs=2),
    ArxivCrawler("math.PR", max_docs=2),
    OpenAlexCrawler("machine learning", max_docs=2),
    InternetArchiveCrawler("data science", max_docs=2),
    ]
    
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