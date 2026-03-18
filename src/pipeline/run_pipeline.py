from src.crawlers.arxiv_crawler import ArxivCrawler
from src.download.pdf_download import PDFDownloader
from src.processing.filter import is_valid_pdf
from src.pipeline.pipeline import PDFPipeline
from src.download.metadata import MetadataStore

def main():
    topic = "cs.AI"

    crawler = ArxivCrawler(topic, max_docs=2)
    downloader = PDFDownloader(save_dir=f"data/raw/{topic}")
    store = MetadataStore("data/metadata.json")

    pipeline = PDFPipeline(
        crawler=crawler,
        downloader=downloader,
        filters=is_valid_pdf,
        metadata_store=store
    )

    pipeline.run()

if __name__ == "__main__":
    main()