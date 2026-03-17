from src.crawlers.arxiv_crawler import ArxivCrawler

def main():
    crawler = ArxivCrawler(topic="cs.AI", max_docs=10)
    pdf_links = crawler.fetch_pdf_links()
    print(pdf_links)
    
if __name__ == "__main__":
    main()