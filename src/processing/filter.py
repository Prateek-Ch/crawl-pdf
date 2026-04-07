from PyPDF2 import PdfReader

def is_valid_pdf(doc, min_pages=30):
    try:
        reader = PdfReader(doc.path)
        doc.pages = len(reader.pages)
        threshold = doc.min_pages if doc.min_pages is not None else min_pages
        return doc.pages >= threshold
    except:
        return False