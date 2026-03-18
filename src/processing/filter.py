from PyPDF2 import PdfReader

def is_valid_pdf(doc, min_pages=30):
    try:
        reader = PdfReader(doc.path)
        doc.pages = len(reader.pages)
        return doc.pages >= min_pages
    except:
        return False