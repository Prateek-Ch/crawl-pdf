from PyPDF2 import PdfReader

def is_valid_pdf(path, min_pages=30):
    try:
        reader = PdfReader(path)
        return len(reader.pages) >= min_pages
    except:
        return False