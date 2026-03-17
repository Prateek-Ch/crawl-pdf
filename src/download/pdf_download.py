import requests
import os

class PDFDownloader:

    def __init__(self, save_dir):
        self.save_dir = save_dir
        os.makedirs(save_dir, exist_ok=True)

    def download(self, url, filename):
        try:
            response = requests.get(url, timeout=10)

            if len(response.content) < 50_000:
                return False

            path = os.path.join(self.save_dir, filename)

            with open(path, "wb") as f:
                f.write(response.content)

            return True

        except:
            return False