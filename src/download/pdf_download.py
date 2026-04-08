import requests
import os


class PDFDownloader:

    def __init__(self, save_dir):
        self.save_dir = save_dir
        os.makedirs(save_dir, exist_ok=True)

    def download(self, url, filename, topic):
        try:
            response = requests.get(url, timeout=20, allow_redirects=True)

            if response.status_code != 200:
                return False, f"http_{response.status_code}"

            content_type = response.headers.get("Content-Type", "").lower()
            if "pdf" not in content_type and not response.content.startswith(b"%PDF"):
                return False, f"not_pdf:{content_type or 'unknown'}"

            if len(response.content) < 50_000:
                return False, f"too_small:{len(response.content)}"
            
            topic_dir = os.path.join(self.save_dir, topic)
            os.makedirs(topic_dir, exist_ok=True)

            path = os.path.join(topic_dir, filename)

            with open(path, "wb") as f:
                f.write(response.content)

            return True, path

        except requests.exceptions.Timeout:
            return False, "timeout"
        except requests.exceptions.RequestException as exc:
            return False, f"request_error:{exc.__class__.__name__}"
        except OSError as exc:
            return False, f"file_error:{exc.__class__.__name__}"
