import requests
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class PDFDownloader:

    def __init__(self, save_dir):
        self.save_dir = save_dir
        os.makedirs(save_dir, exist_ok=True)
        self.session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def download(self, url, filename, topic):
        try:
            response = self.session.get(url, timeout=30, allow_redirects=True, stream=True)

            if response.status_code != 200:
                return False, f"http_{response.status_code}"

            content_type = response.headers.get("Content-Type", "").lower()
            topic_dir = os.path.join(self.save_dir, topic)
            os.makedirs(topic_dir, exist_ok=True)

            path = os.path.join(topic_dir, filename)
            temp_path = f"{path}.part"
            total_bytes = 0
            header_checked = False
            first_chunk = b""

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if not chunk:
                        continue
                    if not header_checked:
                        first_chunk = chunk
                        header_checked = True
                    total_bytes += len(chunk)
                    f.write(chunk)

            if "pdf" not in content_type and not first_chunk.startswith(b"%PDF"):
                self._safe_remove(temp_path)
                return False, f"not_pdf:{content_type or 'unknown'}"

            if total_bytes < 50_000:
                self._safe_remove(temp_path)
                return False, f"too_small:{total_bytes}"

            os.replace(temp_path, path)
            return True, path

        except requests.exceptions.Timeout:
            return False, "timeout"
        except requests.exceptions.RequestException as exc:
            return False, f"request_error:{exc.__class__.__name__}"
        except OSError as exc:
            return False, f"file_error:{exc.__class__.__name__}"

    def _safe_remove(self, path):
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
