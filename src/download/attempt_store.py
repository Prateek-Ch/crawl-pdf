import json
import os
from datetime import datetime, timezone


class AttemptStore:

    RETRYABLE_REASONS = (
        "timeout",
        "request_error:ConnectionError",
        "request_error:ChunkedEncodingError",
        "request_error:SSLError",
        "request_error:ReadTimeout",
        "request_error:ProxyError",
        "request_error:TooManyRedirects",
        "http_429",
        "http_500",
        "http_502",
        "http_503",
        "http_504",
        "http_520",
        "http_521",
        "http_522",
        "http_523",
        "http_524",
        "http_525",
        "http_526",
    )

    def __init__(self, state_path, events_path=None, max_retryable_failures=3):
        self.state_path = state_path
        self.events_path = events_path
        self.max_retryable_failures = max_retryable_failures
        self.state = {}
        self._load_existing()

    def _load_existing(self):
        if not os.path.exists(self.state_path):
            return

        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except (OSError, json.JSONDecodeError):
            loaded = {}

        if isinstance(loaded, dict):
            self.state = loaded

    def should_skip(self, url):
        record = self.state.get(url)
        if not record:
            return False, None

        if record.get("success"):
            return True, "already_succeeded"

        if record.get("permanent_failure"):
            return True, f"previous_{record.get('last_status', 'failure')}"

        failure_count = record.get("retryable_failure_count", 0)
        if failure_count >= self.max_retryable_failures:
            return True, f"retry_limit_reached:{failure_count}"

        return False, None

    def record(self, doc, status, reason=None, run_id=None):
        url = getattr(doc, "url", None)
        if not url:
            return

        record = self.state.get(url, {
            "url": url,
            "success": False,
            "permanent_failure": False,
            "retryable_failure_count": 0,
            "status_counts": {},
        })

        record["title"] = getattr(doc, "title", None)
        record["benchmark"] = getattr(doc, "benchmark", None)
        record["doc_type"] = getattr(doc, "doc_type", None)
        record["source"] = getattr(doc, "source", None)
        record["crawler"] = getattr(doc, "crawler_name", None)
        record["search_query"] = getattr(doc, "search_query", None)
        record["last_status"] = status
        record["last_reason"] = reason
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        record["status_counts"][status] = record["status_counts"].get(status, 0) + 1

        if status == "saved":
            record["success"] = True
            record["permanent_failure"] = False
            record["retryable_failure_count"] = 0
        elif status in {"existing", "rejected", "filtered"}:
            record["permanent_failure"] = True
        elif status == "download_failed":
            if self._is_retryable(reason):
                record["retryable_failure_count"] = record.get("retryable_failure_count", 0) + 1
            else:
                record["permanent_failure"] = True

        self.state[url] = record
        self._append_event(doc, status, reason, run_id)

    def save(self):
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def _append_event(self, doc, status, reason, run_id):
        if not self.events_path:
            return

        os.makedirs(os.path.dirname(self.events_path), exist_ok=True)
        event = {
            "url": getattr(doc, "url", None),
            "title": getattr(doc, "title", None),
            "benchmark": getattr(doc, "benchmark", None),
            "doc_type": getattr(doc, "doc_type", None),
            "source": getattr(doc, "source", None),
            "crawler": getattr(doc, "crawler_name", None),
            "status": status,
            "reason": reason,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with open(self.events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _is_retryable(self, reason):
        if not reason:
            return True

        return any(reason.startswith(prefix) for prefix in self.RETRYABLE_REASONS)
