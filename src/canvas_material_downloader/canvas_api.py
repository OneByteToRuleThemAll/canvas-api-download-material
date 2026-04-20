from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import time
import urllib.error
import urllib.parse
import urllib.request

from .config import Settings


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class CanvasApiError(RuntimeError):
    """Raised when the Canvas API returns an error or invalid response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.detail = detail


class CanvasClient:
    def __init__(self, settings: Settings, *, max_retries: int = 3) -> None:
        self.settings = settings
        self.max_retries = max_retries

    def list_courses(self, *, include_concluded: bool = False) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "per_page": self.settings.page_size,
            "include[]": ["term"],
        }
        if not include_concluded:
            params["state[]"] = ["available"]

        payload = self._get_paginated_json("/api/v1/courses", params=params)
        return [course for course in payload if isinstance(course, dict) and course.get("id") is not None]

    def get_course(self, course_id: int) -> dict[str, Any]:
        payload = self._get_json(f"/api/v1/courses/{course_id}")
        if not isinstance(payload, dict):
            raise CanvasApiError(f"Canvas returned unexpected course data for course {course_id}.")
        return payload

    def list_course_files(self, course_id: int) -> list[dict[str, Any]]:
        payload = self._get_paginated_json(
            f"/api/v1/courses/{course_id}/files",
            params={"per_page": self.settings.page_size},
        )
        return [item for item in payload if isinstance(item, dict)]

    def list_course_modules(self, course_id: int) -> list[dict[str, Any]]:
        payload = self._get_paginated_json(
            f"/api/v1/courses/{course_id}/modules",
            params={
                "per_page": self.settings.page_size,
                "include[]": ["items"],
            },
        )
        return [item for item in payload if isinstance(item, dict)]

    def get_file(self, file_id: int) -> dict[str, Any]:
        payload = self._get_json(f"/api/v1/files/{file_id}")
        if not isinstance(payload, dict):
            raise CanvasApiError(f"Canvas returned unexpected file data for file {file_id}.")
        return payload

    def download_file(self, download_url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = destination.with_suffix(f"{destination.suffix}.part")
        request = self._build_request(download_url, accept="*/*")

        try:
            with self._open(request) as response, temporary_path.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            temporary_path.replace(destination)
        finally:
            if temporary_path.exists():
                temporary_path.unlink(missing_ok=True)

    def _get_paginated_json(self, path_or_url: str, params: dict[str, Any] | None = None) -> list[Any]:
        results: list[Any] = []
        next_url = self._build_url(path_or_url, params)

        while next_url:
            payload, headers = self._request_json(next_url)
            if not isinstance(payload, list):
                raise CanvasApiError(f"Canvas returned unexpected paginated payload for {next_url}.")

            results.extend(payload)
            next_url = self._parse_link_header(headers.get("Link", "")).get("next")

        return results

    def _get_json(self, path_or_url: str, params: dict[str, Any] | None = None) -> Any:
        payload, _headers = self._request_json(self._build_url(path_or_url, params))
        return payload

    def _request_json(self, url: str) -> tuple[Any, Any]:
        request = self._build_request(url)
        with self._open(request) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read().decode(charset)
            try:
                return json.loads(body), response.headers
            except json.JSONDecodeError as exc:
                raise CanvasApiError(f"Canvas returned invalid JSON for {url}.") from exc

    def _build_request(self, url: str, *, accept: str = "application/json") -> urllib.request.Request:
        return urllib.request.Request(
            url,
            headers={
                "Accept": accept,
                "Authorization": f"Bearer {self.settings.access_token}",
                "User-Agent": self.settings.user_agent,
            },
            method="GET",
        )

    def _build_url(self, path_or_url: str, params: dict[str, Any] | None = None) -> str:
        parsed = urllib.parse.urlparse(path_or_url)
        if parsed.scheme and parsed.netloc:
            base = path_or_url
        else:
            base = f"{self.settings.base_url}/{path_or_url.lstrip('/')}"

        if not params:
            return base

        filtered_params = {key: value for key, value in params.items() if value is not None}
        query_string = urllib.parse.urlencode(filtered_params, doseq=True)
        separator = "&" if urllib.parse.urlparse(base).query else "?"
        return f"{base}{separator}{query_string}"

    def _open(self, request: urllib.request.Request):
        for attempt in range(1, self.max_retries + 1):
            try:
                return urllib.request.urlopen(request, timeout=self.settings.timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    raise CanvasApiError(
                        "Canvas rejected the access token with HTTP 401. Check CANVAS_ACCESS_TOKEN."
                        ,
                        status_code=exc.code,
                        url=request.full_url,
                    ) from exc

                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt, exc.headers.get("Retry-After")))
                    continue

                detail = _read_http_error_body(exc)
                raise CanvasApiError(
                    f"Canvas request failed with HTTP {exc.code}: {detail}",
                    status_code=exc.code,
                    url=request.full_url,
                    detail=detail,
                ) from exc
            except urllib.error.URLError as exc:
                if attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt, None))
                    continue
                raise CanvasApiError(
                    f"Canvas request failed: {exc.reason}",
                    url=request.full_url,
                ) from exc

        raise CanvasApiError("Canvas request failed after repeated retries.")

    @staticmethod
    def _retry_delay(attempt: int, retry_after_header: str | None) -> float:
        if retry_after_header:
            try:
                return max(1.0, float(retry_after_header))
            except ValueError:
                pass
        return min(8.0, float(2**attempt))

    @staticmethod
    def _parse_link_header(header: str) -> dict[str, str]:
        links: dict[str, str] = {}
        for part in header.split(","):
            section = [piece.strip() for piece in part.split(";") if piece.strip()]
            if len(section) < 2:
                continue

            url_part = section[0]
            if not (url_part.startswith("<") and url_part.endswith(">")):
                continue

            url = url_part[1:-1]
            for attribute in section[1:]:
                if attribute.startswith("rel="):
                    rel = attribute.split("=", 1)[1].strip('"')
                    links[rel] = url
        return links


def _read_http_error_body(error: urllib.error.HTTPError) -> str:
    try:
        body = error.read().decode("utf-8").strip()
    except Exception:
        body = ""
    return body or "no additional detail returned"
