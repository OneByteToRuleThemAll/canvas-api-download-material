from __future__ import annotations

from html.parser import HTMLParser
import http.cookiejar
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

    def list_course_assignments(self, course_id: int) -> list[dict[str, Any]]:
        payload = self._get_paginated_json(
            f"/api/v1/courses/{course_id}/assignments",
            params={
                "per_page": self.settings.page_size,
                "order_by": "position",
                "include[]": ["all_dates"],
            },
        )
        return [item for item in payload if isinstance(item, dict)]

    def list_course_media_objects(self, course_id: int) -> list[dict[str, Any]]:
        payload = self._get_paginated_json(
            f"/api/v1/courses/{course_id}/media_objects",
            params={
                "per_page": self.settings.page_size,
            },
        )
        return [item for item in payload if isinstance(item, dict)]

    def get_file(self, file_id: int) -> dict[str, Any]:
        payload = self._get_json(f"/api/v1/files/{file_id}")
        if not isinstance(payload, dict):
            raise CanvasApiError(f"Canvas returned unexpected file data for file {file_id}.")
        return payload

    def get_lti_media_for_module_item(self, course_id: int, module_item_id: int) -> dict[str, Any]:
        launch_payload = self._get_json(
            f"/api/v1/courses/{course_id}/external_tools/sessionless_launch",
            params={
                "launch_type": "module_item",
                "module_item_id": module_item_id,
            },
        )
        if not isinstance(launch_payload, dict):
            raise CanvasApiError("Canvas returned invalid sessionless launch payload.")

        launch_url = str(launch_payload.get("url") or "")
        if not launch_url:
            raise CanvasApiError("Canvas did not provide an external tool launch URL.")

        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

        launch_html = self._request_text_with_opener(opener, launch_url)
        form_action, form_fields = _parse_lti_tool_form(launch_html)
        if not form_action:
            raise CanvasApiError("Canvas Studio launch form was not found.")

        action_url = urllib.parse.urljoin(launch_url, form_action)
        post_data = urllib.parse.urlencode(form_fields, doseq=True).encode("utf-8")
        response = self._open_with_opener(
            opener,
            urllib.request.Request(
                action_url,
                data=post_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "text/html",
                    "User-Agent": self.settings.user_agent,
                },
                method="POST",
            ),
        )
        final_url = response.geturl()
        response.close()

        lti_params = _extract_query_param(final_url, "lti_params")
        if not lti_params:
            raise CanvasApiError("Studio launch response did not include lti_params.")

        parsed_final = urllib.parse.urlparse(final_url)
        studio_origin = f"{parsed_final.scheme}://{parsed_final.netloc}"
        lti_media_url = self._build_url(
            f"{studio_origin}/api/media_management/lti_media",
            params={"lti_params": lti_params},
        )
        payload = self._request_json_with_opener(opener, lti_media_url)
        if not isinstance(payload, dict):
            raise CanvasApiError("Studio returned invalid LTI media payload.")
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

    def _request_json_with_opener(self, opener: urllib.request.OpenerDirector, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": self.settings.user_agent,
            },
            method="GET",
        )
        with self._open_with_opener(opener, request) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read().decode(charset)
            try:
                return json.loads(body)
            except json.JSONDecodeError as exc:
                raise CanvasApiError(f"Invalid JSON received from {url}.") from exc

    def _request_text_with_opener(self, opener: urllib.request.OpenerDirector, url: str) -> str:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": self.settings.user_agent,
            },
            method="GET",
        )
        with self._open_with_opener(opener, request) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")

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

    def _open_with_opener(self, opener: urllib.request.OpenerDirector, request: urllib.request.Request):
        for attempt in range(1, self.max_retries + 1):
            try:
                return opener.open(request, timeout=self.settings.timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt, exc.headers.get("Retry-After")))
                    continue

                detail = _read_http_error_body(exc)
                raise CanvasApiError(
                    f"Request failed with HTTP {exc.code}: {detail}",
                    status_code=exc.code,
                    url=request.full_url,
                    detail=detail,
                ) from exc
            except urllib.error.URLError as exc:
                if attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt, None))
                    continue
                raise CanvasApiError(
                    f"Request failed: {exc.reason}",
                    url=request.full_url,
                ) from exc

        raise CanvasApiError("Request failed after repeated retries.")

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


class _LtiToolFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._inside_tool_form = False
        self.action = ""
        self.fields: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value for key, value in attrs if key}
        if tag == "form" and attributes.get("id") == "tool_form":
            self._inside_tool_form = True
            self.action = str(attributes.get("action") or "")
            return

        if not self._inside_tool_form or tag != "input":
            return

        name = attributes.get("name")
        if not name:
            return

        self.fields[str(name)] = str(attributes.get("value") or "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self._inside_tool_form:
            self._inside_tool_form = False


def _parse_lti_tool_form(html_body: str) -> tuple[str, dict[str, str]]:
    parser = _LtiToolFormParser()
    parser.feed(html_body)
    return parser.action, parser.fields


def _extract_query_param(url: str, key: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    values = urllib.parse.parse_qs(parsed.query).get(key)
    if not values:
        return None
    value = values[0]
    if not isinstance(value, str) or not value:
        return None
    return value
