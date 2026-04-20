from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

from .canvas_api import CanvasApiError, CanvasClient
from .config import Settings
from .fs_utils import course_directory_name, file_destination_name, relative_display


@dataclass(slots=True)
class CourseSyncSummary:
    course_id: int
    course_name: str
    course_dir: Path
    files_total: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0
    file_errors: int = 0
    modules_total: int = 0
    issues: list[str] = field(default_factory=list)


class CanvasMaterialDownloader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = CanvasClient(settings)

    def list_courses(self, *, include_concluded: bool = False) -> list[dict[str, Any]]:
        return self.client.list_courses(include_concluded=include_concluded)

    def sync_course(
        self,
        course_id: int,
        *,
        include_files: bool = True,
        include_modules: bool = True,
    ) -> CourseSyncSummary:
        try:
            course = self.client.get_course(course_id)
        except CanvasApiError as exc:
            if not _is_authorization_error(exc):
                raise

            course = self._find_visible_course(course_id)
            if course is None:
                raise

        return self._sync_course_record(
            course,
            include_files=include_files,
            include_modules=include_modules,
        )

    def sync_all_courses(
        self,
        *,
        include_concluded: bool = False,
        include_files: bool = True,
        include_modules: bool = True,
    ) -> list[CourseSyncSummary]:
        summaries: list[CourseSyncSummary] = []
        for course in self.list_courses(include_concluded=include_concluded):
            summaries.append(
                self._sync_course_record(
                    course,
                    include_files=include_files,
                    include_modules=include_modules,
                )
            )
        return summaries

    def _find_visible_course(self, course_id: int) -> dict[str, Any] | None:
        for include_concluded in (False, True):
            for course in self.client.list_courses(include_concluded=include_concluded):
                if int(course.get("id", -1)) == course_id:
                    return course
        return None

    def _sync_course_record(
        self,
        course: dict[str, Any],
        *,
        include_files: bool,
        include_modules: bool,
    ) -> CourseSyncSummary:
        course_id = int(course["id"])
        course_name = str(course.get("name") or f"Course {course_id}")
        course_root = self.settings.output_dir / course_directory_name(course)
        course_root.mkdir(parents=True, exist_ok=True)

        _write_json(course_root / "course.json", course)

        summary = CourseSyncSummary(
            course_id=course_id,
            course_name=course_name,
            course_dir=course_root,
        )
        modules: list[dict[str, Any]] | None = None

        if include_modules:
            try:
                modules = self.client.list_course_modules(course_id)
                summary.modules_total = len(modules)
                _write_json(
                    course_root / "modules.json",
                    {
                        "course_id": course_id,
                        "synced_at": _utc_now_iso(),
                        "modules": modules,
                    },
                )
            except CanvasApiError as exc:
                if not _is_authorization_error(exc):
                    raise

                summary.issues.append(_format_issue("modules", exc))
                _write_json(
                    course_root / "modules.json",
                    {
                        "course_id": course_id,
                        "synced_at": _utc_now_iso(),
                        "modules": [],
                        "error": str(exc),
                    },
                )

        if include_files:
            summary = self._sync_course_files(course_id, course_root, summary, modules=modules)

        return summary

    def _sync_course_files(
        self,
        course_id: int,
        course_root: Path,
        summary: CourseSyncSummary,
        *,
        modules: list[dict[str, Any]] | None = None,
    ) -> CourseSyncSummary:
        file_source = "course_files"
        warning: str | None = None
        try:
            files = self.client.list_course_files(course_id)
        except CanvasApiError as exc:
            if not _is_authorization_error(exc):
                raise

            files = self._list_module_linked_files(course_id, modules=modules)
            if files is None:
                summary.issues.append(_format_issue("files", exc))
                _write_json(
                    course_root / "files.json",
                    {
                        "course_id": course_id,
                        "synced_at": _utc_now_iso(),
                        "files": [],
                        "error": str(exc),
                    },
                )
                return summary

            file_source = "module_file_fallback"
            warning = "Course file listing was denied, so only files referenced in modules were synced."
            summary.issues.append("files: used module fallback")

        files_dir = course_root / "files"
        files_dir.mkdir(parents=True, exist_ok=True)

        existing_manifest = _load_existing_file_manifest(course_root / "files.json")
        manifest_entries: list[dict[str, Any]] = []

        for file_data in files:
            file_id = int(file_data["id"])
            file_name = file_destination_name(file_data)
            destination = files_dir / file_name
            existing_entry = existing_manifest.get(str(file_id))

            download_url = file_data.get("url")
            if not download_url:
                try:
                    file_data = self.client.get_file(file_id)
                    download_url = file_data.get("url")
                except CanvasApiError as exc:
                    summary.file_errors += 1
                    manifest_entries.append(
                        self._file_manifest_entry(
                            file_data,
                            course_root=course_root,
                            destination=destination,
                            status="error",
                            error=str(exc),
                        )
                    )
                    continue

            if not download_url:
                summary.file_errors += 1
                manifest_entries.append(
                    self._file_manifest_entry(
                        file_data,
                        course_root=course_root,
                        destination=destination,
                        status="error",
                        error="Canvas did not provide a download URL for this file.",
                    )
                )
                continue

            if _should_download(existing_entry, file_data, destination):
                try:
                    self.client.download_file(str(download_url), destination)
                    summary.files_downloaded += 1
                    status = "downloaded"
                    error = None
                except CanvasApiError as exc:
                    summary.file_errors += 1
                    status = "error"
                    error = str(exc)
            else:
                summary.files_skipped += 1
                status = "skipped"
                error = None

            manifest_entries.append(
                self._file_manifest_entry(
                    file_data,
                    course_root=course_root,
                    destination=destination,
                    status=status,
                    error=error,
                )
            )

        summary.files_total = len(files)
        payload: dict[str, Any] = {
            "course_id": course_id,
            "synced_at": _utc_now_iso(),
            "files": manifest_entries,
            "source": file_source,
        }
        if warning:
            payload["warning"] = warning
        _write_json(
            course_root / "files.json",
            payload,
        )
        return summary

    def _file_manifest_entry(
        self,
        file_data: dict[str, Any],
        *,
        course_root: Path,
        destination: Path,
        status: str,
        error: str | None,
    ) -> dict[str, Any]:
        entry = dict(file_data)
        entry["status"] = status
        entry["local_path"] = relative_display(destination, course_root)
        entry["synced_at"] = _utc_now_iso()
        if error:
            entry["error"] = error
        return entry

    def _list_module_linked_files(
        self,
        course_id: int,
        *,
        modules: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]] | None:
        module_records = modules
        if module_records is None:
            try:
                module_records = self.client.list_course_modules(course_id)
            except CanvasApiError as exc:
                if _is_authorization_error(exc):
                    return None
                raise

        seen_file_ids: set[int] = set()
        fallback_files: list[dict[str, Any]] = []

        for module in module_records:
            items = module.get("items")
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict) or item.get("type") != "File":
                    continue

                content_id = item.get("content_id")
                if content_id is None:
                    continue

                try:
                    file_id = int(content_id)
                except (TypeError, ValueError):
                    continue

                if file_id in seen_file_ids:
                    continue

                seen_file_ids.add(file_id)
                fallback_files.append(
                    {
                        "id": file_id,
                        "display_name": item.get("title") or f"File {file_id}",
                    }
                )

        return fallback_files


def _load_existing_file_manifest(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    files = payload["files"] if isinstance(payload, dict) and isinstance(payload.get("files"), list) else payload

    index: dict[str, dict[str, Any]] = {}
    if isinstance(files, list):
        for item in files:
            if isinstance(item, dict) and item.get("id") is not None:
                index[str(item["id"])] = item
    return index


def _should_download(
    existing_entry: dict[str, Any] | None,
    file_data: dict[str, Any],
    destination: Path,
) -> bool:
    if not destination.exists():
        return True
    if not existing_entry:
        return True
    if existing_entry.get("updated_at") != file_data.get("updated_at"):
        return True
    if existing_entry.get("size") != file_data.get("size"):
        return True
    return False


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary_path.replace(path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_authorization_error(error: CanvasApiError) -> bool:
    return error.status_code in {401, 403}


def _format_issue(resource_name: str, error: CanvasApiError) -> str:
    if error.status_code is None:
        return f"{resource_name}: {error}"
    return f"{resource_name}: HTTP {error.status_code}"
