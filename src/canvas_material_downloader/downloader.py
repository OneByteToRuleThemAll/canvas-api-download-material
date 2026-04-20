from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
import html
from pathlib import Path
import re
from typing import Any, Callable
import json
from urllib.parse import urljoin, urlparse

from .canvas_api import CanvasApiError, CanvasClient
from .config import Settings
from .fs_utils import assignment_destination_name, course_directory_name, file_destination_name, relative_display


@dataclass(slots=True)
class CourseSyncSummary:
    course_id: int
    course_name: str
    course_dir: Path
    assignments_total: int = 0
    assignment_materials_downloaded: int = 0
    assignment_material_errors: int = 0
    files_total: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0
    file_errors: int = 0
    modules_total: int = 0
    issues: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SyncProgressEvent:
    course_id: int
    course_name: str
    stage: str
    action: str
    current: int | None = None
    total: int | None = None
    message: str | None = None
    summary: CourseSyncSummary | None = None


@dataclass(slots=True)
class AssignmentMaterialSyncResult:
    description_html: str
    material_files: list[dict[str, Any]] = field(default_factory=list)
    downloaded_count: int = 0
    error_count: int = 0


ProgressCallback = Callable[[SyncProgressEvent], None]


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
        include_assignments: bool = True,
        include_files: bool = True,
        include_modules: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> CourseSyncSummary:
        try:
            course = self.client.get_course(course_id)
        except CanvasApiError as exc:
            if not _is_authorization_error(exc):
                raise

            course = self._find_visible_course(course_id)
            if course is None:
                raise

        return self.sync_course_record(
            course,
            include_assignments=include_assignments,
            include_files=include_files,
            include_modules=include_modules,
            progress_callback=progress_callback,
        )

    def sync_course_record(
        self,
        course: dict[str, Any],
        *,
        include_assignments: bool = True,
        include_files: bool = True,
        include_modules: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> CourseSyncSummary:
        return self._sync_course_record(
            course,
            include_assignments=include_assignments,
            include_files=include_files,
            include_modules=include_modules,
            progress_callback=progress_callback,
        )

    def sync_all_courses(
        self,
        *,
        include_concluded: bool = False,
        include_assignments: bool = True,
        include_files: bool = True,
        include_modules: bool = True,
        parallelism: int = 1,
        progress_callback: ProgressCallback | None = None,
    ) -> list[CourseSyncSummary]:
        courses = self.list_courses(include_concluded=include_concluded)
        total_courses = len(courses)
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=0,
                course_name="All Courses",
                stage="courses",
                action="planned",
                current=0,
                total=total_courses,
            ),
        )

        if parallelism <= 1 or total_courses <= 1:
            summaries: list[CourseSyncSummary] = []
            for index, course in enumerate(courses, start=1):
                summary = self.sync_course_record(
                    course,
                    include_assignments=include_assignments,
                    include_files=include_files,
                    include_modules=include_modules,
                )
                summaries.append(summary)
                _emit_progress(
                    progress_callback,
                    SyncProgressEvent(
                        course_id=summary.course_id,
                        course_name=summary.course_name,
                        stage="courses",
                        action="advanced",
                        current=index,
                        total=total_courses,
                        summary=summary,
                    ),
                )
            return summaries

        summaries_by_index: list[CourseSyncSummary | None] = [None] * total_courses
        completed = 0

        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            future_to_index = {
                executor.submit(
                    self.sync_course_record,
                    course,
                    include_assignments=include_assignments,
                    include_files=include_files,
                    include_modules=include_modules,
                ): index
                for index, course in enumerate(courses)
            }

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                summary = future.result()
                summaries_by_index[index] = summary
                completed += 1
                _emit_progress(
                    progress_callback,
                    SyncProgressEvent(
                        course_id=summary.course_id,
                        course_name=summary.course_name,
                        stage="courses",
                        action="advanced",
                        current=completed,
                        total=total_courses,
                        summary=summary,
                    ),
                )

        return [summary for summary in summaries_by_index if summary is not None]

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
        include_assignments: bool,
        include_files: bool,
        include_modules: bool,
        progress_callback: ProgressCallback | None = None,
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
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=course_id,
                course_name=course_name,
                stage="course",
                action="started",
            ),
        )

        if include_modules:
            _emit_progress(
                progress_callback,
                SyncProgressEvent(
                    course_id=course_id,
                    course_name=course_name,
                    stage="modules",
                    action="started",
                ),
            )
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
            finally:
                _emit_progress(
                    progress_callback,
                    SyncProgressEvent(
                        course_id=course_id,
                        course_name=course_name,
                        stage="modules",
                        action="finished",
                        total=summary.modules_total,
                    ),
                )

        if include_assignments:
            summary = self._sync_course_assignments(
                course_id,
                course_root,
                summary,
                progress_callback=progress_callback,
            )

        if include_files:
            summary = self._sync_course_files(
                course_id,
                course_root,
                summary,
                modules=modules,
                progress_callback=progress_callback,
            )

        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=course_id,
                course_name=course_name,
                stage="course",
                action="finished",
                summary=summary,
            ),
        )

        return summary

    def _sync_course_assignments(
        self,
        course_id: int,
        course_root: Path,
        summary: CourseSyncSummary,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> CourseSyncSummary:
        try:
            assignments = self.client.list_course_assignments(course_id)
        except CanvasApiError as exc:
            if not _is_authorization_error(exc):
                raise

            summary.issues.append(_format_issue("assignments", exc))
            _write_json(
                course_root / "assignments.json",
                {
                    "course_id": course_id,
                    "synced_at": _utc_now_iso(),
                    "assignments": [],
                    "error": str(exc),
                },
            )
            return summary

        assignments_dir = course_root / "assignments"
        assignments_dir.mkdir(parents=True, exist_ok=True)
        total_assignments = len(assignments)
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="assignments",
                action="planned",
                current=0,
                total=total_assignments,
            ),
        )

        manifest_entries: list[dict[str, Any]] = []
        for index, assignment in enumerate(assignments, start=1):
            material_result = self._sync_assignment_materials(assignment, assignments_dir=assignments_dir)
            rendered_assignment = dict(assignment)
            rendered_assignment["description"] = material_result.description_html
            destination = assignments_dir / assignment_destination_name(assignment)
            _write_text(
                destination,
                _render_assignment_html(rendered_assignment, course_name=summary.course_name),
            )
            manifest_entries.append(
                self._assignment_manifest_entry(
                    assignment,
                    course_root=course_root,
                    destination=destination,
                    material_files=material_result.material_files,
                )
            )
            summary.assignment_materials_downloaded += material_result.downloaded_count
            summary.assignment_material_errors += material_result.error_count
            _emit_progress(
                progress_callback,
                SyncProgressEvent(
                    course_id=summary.course_id,
                    course_name=summary.course_name,
                    stage="assignments",
                    action="advanced",
                    current=index,
                    total=total_assignments,
                ),
            )

        summary.assignments_total = len(assignments)
        _write_json(
            course_root / "assignments.json",
            {
                "course_id": course_id,
                "synced_at": _utc_now_iso(),
                "assignments": manifest_entries,
                "source": "course_assignments",
            },
        )
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="assignments",
                action="finished",
                current=total_assignments,
                total=total_assignments,
            ),
        )
        return summary

    def _sync_course_files(
        self,
        course_id: int,
        course_root: Path,
        summary: CourseSyncSummary,
        *,
        modules: list[dict[str, Any]] | None = None,
        progress_callback: ProgressCallback | None = None,
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
        total_files = len(files)
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="files",
                action="planned",
                current=0,
                total=total_files,
            ),
        )

        existing_manifest = _load_existing_file_manifest(course_root / "files.json")
        manifest_entries: list[dict[str, Any]] = []

        for index, file_data in enumerate(files, start=1):
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
            _emit_progress(
                progress_callback,
                SyncProgressEvent(
                    course_id=summary.course_id,
                    course_name=summary.course_name,
                    stage="files",
                    action="advanced",
                    current=index,
                    total=total_files,
                ),
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
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="files",
                action="finished",
                current=total_files,
                total=total_files,
            ),
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

    def _assignment_manifest_entry(
        self,
        assignment: dict[str, Any],
        *,
        course_root: Path,
        destination: Path,
        material_files: list[dict[str, Any]],
    ) -> dict[str, Any]:
        entry = dict(assignment)
        entry["local_path"] = relative_display(destination, course_root)
        entry["material_files"] = material_files
        entry["synced_at"] = _utc_now_iso()
        return entry

    def _sync_assignment_materials(
        self,
        assignment: dict[str, Any],
        *,
        assignments_dir: Path,
    ) -> AssignmentMaterialSyncResult:
        description_html = str(assignment.get("description") or "")
        file_references = _extract_assignment_file_references(
            description_html,
            canvas_base_url=self.settings.base_url,
        )

        annotatable_attachment_id = assignment.get("annotatable_attachment_id")
        if annotatable_attachment_id is not None:
            try:
                file_references.setdefault(int(annotatable_attachment_id), set())
            except (TypeError, ValueError):
                pass

        if not file_references:
            return AssignmentMaterialSyncResult(description_html=description_html)

        assignment_id = int(assignment.get("id", 0))
        materials_dir = assignments_dir / "materials" / str(assignment_id)
        result = AssignmentMaterialSyncResult(description_html=description_html)

        for file_id, url_variants in sorted(file_references.items()):
            try:
                file_data = self.client.get_file(file_id)
            except CanvasApiError as exc:
                result.error_count += 1
                result.material_files.append(
                    {
                        "id": file_id,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                continue

            download_url = file_data.get("url")
            destination = materials_dir / file_destination_name(file_data)
            status = "skipped"
            error: str | None = None

            if not download_url:
                status = "error"
                error = "Canvas did not provide a download URL for this assignment material."
                result.error_count += 1
            elif _should_download_assignment_material(file_data, destination):
                try:
                    self.client.download_file(str(download_url), destination)
                    result.downloaded_count += 1
                    status = "downloaded"
                except CanvasApiError as exc:
                    status = "error"
                    error = str(exc)
                    result.error_count += 1

            relative_material_path = relative_display(destination, assignments_dir).replace("\\", "/")
            if status != "error":
                result.description_html = _replace_url_variants(
                    result.description_html,
                    url_variants,
                    relative_material_path,
                )

            material_entry = {
                "id": file_id,
                "display_name": file_data.get("display_name"),
                "local_path": relative_material_path,
                "size": file_data.get("size"),
                "status": status,
                "updated_at": file_data.get("updated_at"),
            }
            if error:
                material_entry["error"] = error
            result.material_files.append(material_entry)

        return result

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


def _should_download_assignment_material(file_data: dict[str, Any], destination: Path) -> bool:
    if not destination.exists():
        return True

    expected_size = file_data.get("size")
    if isinstance(expected_size, int) and expected_size >= 0:
        return destination.stat().st_size != expected_size

    return False


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary_path.replace(path)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_authorization_error(error: CanvasApiError) -> bool:
    return error.status_code in {401, 403}


def _format_issue(resource_name: str, error: CanvasApiError) -> str:
    if error.status_code is None:
        return f"{resource_name}: {error}"
    return f"{resource_name}: HTTP {error.status_code}"


def _render_assignment_html(assignment: dict[str, Any], *, course_name: str) -> str:
    title = str(assignment.get("name") or f"Assignment {assignment.get('id', '')}")
    description_html = assignment.get("description") or "<p><em>No description provided by Canvas.</em></p>"
    html_url = assignment.get("html_url")
    metadata_rows = [
        ("Course", course_name),
        ("Assignment ID", assignment.get("id")),
        ("Due At", assignment.get("due_at")),
        ("Unlock At", assignment.get("unlock_at")),
        ("Lock At", assignment.get("lock_at")),
        ("Points Possible", assignment.get("points_possible")),
        ("Submission Types", ", ".join(assignment.get("submission_types", [])) if assignment.get("submission_types") else None),
    ]
    rendered_rows = "\n".join(
        f"<li><strong>{html.escape(label)}:</strong> {html.escape(str(value))}</li>"
        for label, value in metadata_rows
        if value not in (None, "", [])
    )
    canvas_link = (
        f'<p><a href="{html.escape(str(html_url), quote=True)}" target="_blank" rel="noreferrer">Open in Canvas</a></p>'
        if html_url
        else ""
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      line-height: 1.6;
      margin: 0;
      background: #f6f7fb;
      color: #1f2937;
    }}
    main {{
      max-width: 900px;
      margin: 0 auto;
      padding: 32px 20px 64px;
    }}
    .card {{
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 12px;
      padding: 24px;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
    }}
    h1, h2 {{
      line-height: 1.2;
    }}
    ul {{
      padding-left: 20px;
    }}
    a {{
      color: #1d4ed8;
    }}
  </style>
</head>
<body>
  <main>
    <div class="card">
      <h1>{html.escape(title)}</h1>
      <ul>
        {rendered_rows}
      </ul>
      {canvas_link}
      <h2>Description</h2>
      {description_html}
    </div>
  </main>
</body>
</html>
"""


CANVAS_FILE_PATH_RE = re.compile(r"/(?:api/v1/)?(?:courses/\d+/)?files/(\d+)(?:[/?#]|$)")


class _AssignmentFileReferenceParser(HTMLParser):
    def __init__(self, *, canvas_base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.canvas_base_url = canvas_base_url.rstrip("/") + "/"
        self.canvas_netloc = urlparse(canvas_base_url).netloc
        self.file_references: dict[int, set[str]] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for attribute_name, attribute_value in attrs:
            if not attribute_value:
                continue
            if attribute_name not in {"href", "src", "data-api-endpoint", "data-fullsize", "data-download-url"}:
                continue
            self._collect_reference(attribute_value)

    def _collect_reference(self, value: str) -> None:
        resolved_url = urljoin(self.canvas_base_url, value)
        parsed_url = urlparse(resolved_url)
        if parsed_url.netloc != self.canvas_netloc:
            return

        match = CANVAS_FILE_PATH_RE.search(parsed_url.path)
        if not match:
            return

        file_id = int(match.group(1))
        variants = self.file_references.setdefault(file_id, set())
        variants.add(value)
        variants.add(resolved_url)


def _extract_assignment_file_references(
    description_html: str,
    *,
    canvas_base_url: str,
) -> dict[int, set[str]]:
    parser = _AssignmentFileReferenceParser(canvas_base_url=canvas_base_url)
    parser.feed(description_html)
    parser.close()
    return parser.file_references


def _replace_url_variants(raw_html: str, variants: set[str], replacement: str) -> str:
    updated_html = raw_html
    for variant in sorted(variants, key=len, reverse=True):
        updated_html = updated_html.replace(variant, replacement)
        updated_html = updated_html.replace(html.escape(variant, quote=True), replacement)
    return updated_html


def _emit_progress(
    progress_callback: ProgressCallback | None,
    event: SyncProgressEvent,
) -> None:
    if progress_callback is not None:
        progress_callback(event)
