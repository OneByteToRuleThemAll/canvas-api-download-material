from __future__ import annotations

from concurrent.futures import Future, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
import html
from pathlib import Path
import queue
import re
import threading
from typing import Any, Callable
import json
from urllib.parse import urljoin, urlparse

from .canvas_api import CanvasApiError, CanvasClient
from .config import Settings
from .fs_utils import assignment_destination_name, course_directory_name, file_destination_name, relative_display, sanitize_segment


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
    videos_total: int = 0
    videos_downloaded: int = 0
    videos_skipped: int = 0
    video_errors: int = 0
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


@dataclass(slots=True)
class ModuleNamingIndex:
    file_by_id: dict[int, str] = field(default_factory=dict)
    assignment_by_id: dict[int, str] = field(default_factory=dict)
    video_by_item_id: dict[int, str] = field(default_factory=dict)
    video_by_title: dict[str, str] = field(default_factory=dict)


ProgressCallback = Callable[[SyncProgressEvent], None]


class CanvasMaterialDownloader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = CanvasClient(settings)

    def list_courses(self, *, include_concluded: bool = False) -> list[dict[str, Any]]:
        courses = self.client.list_courses(include_concluded=include_concluded)
        excluded = self.settings.excluded_course_ids
        if not excluded:
            return courses

        filtered: list[dict[str, Any]] = []
        for course in courses:
            course_id = _to_int_or_none(course.get("id"))
            if course_id in excluded:
                continue
            filtered.append(course)
        return filtered

    def sync_course(
        self,
        course_id: int,
        *,
        include_assignments: bool = True,
        include_files: bool = True,
        include_videos: bool = True,
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
            include_videos=include_videos,
            include_modules=include_modules,
            progress_callback=progress_callback,
        )

    def sync_course_record(
        self,
        course: dict[str, Any],
        *,
        include_assignments: bool = True,
        include_files: bool = True,
        include_videos: bool = True,
        include_modules: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> CourseSyncSummary:
        return self._sync_course_record(
            course,
            include_assignments=include_assignments,
            include_files=include_files,
            include_videos=include_videos,
            include_modules=include_modules,
            progress_callback=progress_callback,
        )

    def sync_all_courses(
        self,
        *,
        include_concluded: bool = False,
        include_assignments: bool = True,
        include_files: bool = True,
        include_videos: bool = True,
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
                    include_videos=include_videos,
                    include_modules=include_modules,
                    progress_callback=progress_callback,
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

        executor = self._DaemonThreadPoolExecutor(max_workers=parallelism)
        future_to_index: dict[Any, int] = {}
        interrupted = False
        try:
            future_to_index = {
                executor.submit(
                    self.sync_course_record,
                    course,
                    include_assignments=include_assignments,
                    include_files=include_files,
                    include_videos=include_videos,
                    include_modules=include_modules,
                    progress_callback=progress_callback,
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
        except KeyboardInterrupt:
            interrupted = True
            for future in future_to_index:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            if not interrupted:
                executor.shutdown(wait=True)

        return [summary for summary in summaries_by_index if summary is not None]
    class _DaemonThreadPoolExecutor:
        def __init__(self, *, max_workers: int) -> None:
            if max_workers < 1:
                raise ValueError("max_workers must be at least 1")

            self._max_workers = max_workers
            self._tasks: queue.Queue[tuple[Future[Any], Callable[..., Any], tuple[Any, ...], dict[str, Any]] | None] = queue.Queue()
            self._threads: list[threading.Thread] = []
            self._lock = threading.Lock()
            self._shutdown = False
            self._sentinels_enqueued = False

            for index in range(max_workers):
                thread = threading.Thread(
                    target=self._worker,
                    name=f"canvas-sync-{index + 1}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)

        def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
            future: Future[Any] = Future()
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("cannot schedule new futures after shutdown")
                self._tasks.put((future, fn, args, kwargs))
            return future

        def shutdown(self, *, wait: bool, cancel_futures: bool = False) -> None:
            with self._lock:
                self._shutdown = True
                if cancel_futures:
                    self._cancel_pending_futures_locked()
                if not self._sentinels_enqueued:
                    for _ in self._threads:
                        self._tasks.put(None)
                    self._sentinels_enqueued = True

            if wait:
                for thread in self._threads:
                    thread.join()

        def _cancel_pending_futures_locked(self) -> None:
            pending: list[tuple[Future[Any], Callable[..., Any], tuple[Any, ...], dict[str, Any]] | None] = []
            while True:
                try:
                    pending.append(self._tasks.get_nowait())
                except queue.Empty:
                    break

            for item in pending:
                if item is None:
                    continue
                future, _fn, _args, _kwargs = item
                future.cancel()

        def _worker(self) -> None:
            while True:
                item = self._tasks.get()
                if item is None:
                    return

                future, fn, args, kwargs = item
                if not future.set_running_or_notify_cancel():
                    continue

                try:
                    result = fn(*args, **kwargs)
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)

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
        include_videos: bool,
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
        module_naming = ModuleNamingIndex()
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

        naming_modules = modules
        if naming_modules is None:
            try:
                naming_modules = self.client.list_course_modules(course_id)
            except (CanvasApiError, AttributeError):
                naming_modules = []
        module_naming = _build_module_naming_index(naming_modules)

        if include_assignments:
            summary = self._sync_course_assignments(
                course_id,
                course_root,
                summary,
                module_names_by_assignment_id=module_naming.assignment_by_id,
                progress_callback=progress_callback,
            )

        if include_files:
            summary = self._sync_course_files(
                course_id,
                course_root,
                summary,
                modules=modules,
                module_names_by_file_id=module_naming.file_by_id,
                progress_callback=progress_callback,
            )

        if include_videos:
            summary = self._sync_course_videos(
                course_id,
                course_root,
                summary,
                modules=modules,
                module_names_by_video_item_id=module_naming.video_by_item_id,
                module_names_by_video_title=module_naming.video_by_title,
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

    def _sync_course_videos(
        self,
        course_id: int,
        course_root: Path,
        summary: CourseSyncSummary,
        *,
        modules: list[dict[str, Any]] | None = None,
        module_names_by_video_item_id: dict[int, str],
        module_names_by_video_title: dict[str, str],
        progress_callback: ProgressCallback | None = None,
    ) -> CourseSyncSummary:
        source_label = "course_media_objects"
        warning: str | None = None
        try:
            media_objects = self.client.list_course_media_objects(course_id)
        except CanvasApiError as exc:
            if not _is_authorization_error(exc):
                raise

            module_videos = self._list_module_linked_videos(course_id, modules=modules)
            if module_videos is None:
                summary.issues.append(_format_issue("videos", exc))
                _write_json(
                    course_root / "videos.json",
                    {
                        "course_id": course_id,
                        "synced_at": _utc_now_iso(),
                        "videos": [],
                        "error": str(exc),
                    },
                )
                return summary

            media_objects = module_videos
            source_label = "module_video_fallback"
            warning = (
                "Course media object listing was denied, so videos were discovered from module links. "
                "Direct video URLs and supported Canvas Studio links can be mirrored."
            )
            summary.issues.append("videos: used module fallback")

        videos_dir = course_root / "videos"
        videos_dir.mkdir(parents=True, exist_ok=True)
        total_videos = len(media_objects)
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="videos",
                action="planned",
                current=0,
                total=total_videos,
            ),
        )

        existing_manifest = _load_existing_manifest(course_root / "videos.json", collection_key="videos")
        manifest_entries: list[dict[str, Any]] = []

        for index, media_object in enumerate(media_objects, start=1):
            media_object = self._resolve_module_video_media_object(course_id, media_object)
            media_id = str(media_object.get("media_id") or media_object.get("id") or f"video-{index}")
            existing_entry = existing_manifest.get(media_id)

            source = _pick_media_source(media_object)
            download_url = source.get("url") if source else str(media_object.get("external_url") or "")
            source_ext = _media_source_extension(source)
            if not source_ext:
                source_ext = _extension_from_url(download_url)
            title = str(media_object.get("user_entered_title") or media_object.get("title") or f"Video {media_id}")
            display_name = _strip_video_title_prefix(title)
            display_name = _strip_video_edited_markers(display_name)
            if source_ext and not _has_filename_extension(display_name):
                display_name = f"{display_name}{source_ext}"

            module_name: str | None = None
            raw_item_id = media_object.get("id")
            if isinstance(raw_item_id, int):
                module_name = module_names_by_video_item_id.get(raw_item_id)
            if not module_name:
                module_name = module_names_by_video_title.get(_normalize_lookup_text(title))
            display_name = _strip_module_prefix_from_filename(display_name, module_name=module_name)

            module_videos_dir = _module_content_directory(videos_dir, module_name=module_name)
            module_videos_dir.mkdir(parents=True, exist_ok=True)

            video_name = file_destination_name(
                {
                    "id": media_id,
                    "display_name": display_name,
                    "filename": display_name,
                }
            )
            destination = module_videos_dir / video_name

            media_size = _to_int_or_none(source.get("size") if source else None)
            media_updated_at = str(media_object.get("updated_at") or media_object.get("created_at") or "") or None
            comparison_data: dict[str, Any] = {
                "updated_at": media_updated_at,
                "size": media_size,
            }

            status = "skipped"
            error: str | None = None

            if not download_url:
                summary.video_errors += 1
                status = "error"
                error = "Canvas did not provide a downloadable source URL for this media object."
            elif source is None and not _looks_like_direct_download_url(download_url):
                summary.videos_skipped += 1
                status = "linked"
                error = "Module item points to an external player page, not a direct downloadable media file URL."
            elif _should_download(existing_entry, comparison_data, destination):
                try:
                    self.client.download_file(str(download_url), destination)
                    summary.videos_downloaded += 1
                    status = "downloaded"
                except CanvasApiError as exc:
                    summary.video_errors += 1
                    status = "error"
                    error = str(exc)
            else:
                summary.videos_skipped += 1

            manifest_entries.append(
                self._video_manifest_entry(
                    media_object,
                    course_root=course_root,
                    destination=destination,
                    status=status,
                    error=error,
                    source=source,
                )
            )
            _emit_progress(
                progress_callback,
                SyncProgressEvent(
                    course_id=summary.course_id,
                    course_name=summary.course_name,
                    stage="videos",
                    action="advanced",
                    current=index,
                    total=total_videos,
                ),
            )

        summary.videos_total = len(media_objects)
        _write_json(
            course_root / "videos.json",
            {
                "course_id": course_id,
                "synced_at": _utc_now_iso(),
                "videos": manifest_entries,
                "source": source_label,
                **({"warning": warning} if warning else {}),
            },
        )
        _emit_progress(
            progress_callback,
            SyncProgressEvent(
                course_id=summary.course_id,
                course_name=summary.course_name,
                stage="videos",
                action="finished",
                current=total_videos,
                total=total_videos,
            ),
        )
        return summary

    def _resolve_module_video_media_object(self, course_id: int, media_object: dict[str, Any]) -> dict[str, Any]:
        if media_object.get("media_sources"):
            return media_object

        item_id = _to_int_or_none(media_object.get("id"))
        media_id = str(media_object.get("media_id") or "")
        if item_id is None or not media_id.startswith("module-item-"):
            return media_object

        try:
            lti_payload = self.client.get_lti_media_for_module_item(course_id, item_id)
        except (CanvasApiError, AttributeError):
            return media_object

        media = lti_payload.get("media") if isinstance(lti_payload, dict) else None
        if not isinstance(media, dict):
            return media_object

        sources = media.get("sources")
        if not isinstance(sources, list):
            return media_object

        media_sources: list[dict[str, Any]] = []
        for source in sources:
            if not isinstance(source, dict):
                continue

            url = source.get("download_url") or source.get("url")
            if not url:
                continue

            media_sources.append(
                {
                    "url": url,
                    "content_type": source.get("mime_type"),
                    "size": source.get("size"),
                    "isOriginal": str(source.get("definition") or "").lower() in {"highest", "high"},
                }
            )

        if not media_sources:
            return media_object

        enriched = dict(media_object)
        enriched["title"] = media.get("title") or media_object.get("title")
        enriched["updated_at"] = media.get("updated_at") or media.get("created_at") or media_object.get("updated_at")
        enriched["media_sources"] = media_sources
        enriched["lti_media_id"] = media.get("id")
        return enriched

    def _sync_course_assignments(
        self,
        course_id: int,
        course_root: Path,
        summary: CourseSyncSummary,
        *,
        module_names_by_assignment_id: dict[int, str],
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
            assignment_id = int(assignment.get("id", 0))
            module_name = module_names_by_assignment_id.get(assignment_id)
            material_result = self._sync_assignment_materials(
                assignment,
                assignments_dir=assignments_dir,
                module_name=module_name,
            )
            rendered_assignment = dict(assignment)
            rendered_assignment["description"] = material_result.description_html
            destination_name = assignment_destination_name(assignment)
            destination = assignments_dir / _with_module_prefix(destination_name, module_name=module_name)
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
        module_names_by_file_id: dict[int, str],
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

        existing_manifest = _load_existing_manifest(course_root / "files.json", collection_key="files")
        manifest_entries: list[dict[str, Any]] = []

        for index, file_data in enumerate(files, start=1):
            file_id = int(file_data["id"])
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

            file_name = file_destination_name(file_data)
            module_name = module_names_by_file_id.get(file_id)
            file_name = _strip_module_prefix_from_filename(file_name, module_name=module_name)
            module_files_dir = _module_content_directory(files_dir, module_name=module_name)
            module_files_dir.mkdir(parents=True, exist_ok=True)
            destination = module_files_dir / file_name

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

    def _video_manifest_entry(
        self,
        media_object: dict[str, Any],
        *,
        course_root: Path,
        destination: Path,
        status: str,
        error: str | None,
        source: dict[str, Any] | None,
    ) -> dict[str, Any]:
        entry = dict(media_object)
        entry["id"] = str(media_object.get("media_id") or media_object.get("id") or "")
        entry["status"] = status
        entry["local_path"] = relative_display(destination, course_root)
        entry["synced_at"] = _utc_now_iso()
        if source is not None:
            entry["selected_source"] = source
        if error:
            entry["error"] = error
        return entry

    def _sync_assignment_materials(
        self,
        assignment: dict[str, Any],
        *,
        assignments_dir: Path,
        module_name: str | None,
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
            material_name = file_destination_name(file_data)
            material_name = _with_module_prefix(material_name, module_name=module_name)
            destination = materials_dir / material_name
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

    def _list_module_linked_videos(
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

        seen_item_ids: set[int] = set()
        fallback_videos: list[dict[str, Any]] = []

        for module in module_records:
            items = module.get("items")
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                item_type = str(item.get("type") or "")
                if item_type not in {"ExternalTool", "ExternalUrl"}:
                    continue

                item_id_raw = item.get("id")
                try:
                    item_id = int(item_id_raw)
                except (TypeError, ValueError):
                    continue

                if item_id in seen_item_ids:
                    continue

                external_url = str(item.get("external_url") or "")
                if not _looks_like_video_item(item, external_url=external_url):
                    continue

                seen_item_ids.add(item_id)
                fallback_videos.append(
                    {
                        "id": item_id,
                        "media_id": f"module-item-{item_id}",
                        "title": item.get("title") or f"Video {item_id}",
                        "external_url": external_url,
                        "module_item_type": item_type,
                        "html_url": item.get("html_url"),
                    }
                )

        return fallback_videos


def _build_module_naming_index(modules: list[dict[str, Any]]) -> ModuleNamingIndex:
    index = ModuleNamingIndex()

    for module in modules:
        module_name = str(module.get("name") or "").strip()
        if not module_name:
            continue

        items = module.get("items")
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            item_type = str(item.get("type") or "")
            content_id = item.get("content_id")

            if item_type == "File" and content_id is not None:
                try:
                    index.file_by_id.setdefault(int(content_id), module_name)
                except (TypeError, ValueError):
                    pass
                continue

            if item_type == "Assignment" and content_id is not None:
                try:
                    index.assignment_by_id.setdefault(int(content_id), module_name)
                except (TypeError, ValueError):
                    pass
                continue

            if item_type in {"ExternalTool", "ExternalUrl"}:
                external_url = str(item.get("external_url") or "")
                if not _looks_like_video_item(item, external_url=external_url):
                    continue

                item_id = item.get("id")
                if item_id is not None:
                    try:
                        index.video_by_item_id.setdefault(int(item_id), module_name)
                    except (TypeError, ValueError):
                        pass

                title_key = _normalize_lookup_text(str(item.get("title") or ""))
                if title_key:
                    index.video_by_title.setdefault(title_key, module_name)

    return index


def _with_module_prefix(name: str, *, module_name: str | None) -> str:
    if not module_name:
        return sanitize_segment(name, fallback="item")

    trimmed_name = name.strip()
    trimmed_module = _normalize_module_label(module_name.strip())
    prefix = f"{trimmed_module} - "
    if trimmed_name.startswith(prefix):
        combined = trimmed_name
    else:
        combined = f"{prefix}{trimmed_name}"
    return sanitize_segment(combined, fallback=trimmed_name or "item")


def _module_content_directory(base_dir: Path, *, module_name: str | None) -> Path:
    if not module_name:
        return base_dir
    return base_dir / _module_directory_name(module_name)


def _module_directory_name(module_name: str) -> str:
    label = module_name.strip()
    label = _strip_week_prefix(label)
    normalized = _normalize_module_label(label)
    return sanitize_segment(normalized, fallback="Module")


def _strip_module_prefix_from_filename(name: str, *, module_name: str | None) -> str:
    if not module_name:
        return name

    trimmed_name = name.strip()
    if not trimmed_name:
        return name

    candidates = {
        module_name.strip(),
        _strip_week_prefix(module_name.strip()).strip(),
        _normalize_module_label(module_name.strip()),
    }

    lowered_name = trimmed_name.lower()
    for candidate in candidates:
        if not candidate:
            continue

        for candidate_variant in {candidate, sanitize_segment(candidate, fallback="Module")}:
            prefix = f"{candidate_variant} - "
            if lowered_name.startswith(prefix.lower()):
                return trimmed_name[len(prefix):].strip() or trimmed_name

    return trimmed_name


def _strip_week_prefix(value: str) -> str:
    return re.sub(r"^\s*Week\s*\d+\s*[.:-]\s*", "", value, flags=re.IGNORECASE)


def _strip_video_title_prefix(value: str) -> str:
    return re.sub(
        r"^\s*COMP(?:[- ]?)\d{4}\s+\d{1,2}(?:[ .]\d{1,2})+\s+",
        "",
        value.strip(),
        flags=re.IGNORECASE,
    )

def _strip_video_edited_markers(value: str) -> str:
    cleaned = re.sub(r"\bedited\s+video\b", "", value, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bedited\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    cleaned = re.sub(r"\s+\.(?=[A-Za-z0-9]{2,8}$)", ".", cleaned)
    return cleaned.rstrip("-_. ")


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _normalize_module_label(module_name: str) -> str:
    match = re.match(r"^(Module\s+\d+)\s+(?!-)(.+)$", module_name, flags=re.IGNORECASE)
    if not match:
        return module_name
    return f"{match.group(1)} - {match.group(2).strip()}"


def _load_existing_manifest(path: Path, *, collection_key: str) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload[collection_key] if isinstance(payload, dict) and isinstance(payload.get(collection_key), list) else payload

    index: dict[str, dict[str, Any]] = {}
    if isinstance(entries, list):
        for item in entries:
            if isinstance(item, dict) and item.get("id") is not None:
                index[str(item["id"])] = item
    return index


def _should_download(
    existing_entry: dict[str, Any] | None,
    file_data: dict[str, Any],
    destination: Path,
) -> bool:
    if destination.exists():
        return False
    return True


def _should_download_assignment_material(file_data: dict[str, Any], destination: Path) -> bool:
    if not destination.exists():
        return True

    expected_size = file_data.get("size")
    if isinstance(expected_size, int) and expected_size >= 0:
        return destination.stat().st_size != expected_size

    return False


def _looks_like_video_item(item: dict[str, Any], *, external_url: str) -> bool:
    title = str(item.get("title") or "").lower()
    if any(token in title for token in ("video", "lecture", "recording", "live session", "asynchronous")):
        return True

    url = external_url.lower()
    if "instructuremedia.com" in url or "custom_arc_media_id=" in url:
        return True
    if "zoom.us/rec/" in url:
        return True
    return _looks_like_direct_download_url(external_url)


def _looks_like_direct_download_url(url: str) -> bool:
    ext = _extension_from_url(url)
    return ext in {".mp4", ".webm", ".mov", ".m4v", ".avi", ".mkv", ".flv", ".ogg"}


def _extension_from_url(url: str) -> str:
    if not url:
        return ""
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    return suffix if len(suffix) <= 10 else ""


def _has_filename_extension(name: str) -> bool:
    suffix = Path(name).suffix
    if not suffix:
        return False

    # Treat only short, alphanumeric suffixes as real extensions.
    # This avoids false positives for titles like "Lecture 10.20 Intro".
    if " " in suffix:
        return False
    return bool(re.fullmatch(r"\.[A-Za-z0-9]{2,8}", suffix))


def _pick_media_source(media_object: dict[str, Any]) -> dict[str, Any] | None:
    media_sources = media_object.get("media_sources")
    if not isinstance(media_sources, list):
        return None

    candidates = [source for source in media_sources if isinstance(source, dict) and source.get("url")]
    if not candidates:
        return None

    originals = [source for source in candidates if str(source.get("isOriginal", "")).lower() in {"1", "true"}]
    ranked = originals if originals else candidates
    ranked.sort(key=lambda source: _to_int_or_none(source.get("size")) or -1, reverse=True)
    return ranked[0]


def _media_source_extension(source: dict[str, Any] | None) -> str:
    if not source:
        return ""

    file_ext = str(source.get("fileExt") or "").strip().lstrip(".")
    if file_ext:
        return f".{file_ext}"

    content_type = str(source.get("content_type") or "").strip().lower()
    if content_type.endswith("/mp4"):
        return ".mp4"
    if content_type.endswith("/webm"):
        return ".webm"
    if content_type.endswith("/ogg"):
        return ".ogg"
    if content_type.endswith("/x-flv"):
        return ".flv"
    return ""


def _to_int_or_none(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


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
