from __future__ import annotations

import argparse
import shutil
import sys
import threading
from typing import Sequence

from .canvas_api import CanvasApiError
from .config import ConfigError, Settings
from .downloader import CanvasMaterialDownloader, CourseSyncSummary, SyncProgressEvent
from tqdm.auto import tqdm


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="canvas-material",
        description="Download course material from Canvas into a local directory.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file to load before reading config values.",
    )
    parser.add_argument(
        "--output-dir",
        help="Override CANVAS_OUTPUT_DIR for this command.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_courses = subparsers.add_parser("list-courses", help="List Canvas courses visible to the current token.")
    list_courses.add_argument(
        "--include-concluded",
        action="store_true",
        help="Include concluded courses in the course listing.",
    )

    sync_course = subparsers.add_parser("sync-course", help="Download metadata, assignments, and files for one course.")
    sync_course.add_argument("course_id", type=int, help="Canvas course ID.")
    sync_course.add_argument("--skip-assignments", action="store_true", help="Do not fetch course assignments.")
    sync_course.add_argument("--skip-files", action="store_true", help="Do not download course files.")
    sync_course.add_argument("--skip-videos", action="store_true", help="Do not download course videos.")
    sync_course.add_argument("--skip-modules", action="store_true", help="Do not fetch module metadata.")

    sync_all = subparsers.add_parser("sync-all", help="Download metadata, assignments, and files for all visible courses.")
    sync_all.add_argument(
        "--include-concluded",
        action="store_true",
        help="Include concluded courses in the sync.",
    )
    sync_all.add_argument(
        "--parallelism",
        type=_positive_int,
        default=4,
        help="Number of courses to sync in parallel. Use 1 for sequential syncing.",
    )
    sync_all.add_argument("--skip-assignments", action="store_true", help="Do not fetch course assignments.")
    sync_all.add_argument("--skip-files", action="store_true", help="Do not download course files.")
    sync_all.add_argument("--skip-videos", action="store_true", help="Do not download course videos.")
    sync_all.add_argument("--skip-modules", action="store_true", help="Do not fetch module metadata.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        settings = Settings.from_env(args.env_file).with_overrides(output_dir=args.output_dir)
        downloader = CanvasMaterialDownloader(settings)

        if args.command == "list-courses":
            return _run_list_courses(downloader, include_concluded=args.include_concluded)

        if args.command == "sync-course":
            progress_renderer = _SingleCourseProgressRenderer()
            try:
                summary = downloader.sync_course(
                    args.course_id,
                    include_assignments=not args.skip_assignments,
                    include_files=not args.skip_files,
                    include_videos=not args.skip_videos,
                    include_modules=not args.skip_modules,
                    progress_callback=progress_renderer.handle,
                )
            finally:
                progress_renderer.close()
            _print_sync_summary(summary)
            return 0

        if args.command == "sync-all":
            progress_renderer = _BatchCourseProgressRenderer(parallelism=args.parallelism)
            try:
                summaries = downloader.sync_all_courses(
                    include_concluded=args.include_concluded,
                    include_assignments=not args.skip_assignments,
                    include_files=not args.skip_files,
                    include_videos=not args.skip_videos,
                    include_modules=not args.skip_modules,
                    parallelism=args.parallelism,
                    progress_callback=progress_renderer.handle,
                )
            finally:
                progress_renderer.close()
            _print_totals(summaries)
            return 0

        parser.error("Unknown command.")
        return 2
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        return 130
    except (CanvasApiError, ConfigError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _run_list_courses(downloader: CanvasMaterialDownloader, *, include_concluded: bool) -> int:
    courses = downloader.list_courses(include_concluded=include_concluded)
    if not courses:
        print("No courses found.")
        return 0

    for course in courses:
        course_id = course.get("id", "-")
        course_code = course.get("course_code") or "-"
        course_name = course.get("name") or "(unnamed course)"
        print(f"{course_id}\t{course_code}\t{course_name}")
    return 0


def _print_sync_summary(summary: CourseSyncSummary) -> None:
    print(_format_sync_summary(summary))


def _print_totals(summaries: list[CourseSyncSummary]) -> None:
    total_courses = len(summaries)
    total_assignments = sum(summary.assignments_total for summary in summaries)
    total_assignment_materials = sum(summary.assignment_materials_downloaded for summary in summaries)
    total_files = sum(summary.files_total for summary in summaries)
    total_downloaded = sum(summary.files_downloaded for summary in summaries)
    total_skipped = sum(summary.files_skipped for summary in summaries)
    total_errors = sum(summary.file_errors for summary in summaries)
    total_videos = sum(summary.videos_total for summary in summaries)
    total_videos_downloaded = sum(summary.videos_downloaded for summary in summaries)
    total_videos_skipped = sum(summary.videos_skipped for summary in summaries)
    total_video_errors = sum(summary.video_errors for summary in summaries)
    total_issues = sum(len(summary.issues) for summary in summaries)

    print()
    print("Totals")
    print(f"  courses: {total_courses}")
    print(f"  assignments: {total_assignments}")
    print(f"  assignment_materials: {total_assignment_materials}")
    print(f"  files: {total_files}")
    print(f"  downloaded: {total_downloaded}")
    print(f"  skipped: {total_skipped}")
    print(f"  errors: {total_errors}")
    print(f"  videos: {total_videos}")
    print(f"  videos_downloaded: {total_videos_downloaded}")
    print(f"  videos_skipped: {total_videos_skipped}")
    print(f"  video_errors: {total_video_errors}")
    print(f"  issues: {total_issues}")


def _format_sync_summary(summary: CourseSyncSummary) -> str:
    line = "\n".join(
        [
            f"[{summary.course_id}] {summary.course_name}",
            f"  assignments: {summary.assignments_total}",
            f"  assignment_materials: {summary.assignment_materials_downloaded}",
            f"  downloaded: {summary.files_downloaded}",
            f"  skipped: {summary.files_skipped}",
            f"  errors: {summary.file_errors}",
            f"  videos_downloaded: {summary.videos_downloaded}",
            f"  videos_skipped: {summary.videos_skipped}",
            f"  video_errors: {summary.video_errors}",
            f"  modules: {summary.modules_total}",
            f"  path: {summary.course_dir}",
        ]
    )
    if summary.issues:
        line = f"{line}\n  issues: {'; '.join(summary.issues)}"
    return line


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("Value must be at least 1.")
    return parsed


class _SingleCourseProgressRenderer:
    def __init__(self) -> None:
        self._bars: dict[str, tqdm] = {}
        self._current: dict[str, int] = {}
        self._empty_stages: set[str] = set()

    def handle(self, event: SyncProgressEvent) -> None:
        if event.stage == "course" and event.action == "started":
            tqdm.write(f"[{event.course_id}] Syncing {event.course_name}...", file=sys.stdout)
            return

        if event.stage == "modules":
            if event.action == "started":
                tqdm.write(f"[{event.course_id}] Fetching modules...", file=sys.stdout)
            elif event.action == "finished":
                tqdm.write(f"[{event.course_id}] Modules ready: {event.total or 0}", file=sys.stdout)
            return

        if event.stage not in {"assignments", "files", "videos"}:
            return

        if event.action == "planned":
            total = event.total or 0
            if total == 0:
                self._empty_stages.add(event.stage)
                tqdm.write(f"[{event.course_id}] {event.stage.capitalize()}: 0", file=sys.stdout)
                return

            self._current[event.stage] = 0
            self._bars[event.stage] = tqdm(
                total=total,
                desc=f"[{event.course_id}] {event.stage.capitalize()}",
                unit=event.stage[:-1] if event.stage.endswith("s") else event.stage,
                leave=False,
                file=sys.stdout,
                dynamic_ncols=True,
            )
            return

        if event.action == "advanced":
            bar = self._bars.get(event.stage)
            if bar is None:
                return

            previous = self._current.get(event.stage, 0)
            current = event.current or 0
            delta = max(0, current - previous)
            if delta:
                bar.update(delta)
                self._current[event.stage] = current
            return

        if event.action == "finished":
            if event.stage in self._empty_stages:
                self._empty_stages.discard(event.stage)
                return
            bar = self._bars.pop(event.stage, None)
            total = event.total or 0
            if bar is not None:
                bar.close()
            tqdm.write(f"[{event.course_id}] {event.stage.capitalize()} done: {total}", file=sys.stdout)

    def close(self) -> None:
        for bar in self._bars.values():
            bar.close()
        self._bars.clear()
        self._current.clear()
        self._empty_stages.clear()


class _BatchCourseProgressRenderer:
    def __init__(self, *, parallelism: int) -> None:
        self._parallelism = parallelism
        self._bar: tqdm | None = None
        self._lock = threading.Lock()
        self._course_summaries: list[CourseSyncSummary] = []
        self._course_bars: dict[int, tqdm] = {}
        self._course_slots: dict[int, int] = {}
        self._stage_totals: dict[int, dict[str, int]] = {}
        self._stage_current: dict[int, dict[str, int]] = {}
        self._stage_done: dict[int, dict[str, bool]] = {}
        self._free_slots: list[int] = list(range(1, parallelism + 1))

    def handle(self, event: SyncProgressEvent) -> None:
        with self._lock:
            if event.stage == "courses":
                self._handle_courses_event(event)
                return

            if event.stage == "course":
                self._handle_course_lifecycle_event(event)
                return

            if event.stage in {"modules", "assignments", "files", "videos"}:
                self._handle_course_stage_event(event)

    def close(self) -> None:
        with self._lock:
            for bar in self._course_bars.values():
                bar.close()
            self._course_bars.clear()
            self._course_slots.clear()

            if self._bar is not None:
                self._bar.close()
                self._bar = None

            if self._course_summaries:
                print()
                print("Course Summaries")
                for summary in self._course_summaries:
                    print()
                    print(_format_sync_summary(summary))
                self._course_summaries.clear()

    def _handle_courses_event(self, event: SyncProgressEvent) -> None:
        if event.action == "planned":
            total = event.total or 0
            self._bar = tqdm(
                total=total,
                desc=f"Courses ({self._parallelism} workers)",
                unit="course",
                file=sys.stdout,
                dynamic_ncols=True,
                position=0,
            )
            return

        if event.action == "advanced" and self._bar is not None:
            self._bar.update(1)
            if event.summary is not None:
                self._course_summaries.append(event.summary)

    def _handle_course_lifecycle_event(self, event: SyncProgressEvent) -> None:
        if event.action == "started":
            self._start_course_bar(event)
            return

        if event.action == "finished":
            self._finish_course_bar(event)

    def _handle_course_stage_event(self, event: SyncProgressEvent) -> None:
        course_id = event.course_id
        if course_id <= 0 or course_id not in self._course_bars:
            return

        if event.stage == "modules":
            if event.action == "finished":
                stage_done = self._stage_done.setdefault(course_id, {})
                stage_done["modules"] = True
                self._refresh_course_bar(course_id)
            return

        stage_totals = self._stage_totals.setdefault(course_id, {})
        stage_current = self._stage_current.setdefault(course_id, {})
        stage_done = self._stage_done.setdefault(course_id, {})

        if event.action == "planned":
            stage_totals[event.stage] = max(0, event.total or 0)
            stage_current[event.stage] = 0
            stage_done[event.stage] = stage_totals[event.stage] == 0
            self._refresh_course_bar(course_id)
            return

        if event.action == "advanced":
            stage_current[event.stage] = max(0, event.current or 0)
            self._refresh_course_bar(course_id)
            return

        if event.action == "finished":
            total = stage_totals.get(event.stage, max(0, event.total or 0))
            stage_totals[event.stage] = total
            stage_current[event.stage] = total
            stage_done[event.stage] = True
            self._refresh_course_bar(course_id)

    def _start_course_bar(self, event: SyncProgressEvent) -> None:
        if event.course_id in self._course_bars:
            return

        if self._free_slots:
            slot = self._free_slots.pop(0)
        else:
            slot = self._parallelism

        desc = _format_course_progress_label(event.course_id, event.course_name)
        bar = tqdm(
            total=100,
            initial=0,
            desc=desc,
            unit="%",
            leave=False,
            file=sys.stdout,
            dynamic_ncols=True,
            position=slot,
            ncols=_recommended_progress_width(),
            bar_format="{desc:<28} {percentage:3.0f}%|{bar:12}| {postfix}",
        )
        self._course_bars[event.course_id] = bar
        self._course_slots[event.course_id] = slot
        self._stage_totals[event.course_id] = {}
        self._stage_current[event.course_id] = {}
        self._stage_done[event.course_id] = {}
        self._refresh_course_bar(event.course_id)

    def _finish_course_bar(self, event: SyncProgressEvent) -> None:
        course_id = event.course_id
        bar = self._course_bars.pop(course_id, None)
        slot = self._course_slots.pop(course_id, None)
        if slot is not None and slot not in self._free_slots:
            self._free_slots.append(slot)
            self._free_slots.sort()

        if bar is not None:
            bar.n = 100
            bar.refresh()
            bar.close()

        self._stage_totals.pop(course_id, None)
        self._stage_current.pop(course_id, None)
        self._stage_done.pop(course_id, None)

    def _refresh_course_bar(self, course_id: int) -> None:
        bar = self._course_bars.get(course_id)
        if bar is None:
            return

        stage_totals = self._stage_totals.get(course_id, {})
        stage_current = self._stage_current.get(course_id, {})
        stage_done = self._stage_done.get(course_id, {})

        module_weight = 1
        content_stages = ["assignments", "files", "videos"]
        content_total = sum(max(1, stage_totals.get(stage, 0)) for stage in content_stages if stage in stage_totals)
        total_units = module_weight + content_total
        if total_units <= 0:
            total_units = 1

        completed_units = 0
        completed_units += 1 if stage_done.get("modules") else 0
        for stage in content_stages:
            if stage not in stage_totals:
                continue
            stage_total = max(1, stage_totals.get(stage, 0))
            stage_now = min(stage_total, max(0, stage_current.get(stage, 0)))
            completed_units += stage_now

        percent = int((completed_units / total_units) * 100)
        percent = max(0, min(99, percent))

        postfix_items: list[str] = []
        if "files" in stage_totals:
            postfix_items.append(f"F {stage_current.get('files', 0)}/{stage_totals['files']}")
        if "videos" in stage_totals:
            postfix_items.append(f"V {stage_current.get('videos', 0)}/{stage_totals['videos']}")
        if "assignments" in stage_totals:
            postfix_items.append(f"A {stage_current.get('assignments', 0)}/{stage_totals['assignments']}")
        postfix_text = "  ".join(postfix_items) if postfix_items else "starting"

        bar.n = percent
        bar.set_postfix_str(postfix_text, refresh=False)
        bar.refresh()


def _format_course_progress_label(course_id: int, course_name: str) -> str:
    short_name = _short_course_name(course_name)
    return _truncate_label(f"[{course_id}] {short_name}", max_length=28)


def _short_course_name(course_name: str) -> str:
    parts = [part.strip() for part in course_name.split(" - ") if part.strip()]
    if parts:
        return parts[-1]
    return course_name.strip() or "Course"


def _truncate_label(value: str, *, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    if max_length <= 3:
        return value[:max_length]
    return f"{value[: max_length - 3].rstrip()}..."


def _recommended_progress_width() -> int | None:
    width = shutil.get_terminal_size(fallback=(100, 24)).columns
    if width < 60:
        return 60
    return min(width, 120)
