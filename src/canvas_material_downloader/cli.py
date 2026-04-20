from __future__ import annotations

import argparse
import sys
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
    total_issues = sum(len(summary.issues) for summary in summaries)

    print(
        f"Totals -> courses={total_courses}, assignments={total_assignments}, "
        f"assignment_materials={total_assignment_materials}, files={total_files}, "
        f"downloaded={total_downloaded}, skipped={total_skipped}, "
        f"errors={total_errors}, issues={total_issues}"
    )


def _format_sync_summary(summary: CourseSyncSummary) -> str:
    line = (
        f"[{summary.course_id}] {summary.course_name} -> "
        f"assignments={summary.assignments_total}, "
        f"assignment_materials={summary.assignment_materials_downloaded}, "
        f"downloaded={summary.files_downloaded}, "
        f"skipped={summary.files_skipped}, "
        f"errors={summary.file_errors}, "
        f"modules={summary.modules_total}, "
        f"path={summary.course_dir}"
    )
    if summary.issues:
        line = f"{line}, issues={'; '.join(summary.issues)}"
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

        if event.stage not in {"assignments", "files"}:
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

    def handle(self, event: SyncProgressEvent) -> None:
        if event.stage != "courses":
            return

        if event.action == "planned":
            total = event.total or 0
            self._bar = tqdm(
                total=total,
                desc=f"Courses ({self._parallelism} workers)",
                unit="course",
                file=sys.stdout,
                dynamic_ncols=True,
            )
            return

        if event.action == "advanced" and self._bar is not None:
            self._bar.update(1)
            if event.summary is not None:
                tqdm.write(_format_sync_summary(event.summary), file=sys.stdout)

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None
