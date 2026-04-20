from __future__ import annotations

import argparse
import sys
from typing import Sequence

from .canvas_api import CanvasApiError
from .config import ConfigError, Settings
from .downloader import CanvasMaterialDownloader, CourseSyncSummary


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

    sync_course = subparsers.add_parser("sync-course", help="Download metadata and files for one course.")
    sync_course.add_argument("course_id", type=int, help="Canvas course ID.")
    sync_course.add_argument("--skip-files", action="store_true", help="Do not download course files.")
    sync_course.add_argument("--skip-modules", action="store_true", help="Do not fetch module metadata.")

    sync_all = subparsers.add_parser("sync-all", help="Download metadata and files for all visible courses.")
    sync_all.add_argument(
        "--include-concluded",
        action="store_true",
        help="Include concluded courses in the sync.",
    )
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
            summary = downloader.sync_course(
                args.course_id,
                include_files=not args.skip_files,
                include_modules=not args.skip_modules,
            )
            _print_sync_summary(summary)
            return 0

        if args.command == "sync-all":
            summaries = downloader.sync_all_courses(
                include_concluded=args.include_concluded,
                include_files=not args.skip_files,
                include_modules=not args.skip_modules,
            )
            for summary in summaries:
                _print_sync_summary(summary)
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
    line = (
        f"[{summary.course_id}] {summary.course_name} -> "
        f"downloaded={summary.files_downloaded}, "
        f"skipped={summary.files_skipped}, "
        f"errors={summary.file_errors}, "
        f"modules={summary.modules_total}, "
        f"path={summary.course_dir}"
    )
    if summary.issues:
        line = f"{line}, issues={'; '.join(summary.issues)}"
    print(line)


def _print_totals(summaries: list[CourseSyncSummary]) -> None:
    total_courses = len(summaries)
    total_files = sum(summary.files_total for summary in summaries)
    total_downloaded = sum(summary.files_downloaded for summary in summaries)
    total_skipped = sum(summary.files_skipped for summary in summaries)
    total_errors = sum(summary.file_errors for summary in summaries)
    total_issues = sum(len(summary.issues) for summary in summaries)

    print(
        f"Totals -> courses={total_courses}, files={total_files}, "
        f"downloaded={total_downloaded}, skipped={total_skipped}, "
        f"errors={total_errors}, issues={total_issues}"
    )
