from __future__ import annotations

from pathlib import Path
import re
from typing import Any


INVALID_SEGMENT_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
MULTISPACE = re.compile(r"\s+")
WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}
MAX_SEGMENT_LENGTH = 120


def sanitize_segment(value: str | None, fallback: str = "untitled") -> str:
    candidate = (value or "").strip()
    candidate = INVALID_SEGMENT_CHARS.sub("_", candidate)
    candidate = MULTISPACE.sub(" ", candidate)
    candidate = candidate.strip(" .")

    if not candidate:
        candidate = fallback

    stem, suffix = _split_suffix(candidate)
    if stem.upper() in WINDOWS_RESERVED_NAMES:
        stem = f"{stem}_"

    allowed_stem_length = max(1, MAX_SEGMENT_LENGTH - len(suffix))
    stem = stem[:allowed_stem_length].rstrip(" .") or fallback
    return f"{stem}{suffix}"


def course_directory_name(course: dict[str, Any]) -> str:
    display_name = (
        course.get("name")
        or course.get("original_name")
        or course.get("course_code")
        or f"course-{course.get('id') or 'unknown'}"
    )
    return sanitize_segment(_strip_course_name_prefix(str(display_name)), fallback="course")


def file_destination_name(file_data: dict[str, Any]) -> str:
    display_name = str(file_data.get("display_name") or "").strip()
    filename = str(file_data.get("filename") or "").strip()

    chosen_name = display_name or filename or "download"
    if display_name and filename:
        chosen_name = _ensure_suffix(display_name, fallback_name=filename)
    chosen_name = _strip_leading_numeric_pair_prefix(chosen_name)
    chosen_name = _strip_leading_slides_prefix(chosen_name)

    return sanitize_segment(chosen_name, fallback="download")


def assignment_destination_name(assignment: dict[str, Any]) -> str:
    assignment_name = assignment.get("name") or "assignment"
    return sanitize_segment(
        f"{assignment_name}.html",
        fallback="assignment.html",
    )


def relative_display(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _split_suffix(filename: str) -> tuple[str, str]:
    if filename.startswith(".") or "." not in filename:
        return filename, ""

    stem, suffix = filename.rsplit(".", 1)
    if not stem or len(suffix) > 10:
        return filename, ""
    return stem, f".{suffix}"


def _ensure_suffix(name: str, *, fallback_name: str) -> str:
    _name_stem, name_suffix = _split_suffix(name)
    _fallback_stem, fallback_suffix = _split_suffix(fallback_name)
    if name_suffix or not fallback_suffix:
        return name
    return f"{name}{fallback_suffix}"


def _strip_leading_numeric_pair_prefix(value: str) -> str:
    cleaned = re.sub(r"^\s*\d{1,3}_\d{1,3}_+", "", value)
    return cleaned.strip() or value


def _strip_leading_slides_prefix(value: str) -> str:
    cleaned = re.sub(r"^\s*slides\s*_+\s*", "", value, flags=re.IGNORECASE)
    return cleaned.strip() or value


def _strip_course_name_prefix(value: str) -> str:
    return re.sub(
        r"^\s*BSc Shared Courses\s*-\s*Term\s*\d+\s*\(Fast Track\)\s*-\s*",
        "",
        value.strip(),
        flags=re.IGNORECASE,
    )
