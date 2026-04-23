from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.cli import (
    _format_course_progress_label,
    _short_course_name,
    _truncate_label,
)


class CliFormattingTests(unittest.TestCase):
    def test_short_course_name_uses_last_dash_segment(self) -> None:
        self.assertEqual(
            _short_course_name("BSc Shared Courses - Term 4 (Fast Track) - Digital Marketing"),
            "Digital Marketing",
        )

    def test_format_course_progress_label_includes_id_and_short_name(self) -> None:
        self.assertEqual(
            _format_course_progress_label(356, "BSc Shared Courses - Term 4 (Fast Track) - Digital Marketing"),
            "[356] Digital Marketing",
        )

    def test_truncate_label_adds_ellipsis(self) -> None:
        self.assertEqual(_truncate_label("1234567890", max_length=8), "12345...")


if __name__ == "__main__":
    unittest.main()
