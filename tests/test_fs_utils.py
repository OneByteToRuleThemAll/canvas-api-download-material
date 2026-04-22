from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.fs_utils import (
    assignment_destination_name,
    course_directory_name,
    file_destination_name,
    sanitize_segment,
)


class FsUtilsTests(unittest.TestCase):
    def test_sanitize_segment_removes_windows_invalid_characters(self) -> None:
        self.assertEqual(sanitize_segment('Week 1: Intro/Overview?.pdf'), "Week 1_ Intro_Overview_.pdf")

    def test_sanitize_segment_handles_reserved_names(self) -> None:
        self.assertEqual(sanitize_segment("CON"), "CON_")

    def test_course_directory_name_uses_course_name(self) -> None:
        course = {"id": 42, "course_code": "CS101", "name": "Intro to Programming"}
        self.assertEqual(course_directory_name(course), "Intro to Programming")

    def test_course_directory_name_falls_back_to_course_code(self) -> None:
        course = {"id": 42, "course_code": "CS101", "name": ""}
        self.assertEqual(course_directory_name(course), "CS101")

    def test_course_directory_name_strips_shared_course_prefix(self) -> None:
        course = {
            "id": 356,
            "course_code": "BSc Shared Courses - T4 FT - COMP-4004",
            "name": "BSc Shared Courses - Term 4 (Fast Track) - Digital Marketing",
        }
        self.assertEqual(course_directory_name(course), "Digital Marketing")

    def test_file_destination_name_prefixes_file_id(self) -> None:
        file_data = {"id": 99, "display_name": "Syllabus.pdf"}
        self.assertEqual(file_destination_name(file_data), "Syllabus.pdf")

    def test_file_destination_name_uses_filename_suffix_when_display_name_has_none(self) -> None:
        file_data = {
            "id": 100,
            "display_name": "Slides: Week 1 Intro",
            "filename": "Slides+Week+1+Intro.pdf",
        }
        self.assertEqual(file_destination_name(file_data), "Slides_ Week 1 Intro.pdf")

    def test_file_destination_name_keeps_display_name_suffix_when_present(self) -> None:
        file_data = {
            "id": 101,
            "display_name": "Slides v2.pptx",
            "filename": "Slides+v2.pdf",
        }
        self.assertEqual(file_destination_name(file_data), "Slides v2.pptx")

    def test_file_destination_name_strips_leading_numeric_pair_prefix(self) -> None:
        file_data = {
            "id": 102,
            "display_name": "70_10_AI, Bias, and Fairness.pdf",
        }
        self.assertEqual(file_destination_name(file_data), "AI, Bias, and Fairness.pdf")

    def test_file_destination_name_strips_leading_numeric_pair_prefix_with_multiple_digits(self) -> None:
        file_data = {
            "id": 103,
            "display_name": "120_305_Container Security Basics.pdf",
        }
        self.assertEqual(file_destination_name(file_data), "Container Security Basics.pdf")

    def test_file_destination_name_strips_leading_numeric_pair_prefix_110_10_format(self) -> None:
        file_data = {
            "id": 105,
            "display_name": "110_10_Regulation_vs_Innovation.pdf",
        }
        self.assertEqual(file_destination_name(file_data), "Regulation_vs_Innovation.pdf")

    def test_file_destination_name_strips_leading_slides_prefix(self) -> None:
        file_data = {
            "id": 104,
            "display_name": "Slides_ Business Strategy - Management Implications of Disruption part 2.pdf",
        }
        self.assertEqual(
            file_destination_name(file_data),
            "Business Strategy - Management Implications of Disruption part 2.pdf",
        )

    def test_assignment_destination_name_prefixes_assignment_id(self) -> None:
        assignment = {"id": 12, "name": "Essay 1"}
        self.assertEqual(assignment_destination_name(assignment), "Essay 1.html")


if __name__ == "__main__":
    unittest.main()
