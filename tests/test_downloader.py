from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.canvas_api import CanvasApiError
from canvas_material_downloader.config import Settings
from canvas_material_downloader.downloader import CanvasMaterialDownloader, CourseSyncSummary


class CanvasMaterialDownloaderTests(unittest.TestCase):
    def test_sync_course_falls_back_to_visible_course_when_get_course_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ForbiddenGetCourseClient()

            summary = downloader.sync_course(
                333,
                include_assignments=False,
                include_files=False,
                include_modules=False,
            )

            self.assertEqual(summary.course_id, 333)
            self.assertEqual(summary.course_name, "Term 1 - ICT Fundamentals")

    def test_sync_course_records_authorization_issues_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = RestrictedResourceClient()

            summary = downloader.sync_course(333, include_assignments=False)

            self.assertEqual(summary.course_id, 333)
            self.assertEqual(summary.modules_total, 0)
            self.assertEqual(summary.files_total, 0)
            self.assertEqual(summary.issues, ["modules: HTTP 403", "files: HTTP 403"])
            self.assertTrue((Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals" / "modules.json").exists())
            self.assertTrue((Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals" / "files.json").exists())

    def test_sync_course_uses_module_file_fallback_when_file_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ModuleFallbackClient()

            summary = downloader.sync_course(333, include_assignments=False)
            output_root = Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals"

            self.assertEqual(summary.files_total, 2)
            self.assertEqual(summary.files_downloaded, 2)
            self.assertEqual(summary.issues, ["files: used module fallback"])
            self.assertTrue((output_root / "files" / "13915 - ICT Fundamentals Course Introduction 2024.pdf").exists())
            self.assertTrue((output_root / "files" / "13916 - Week 1 Slides.pdf").exists())
            manifest = (output_root / "files.json").read_text(encoding="utf-8")
            self.assertIn('"source": "module_file_fallback"', manifest)

    def test_sync_course_exports_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = AssignmentClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)
            output_root = Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals"

            self.assertEqual(summary.assignments_total, 2)
            self.assertTrue((output_root / "assignments.json").exists())
            self.assertTrue((output_root / "assignments" / "401 - Essay 1.html").exists())
            self.assertTrue((output_root / "assignments" / "402 - Group Project.html").exists())
            manifest = (output_root / "assignments.json").read_text(encoding="utf-8")
            self.assertIn('"source": "course_assignments"', manifest)

    def test_sync_course_records_assignment_issue_when_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = RestrictedAssignmentsClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)

            self.assertEqual(summary.assignments_total, 0)
            self.assertEqual(summary.issues, ["assignments: HTTP 403"])
            self.assertTrue((Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals" / "assignments.json").exists())

    def test_sync_course_downloads_assignment_materials_and_rewrites_html(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = AssignmentMaterialClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)
            output_root = Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals"
            html_export = output_root / "assignments" / "501 - Password Challenge.html"
            csv_material = output_root / "assignments" / "materials" / "501" / "17766 - usernames_passwords.csv"
            doc_material = output_root / "assignments" / "materials" / "501" / "17801 - secret_text.docx"

            self.assertEqual(summary.assignment_materials_downloaded, 2)
            self.assertEqual(summary.assignment_material_errors, 0)
            self.assertTrue(csv_material.exists())
            self.assertTrue(doc_material.exists())
            html_body = html_export.read_text(encoding="utf-8")
            self.assertIn("materials/501/17766 - usernames_passwords.csv", html_body)
            self.assertIn("materials/501/17801 - secret_text.docx", html_body)
            manifest = (output_root / "assignments.json").read_text(encoding="utf-8")
            self.assertIn('"material_files"', manifest)
            self.assertIn('"local_path": "materials/501/17766 - usernames_passwords.csv"', manifest)

    def test_sync_course_emits_progress_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ModuleFallbackClient()
            events = []

            downloader.sync_course(
                333,
                include_assignments=False,
                progress_callback=events.append,
            )

            self.assertEqual(events[0].stage, "course")
            self.assertEqual(events[0].action, "started")
            self.assertTrue(any(event.stage == "modules" and event.action == "finished" for event in events))
            self.assertTrue(any(event.stage == "files" and event.action == "planned" and event.total == 2 for event in events))
            self.assertTrue(any(event.stage == "files" and event.action == "advanced" and event.current == 2 for event in events))
            self.assertEqual(events[-1].stage, "course")
            self.assertEqual(events[-1].action, "finished")

    def test_sync_all_courses_parallel_preserves_input_order_and_reports_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            courses = [
                {"id": 101, "course_code": "A", "name": "Course A"},
                {"id": 202, "course_code": "B", "name": "Course B"},
            ]
            events = []

            def fake_sync_course_record(course, **_kwargs):
                if course["id"] == 101:
                    time.sleep(0.05)
                else:
                    time.sleep(0.01)
                return CourseSyncSummary(
                    course_id=int(course["id"]),
                    course_name=str(course["name"]),
                    course_dir=Path(temp_dir) / str(course["id"]),
                )

            with mock.patch.object(downloader, "list_courses", return_value=courses):
                with mock.patch.object(downloader, "sync_course_record", side_effect=fake_sync_course_record):
                    summaries = downloader.sync_all_courses(parallelism=2, progress_callback=events.append)

            self.assertEqual([summary.course_id for summary in summaries], [101, 202])
            self.assertEqual(events[0].stage, "courses")
            self.assertEqual(events[0].action, "planned")
            advanced_events = [event for event in events if event.stage == "courses" and event.action == "advanced"]
            self.assertEqual(len(advanced_events), 2)
            self.assertEqual(advanced_events[-1].current, 2)


class ForbiddenGetCourseClient:
    def get_course(self, course_id: int):
        raise CanvasApiError("forbidden", status_code=403)

    def list_courses(self, *, include_concluded: bool = False):
        return [
            {
                "id": 333,
                "course_code": "COMP-1005",
                "name": "Term 1 - ICT Fundamentals",
            }
        ]


class RestrictedResourceClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_modules(self, course_id: int):
        raise CanvasApiError("modules forbidden", status_code=403)

    def list_course_files(self, course_id: int):
        raise CanvasApiError("files forbidden", status_code=403)


class ModuleFallbackClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_modules(self, course_id: int):
        return [
            {
                "id": 1,
                "items": [
                    {"type": "File", "content_id": 13915, "title": "ICT Fundamentals Course Introduction 2024.pdf"},
                    {"type": "File", "content_id": 13916, "title": "Week 1 Slides.pdf"},
                    {"type": "File", "content_id": 13915, "title": "Duplicate"},
                ],
            }
        ]

    def list_course_files(self, course_id: int):
        raise CanvasApiError("files forbidden", status_code=403)

    def get_file(self, file_id: int):
        names = {
            13915: "ICT Fundamentals Course Introduction 2024.pdf",
            13916: "Week 1 Slides.pdf",
        }
        return {
            "id": file_id,
            "display_name": names[file_id],
            "size": file_id,
            "updated_at": "2026-04-20T00:00:00Z",
            "url": f"https://canvas.example.edu/files/{file_id}/download",
        }

    def download_file(self, download_url: str, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(download_url, encoding="utf-8")


class AssignmentClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_assignments(self, course_id: int):
        return [
            {
                "id": 401,
                "name": "Essay 1",
                "description": "<p>Write a short essay.</p>",
                "due_at": "2026-05-01T12:00:00Z",
                "submission_types": ["online_upload"],
                "html_url": "https://canvas.example.edu/courses/333/assignments/401",
            },
            {
                "id": 402,
                "name": "Group Project",
                "description": "",
                "submission_types": ["online_upload", "online_text_entry"],
            },
        ]


class RestrictedAssignmentsClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_assignments(self, course_id: int):
        raise CanvasApiError("assignments forbidden", status_code=403)


class AssignmentMaterialClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_assignments(self, course_id: int):
        return [
            {
                "id": 501,
                "name": "Password Challenge",
                "description": (
                    '<p><a title="usernames_passwords.csv" '
                    'href="https://canvas.example.edu/courses/333/files/17766?verifier=abc&amp;wrap=1" '
                    'data-api-endpoint="https://canvas.example.edu/api/v1/courses/333/files/17766">CSV file</a></p>'
                    '<p><a title="secret_text.docx" '
                    'href="https://canvas.example.edu/courses/333/files/17801?verifier=def&amp;wrap=1" '
                    'data-api-endpoint="https://canvas.example.edu/api/v1/courses/333/files/17801">Word file</a></p>'
                ),
            }
        ]

    def get_file(self, file_id: int):
        names = {
            17766: "usernames_passwords.csv",
            17801: "secret_text.docx",
        }
        return {
            "id": file_id,
            "display_name": names[file_id],
            "size": file_id,
            "updated_at": "2026-04-20T00:00:00Z",
            "url": f"https://canvas.example.edu/files/{file_id}/download",
        }

    def download_file(self, download_url: str, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(download_url, encoding="utf-8")


def _settings(output_dir: str) -> Settings:
    return Settings(
        base_url="https://canvas.example.edu",
        access_token="test-token",
        output_dir=Path(output_dir),
        timeout_seconds=30.0,
        page_size=100,
    )


if __name__ == "__main__":
    unittest.main()
