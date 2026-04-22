from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.canvas_api import CanvasApiError
from canvas_material_downloader.config import Settings
from canvas_material_downloader.downloader import (
    CanvasMaterialDownloader,
    CourseSyncSummary,
    _module_directory_name,
    _strip_video_edited_markers,
    _strip_module_prefix_from_filename,
    _strip_video_title_prefix,
    _with_module_prefix,
)


COURSE_DIR_NAME = "Term 1 - ICT Fundamentals"


class CanvasMaterialDownloaderTests(unittest.TestCase):
    def test_daemon_thread_pool_executor_uses_daemon_workers(self) -> None:
        executor = CanvasMaterialDownloader._DaemonThreadPoolExecutor(max_workers=2)
        try:
            future = executor.submit(lambda: 42)
            self.assertEqual(future.result(timeout=1), 42)
            self.assertTrue(executor._threads)
            self.assertTrue(all(thread.daemon for thread in executor._threads))
        finally:
            executor.shutdown(wait=True)

    def test_module_directory_name_removes_week_prefix(self) -> None:
        self.assertEqual(
            _module_directory_name("Week 9 - Module 9 Containers and Container Services"),
            "Module 9 - Containers and Container Services",
        )

    def test_module_directory_name_removes_dotted_week_prefix(self) -> None:
        self.assertEqual(
            _module_directory_name("Week 1. Module 1. Course Introduction"),
            "Module 1. Course Introduction",
        )

    def test_strip_module_prefix_from_filename(self) -> None:
        cleaned = _strip_module_prefix_from_filename(
            "Week 9 - Module 9 Containers and Container Services - Containers and Microservices.pdf",
            module_name="Week 9 - Module 9 Containers and Container Services",
        )
        self.assertEqual(cleaned, "Containers and Microservices.pdf")

    def test_strip_video_title_prefix_handles_space_separated_numeric_block(self) -> None:
        cleaned = _strip_video_title_prefix("COMP5007 30 10 Introduction to CloudFormation Basics Edited Video.mp4")
        self.assertEqual(cleaned, "Introduction to CloudFormation Basics Edited Video.mp4")

    def test_strip_video_title_prefix_handles_dotted_numeric_block(self) -> None:
        cleaned = _strip_video_title_prefix("COMP-3004 20.10 History of AI Edited Video.mp4")
        self.assertEqual(cleaned, "History of AI Edited Video.mp4")

    def test_strip_video_edited_markers_removes_edited_video(self) -> None:
        cleaned = _strip_video_edited_markers("Agent Features Edited Video.mp4")
        self.assertEqual(cleaned, "Agent Features.mp4")

    def test_strip_video_edited_markers_removes_edited(self) -> None:
        cleaned = _strip_video_edited_markers("CloudFormation Basics Edited.mp4")
        self.assertEqual(cleaned, "CloudFormation Basics.mp4")

    def test_with_module_prefix_sanitizes_invalid_windows_characters(self) -> None:
        result = _with_module_prefix(
            "Practice Quiz 3 CloudFormation.html",
            module_name="Week 4 - Module 4 CloudFormation: Conditions and Stacks",
        )

        self.assertEqual(
            result,
            "Week 4 - Module 4 CloudFormation_ Conditions and Stacks - Practice Quiz 3 CloudFormation.html",
        )

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
            self.assertTrue((Path(temp_dir) / COURSE_DIR_NAME / "modules.json").exists())
            self.assertTrue((Path(temp_dir) / COURSE_DIR_NAME / "files.json").exists())

    def test_sync_course_uses_module_file_fallback_when_file_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ModuleFallbackClient()

            summary = downloader.sync_course(333, include_assignments=False)
            output_root = Path(temp_dir) / COURSE_DIR_NAME

            self.assertEqual(summary.files_total, 2)
            self.assertEqual(summary.files_downloaded, 2)
            self.assertEqual(summary.issues, ["files: used module fallback"])
            self.assertTrue((output_root / "files" / "Module 1" / "ICT Fundamentals Course Introduction 2024.pdf").exists())
            self.assertTrue((output_root / "files" / "Module 1" / "Week 1 Slides.pdf").exists())
            manifest = (output_root / "files.json").read_text(encoding="utf-8")
            self.assertIn('"source": "module_file_fallback"', manifest)

    def test_sync_course_exports_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = AssignmentClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)
            output_root = Path(temp_dir) / COURSE_DIR_NAME

            self.assertEqual(summary.assignments_total, 2)
            self.assertTrue((output_root / "assignments.json").exists())
            self.assertTrue((output_root / "assignments" / "Essay 1.html").exists())
            self.assertTrue((output_root / "assignments" / "Group Project.html").exists())
            manifest = (output_root / "assignments.json").read_text(encoding="utf-8")
            self.assertIn('"source": "course_assignments"', manifest)

    def test_sync_course_records_assignment_issue_when_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = RestrictedAssignmentsClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)

            self.assertEqual(summary.assignments_total, 0)
            self.assertEqual(summary.issues, ["assignments: HTTP 403"])
            self.assertTrue((Path(temp_dir) / COURSE_DIR_NAME / "assignments.json").exists())

    def test_sync_course_downloads_assignment_materials_and_rewrites_html(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = AssignmentMaterialClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)
            output_root = Path(temp_dir) / COURSE_DIR_NAME
            html_export = output_root / "assignments" / "Password Challenge.html"
            csv_material = output_root / "assignments" / "materials" / "501" / "usernames_passwords.csv"
            doc_material = output_root / "assignments" / "materials" / "501" / "secret_text.docx"

            self.assertEqual(summary.assignment_materials_downloaded, 2)
            self.assertEqual(summary.assignment_material_errors, 0)
            self.assertTrue(csv_material.exists())
            self.assertTrue(doc_material.exists())
            html_body = html_export.read_text(encoding="utf-8")
            self.assertIn("materials/501/usernames_passwords.csv", html_body)
            self.assertIn("materials/501/secret_text.docx", html_body)
            manifest = (output_root / "assignments.json").read_text(encoding="utf-8")
            self.assertIn('"material_files"', manifest)
            self.assertIn('"local_path": "materials/501/usernames_passwords.csv"', manifest)

    def test_sync_course_downloads_videos_and_writes_video_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = VideoClient()

            summary = downloader.sync_course(
                333,
                include_assignments=False,
                include_files=False,
                include_modules=False,
            )
            output_root = Path(temp_dir) / COURSE_DIR_NAME

            self.assertEqual(summary.videos_total, 2)
            self.assertEqual(summary.videos_downloaded, 2)
            self.assertTrue((output_root / "videos" / "Introduction to Cloud Computing.mp4").exists())
            self.assertTrue((output_root / "videos" / "Cloud Economics.mp4").exists())
            manifest = (output_root / "videos.json").read_text(encoding="utf-8")
            self.assertIn('"source": "course_media_objects"', manifest)

    def test_sync_course_appends_mp4_when_video_title_contains_decimal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = DecimalVideoTitleClient()

            summary = downloader.sync_course(
                333,
                include_assignments=False,
                include_files=False,
                include_modules=False,
            )
            output_root = Path(temp_dir) / COURSE_DIR_NAME

            self.assertEqual(summary.videos_downloaded, 1)
            self.assertTrue((output_root / "videos" / "Business Strategy Intro.mp4").exists())

    def test_sync_course_records_video_issue_when_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = RestrictedVideoClient()

            summary = downloader.sync_course(
                333,
                include_assignments=False,
                include_files=False,
                include_modules=False,
            )

            self.assertEqual(summary.videos_total, 0)
            self.assertEqual(summary.issues, ["videos: HTTP 403"])
            self.assertTrue((Path(temp_dir) / COURSE_DIR_NAME / "videos.json").exists())

    def test_sync_course_resolves_studio_module_video_via_lti_media(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = StudioModuleVideoClient()

            summary = downloader.sync_course(
                333,
                include_assignments=False,
                include_files=False,
                include_modules=False,
            )
            output_root = Path(temp_dir) / COURSE_DIR_NAME

            self.assertEqual(summary.videos_total, 1)
            self.assertEqual(summary.videos_downloaded, 1)
            self.assertEqual(summary.videos_skipped, 0)
            self.assertTrue((output_root / "videos" / "Module 2" / "Week 2 Cloud Lecture.mp4").exists())
            manifest = (output_root / "videos.json").read_text(encoding="utf-8")
            self.assertIn('"status": "downloaded"', manifest)
            self.assertIn('"source": "module_video_fallback"', manifest)

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

    def test_list_courses_excludes_configured_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ListedCoursesClient()

            courses = downloader.list_courses()

            self.assertEqual([int(course["id"]) for course in courses], [333])

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

    def list_course_media_objects(self, course_id: int):
        return []


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

    def list_course_media_objects(self, course_id: int):
        return []


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
                "name": "Module 1",
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

    def list_course_media_objects(self, course_id: int):
        return []


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

    def list_course_media_objects(self, course_id: int):
        return []


class RestrictedAssignmentsClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_assignments(self, course_id: int):
        raise CanvasApiError("assignments forbidden", status_code=403)

    def list_course_media_objects(self, course_id: int):
        return []


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

    def list_course_media_objects(self, course_id: int):
        return []


class VideoClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_media_objects(self, course_id: int):
        return [
            {
                "id": 10,
                "media_id": "m-video-1",
                "media_type": "video",
                "title": "Introduction to Cloud Computing",
                "updated_at": "2026-04-20T00:00:00Z",
                "media_sources": [
                    {
                        "url": "https://canvas.example.edu/media/video-1.mp4",
                        "fileExt": "mp4",
                        "size": "1024",
                        "isOriginal": "1",
                    }
                ],
            },
            {
                "id": 11,
                "media_id": "m-video-2",
                "media_type": "video",
                "title": "Cloud Economics",
                "updated_at": "2026-04-20T00:00:00Z",
                "media_sources": [
                    {
                        "url": "https://canvas.example.edu/media/video-2.mp4",
                        "content_type": "video/mp4",
                        "size": "2048",
                        "isOriginal": "1",
                    }
                ],
            },
        ]

    def download_file(self, download_url: str, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(download_url, encoding="utf-8")


class DecimalVideoTitleClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_media_objects(self, course_id: int):
        return [
            {
                "id": 20,
                "media_id": "m-decimal-1",
                "media_type": "video",
                "title": "COMP-3005 10.20 Business Strategy Intro",
                "updated_at": "2026-04-20T00:00:00Z",
                "media_sources": [
                    {
                        "url": "https://canvas.example.edu/media/decimal-title.mp4",
                        "content_type": "video/mp4",
                        "size": "1024",
                        "isOriginal": "1",
                    }
                ],
            }
        ]

    def download_file(self, download_url: str, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(download_url, encoding="utf-8")


class RestrictedVideoClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_media_objects(self, course_id: int):
        raise CanvasApiError("videos forbidden", status_code=403)

    def list_course_modules(self, course_id: int):
        raise CanvasApiError("modules forbidden", status_code=403)


class StudioModuleVideoClient:
    def get_course(self, course_id: int):
        return {
            "id": course_id,
            "course_code": "COMP-1005",
            "name": "Term 1 - ICT Fundamentals",
        }

    def list_course_media_objects(self, course_id: int):
        raise CanvasApiError("videos forbidden", status_code=403)

    def list_course_modules(self, course_id: int):
        return [
            {
                "id": 1,
                "name": "Module 2",
                "items": [
                    {
                        "id": 6959,
                        "type": "ExternalTool",
                        "title": "Week 2 Cloud Lecture",
                        "external_url": "https://shieldhe.instructuremedia.com/lti-app/bare-embed/placeholder",
                    }
                ],
            }
        ]

    def get_lti_media_for_module_item(self, course_id: int, module_item_id: int):
        if course_id != 333 or module_item_id != 6959:
            raise CanvasApiError("unexpected module item", status_code=404)

        return {
            "media": {
                "id": 3574,
                "title": "Week 2 Cloud Lecture",
                "updated_at": "2026-04-20T00:00:00Z",
                "sources": [
                    {
                        "definition": "highest",
                        "mime_type": "video/mp4",
                        "url": "https://canvas.example.edu/media/week2-cloud-lecture.mp4",
                        "size": "1234",
                    }
                ],
            }
        }

    def download_file(self, download_url: str, destination: Path):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(download_url, encoding="utf-8")


class ListedCoursesClient:
    def list_courses(self, *, include_concluded: bool = False):
        return [
            {"id": 153, "course_code": "HUB", "name": "OPIT Hub"},
            {"id": 333, "course_code": "COMP-1005", "name": "ICT Fundamentals"},
            {"id": 412, "course_code": "CAREER", "name": "OPIT Career Center"},
        ]


def _settings(output_dir: str) -> Settings:
    return Settings(
        base_url="https://canvas.example.edu",
        access_token="test-token",
        output_dir=Path(output_dir),
        timeout_seconds=30.0,
        page_size=100,
        excluded_course_ids=frozenset({153, 412}),
    )


if __name__ == "__main__":
    unittest.main()
