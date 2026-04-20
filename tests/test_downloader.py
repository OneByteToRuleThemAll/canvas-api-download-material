from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.canvas_api import CanvasApiError
from canvas_material_downloader.config import Settings
from canvas_material_downloader.downloader import CanvasMaterialDownloader


class CanvasMaterialDownloaderTests(unittest.TestCase):
    def test_sync_course_falls_back_to_visible_course_when_get_course_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ForbiddenGetCourseClient()

            summary = downloader.sync_course(333, include_files=False, include_modules=False)

            self.assertEqual(summary.course_id, 333)
            self.assertEqual(summary.course_name, "Term 1 - ICT Fundamentals")

    def test_sync_course_records_authorization_issues_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = RestrictedResourceClient()

            summary = downloader.sync_course(333)

            self.assertEqual(summary.course_id, 333)
            self.assertEqual(summary.modules_total, 0)
            self.assertEqual(summary.files_total, 0)
            self.assertEqual(summary.issues, ["modules: HTTP 403", "files: HTTP 403"])
            self.assertTrue((Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals" / "modules.json").exists())
            self.assertTrue((Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals" / "files.json").exists())


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
