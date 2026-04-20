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

    def test_sync_course_uses_module_file_fallback_when_file_listing_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = CanvasMaterialDownloader(_settings(temp_dir))
            downloader.client = ModuleFallbackClient()

            summary = downloader.sync_course(333)
            output_root = Path(temp_dir) / "333 - COMP-1005 - Term 1 - ICT Fundamentals"

            self.assertEqual(summary.files_total, 2)
            self.assertEqual(summary.files_downloaded, 2)
            self.assertEqual(summary.issues, ["files: used module fallback"])
            self.assertTrue((output_root / "files" / "13915 - ICT Fundamentals Course Introduction 2024.pdf").exists())
            self.assertTrue((output_root / "files" / "13916 - Week 1 Slides.pdf").exists())
            manifest = (output_root / "files.json").read_text(encoding="utf-8")
            self.assertIn('"source": "module_file_fallback"', manifest)


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
