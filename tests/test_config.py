from pathlib import Path
import os
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from canvas_material_downloader.config import ConfigError, Settings


class SettingsConfigTests(unittest.TestCase):
    def test_no_excluded_course_ids_when_not_set(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "CANVAS_BASE_URL=https://canvas.example.edu",
                        "CANVAS_ACCESS_TOKEN=test-token",
                    ]
                ),
                encoding="utf-8",
            )

            with _clean_env(["CANVAS_EXCLUDED_COURSE_IDS"]):
                settings = Settings.from_env(env_path)

            self.assertEqual(settings.excluded_course_ids, frozenset())

    def test_excluded_course_ids_can_be_overridden_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "CANVAS_BASE_URL=https://canvas.example.edu",
                        "CANVAS_ACCESS_TOKEN=test-token",
                        "CANVAS_EXCLUDED_COURSE_IDS=100, 200",
                    ]
                ),
                encoding="utf-8",
            )

            with _clean_env(["CANVAS_EXCLUDED_COURSE_IDS"]):
                settings = Settings.from_env(env_path)

            self.assertEqual(settings.excluded_course_ids, frozenset({100, 200}))

    def test_invalid_excluded_course_ids_raise_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "CANVAS_BASE_URL=https://canvas.example.edu",
                        "CANVAS_ACCESS_TOKEN=test-token",
                        "CANVAS_EXCLUDED_COURSE_IDS=153,abc",
                    ]
                ),
                encoding="utf-8",
            )

            with _clean_env(["CANVAS_EXCLUDED_COURSE_IDS"]):
                with self.assertRaises(ConfigError):
                    Settings.from_env(env_path)


def _clean_env(keys: list[str]):
    class _ContextManager:
        def __enter__(self):
            self._previous = {key: os.environ.get(key) for key in keys}
            for key in keys:
                os.environ.pop(key, None)

        def __exit__(self, exc_type, exc, tb):
            for key, value in self._previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    return _ContextManager()


if __name__ == "__main__":
    unittest.main()
