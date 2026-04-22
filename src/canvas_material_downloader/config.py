from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import os

class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class Settings:
    base_url: str
    access_token: str
    output_dir: Path
    timeout_seconds: float
    page_size: int
    excluded_course_ids: frozenset[int] = frozenset()
    user_agent: str = "canvas-material-downloader/0.1.0"

    @classmethod
    def from_env(cls, env_file: str | Path = ".env") -> "Settings":
        load_env_file(Path(env_file))

        base_url = os.getenv("CANVAS_BASE_URL", "").strip().rstrip("/")
        access_token = os.getenv("CANVAS_ACCESS_TOKEN", "").strip()
        output_dir = Path(os.getenv("CANVAS_OUTPUT_DIR", "downloads")).expanduser()

        timeout_raw = os.getenv("CANVAS_TIMEOUT_SECONDS", "60").strip()
        page_size_raw = os.getenv("CANVAS_PAGE_SIZE", "100").strip()
        excluded_raw = os.getenv("CANVAS_EXCLUDED_COURSE_IDS")

        if not base_url:
            raise ConfigError("Missing CANVAS_BASE_URL. Add it to your environment or .env file.")
        if not access_token:
            raise ConfigError("Missing CANVAS_ACCESS_TOKEN. Add it to your environment or .env file.")

        try:
            timeout_seconds = float(timeout_raw)
        except ValueError as exc:
            raise ConfigError(f"CANVAS_TIMEOUT_SECONDS must be numeric, got {timeout_raw!r}.") from exc

        try:
            page_size = int(page_size_raw)
        except ValueError as exc:
            raise ConfigError(f"CANVAS_PAGE_SIZE must be an integer, got {page_size_raw!r}.") from exc

        if timeout_seconds <= 0:
            raise ConfigError("CANVAS_TIMEOUT_SECONDS must be greater than 0.")
        if page_size <= 0:
            raise ConfigError("CANVAS_PAGE_SIZE must be greater than 0.")

        excluded_course_ids = _parse_excluded_course_ids(excluded_raw)

        return cls(
            base_url=base_url,
            access_token=access_token,
            output_dir=output_dir,
            timeout_seconds=timeout_seconds,
            page_size=page_size,
            excluded_course_ids=excluded_course_ids,
        )

    def with_overrides(self, *, output_dir: str | Path | None = None) -> "Settings":
        if output_dir is None:
            return self
        return replace(self, output_dir=Path(output_dir).expanduser())


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key:
            os.environ.setdefault(key, value)


def _parse_excluded_course_ids(raw: str | None) -> frozenset[int]:
    if raw is None:
        return frozenset()

    value = raw.strip()
    if not value:
        return frozenset()

    ids: set[int] = set()
    for token in value.replace(";", ",").split(","):
        item = token.strip()
        if not item:
            continue
        try:
            parsed = int(item)
        except ValueError as exc:
            raise ConfigError(
                f"CANVAS_EXCLUDED_COURSE_IDS must contain comma-separated integers, got {raw!r}."
            ) from exc
        if parsed <= 0:
            raise ConfigError("CANVAS_EXCLUDED_COURSE_IDS must contain positive integers only.")
        ids.add(parsed)

    return frozenset(ids)

