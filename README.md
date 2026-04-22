# Canvas Material Downloader

Small Python CLI for exporting student-visible course content from Canvas into one local folder.

The project is built around the Canvas REST API and is meant to be a practical foundation rather than a finished archival system. It already handles a few messy real-world cases, including paginated endpoints, denied course-wide file listings, and per-course manifests.

## Current scope

- List the Canvas courses available to the authenticated user
- Sync one course or all visible courses
- Run `sync-all` in parallel with live course progress output
- Save per-course metadata as `course.json`, `modules.json`, `assignments.json`, and `files.json`
- Save per-course video metadata as `videos.json`
- Export each assignment to a local HTML file
- Download Canvas-hosted files linked inside assignment descriptions when they are still accessible
- Download course files into a local `downloads/` directory
- Download course videos into a local `videos/` directory
- Organize downloaded files and videos into module subfolders when module information is available
- Normalize course/file/video names to reduce noisy prefixes in saved paths
- Skip re-downloading unchanged files when `size` and `updated_at` still match

## Important limitations

- Some Canvas instances deny the course-wide files endpoint. When that happens, the downloader falls back to files referenced inside modules only.
- Some Canvas instances deny the course media objects endpoint. When that happens, the downloader falls back to video links discovered in modules and records them in `videos.json`.
- Concluded or locked courses may still expose metadata while hiding the actual file or assignment material. In those cases you may get assignment HTML and manifests, but not the linked files.
- External links in assignments, such as Google Colab, Kaggle, Hugging Face, or arbitrary websites, are preserved in the exported HTML but are not mirrored locally.
- Video links that point to external player pages (for example Instructure Media LTI pages or some Zoom pages) are indexed in `videos.json`, but may not be directly downloadable as media files.
- If a linked Canvas file no longer exists or is no longer visible to the current token, the manifest will record the error.

## Project layout

```text
.
├── src/canvas_material_downloader/
├── tests/
├── .env.example
├── pyproject.toml
└── README.md
```

Downloaded content is written like this:

```text
downloads/
└── Intro to Biology/
    ├── assignments/
   │   ├── Module 3 - Essay 1.html
    │   └── materials/
   │       └── 4567/
   │           └── Module 3 - project-brief.pdf
    ├── assignments.json
    ├── course.json
    ├── files/
   │   ├── Module 1/
   │   │   └── Syllabus.pdf
   │   └── Module 2/
   │       └── Week 01 Slides.pdf
    ├── files.json
   ├── videos/
   │   ├── Module 1/
   │   │   └── Lecture 01.mp4
   │   └── Module 2/
   │       └── Lecture 02.mp4
   ├── videos.json
    └── modules.json
```

## Setup

1. Create a virtual environment if you want one:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. Copy `.env.example` to `.env` and fill in your Canvas values:

   ```env
   CANVAS_BASE_URL=https://your-school.instructure.com
   CANVAS_ACCESS_TOKEN=your-access-token
   CANVAS_OUTPUT_DIR=downloads
   CANVAS_EXCLUDED_COURSE_IDS=153,412
   ```

   `CANVAS_EXCLUDED_COURSE_IDS` is optional. When omitted or empty, no courses are excluded.
   Set it to a comma-separated list (for example `153,412`) to skip specific courses.

3. Install the package in editable mode:

   ```powershell
   python -m pip install -e .
   ```

## Usage

List available courses:

```powershell
python -m canvas_material_downloader list-courses
```

Sync one course:

```powershell
python -m canvas_material_downloader sync-course 12345
```

Sync every available course:

```powershell
python -m canvas_material_downloader sync-all
```

Control how many courses run in parallel:

```powershell
python -m canvas_material_downloader sync-all --parallelism 6
```

Include concluded courses:

```powershell
python -m canvas_material_downloader sync-all --include-concluded
```

Skip specific parts of the sync:

```powershell
python -m canvas_material_downloader sync-course 12345 --skip-assignments
python -m canvas_material_downloader sync-course 12345 --skip-files
python -m canvas_material_downloader sync-course 12345 --skip-videos
python -m canvas_material_downloader sync-course 12345 --skip-modules
```

After installation you can also use the console script:

```powershell
canvas-material sync-course 12345
```

## What to expect in practice

- `modules.json` is often the most reliable source of structure and file references.
- `files.json` may come either from the course files endpoint or from the module-file fallback path.
- `videos.json` is sourced from Canvas Media Objects and stores download status for each media object.
- `assignments.json` may contain assignment metadata even when the linked materials are no longer downloadable.
- Exported assignment HTML is mainly an offline snapshot of the assignment instructions plus any local rewrites for Canvas-hosted file links that were successfully downloaded.
- `sync-all` shows a live overall progress bar with per-worker course lines.
- Per-course summaries are printed at the end of the run, followed by totals.
- `Ctrl+C` can be used to interrupt sync runs.

## Canvas token note

Most Canvas instances let users create an access token from the account settings page. Once you have that token, place it in `.env` as `CANVAS_ACCESS_TOKEN`. If you are a student of OPIT, you would use `CANVAS_BASE_URL=https://opit.instructure.com`.
