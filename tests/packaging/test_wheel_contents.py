import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize("include_gitignore", [False, True])
def test_source_archive_wheel_keeps_dashboard_without_frontend_build_inputs(
    tmp_path, include_gitignore
):
    repository = Path(__file__).resolve().parents[2]
    for name in ("pyproject.toml", "hatch_build.py", "README.md", "LICENSE"):
        shutil.copyfile(repository / name, tmp_path / name)
    if include_gitignore:
        shutil.copyfile(repository / ".gitignore", tmp_path / ".gitignore")
    files = {
        "trackio/__init__.py": "",
        "trackio/package.json": '{"version":"0.46.1"}',
        "trackio/frontend/dist/index.html": "<html></html>",
        "trackio/frontend/dist/assets/dashboard.js": "export default 1;",
        "trackio/frontend_templates/page.html": "<html></html>",
        "trackio/frontend/node_modules/esbuild/bin/esbuild": "build tool",
        "trackio/frontend/src/App.svelte": "source only",
        "trackio/frontend/.env.local": "private build settings",
        "trackio/frontend/package-lock.json": "{}",
    }
    for name, content in files.items():
        destination = tmp_path / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content)
    subprocess.run(
        [sys.executable, "-m", "hatchling", "build", "--target", "wheel"],
        cwd=tmp_path,
        env={**os.environ, "SKIP_FRONTEND_BUILD": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    with zipfile.ZipFile(next((tmp_path / "dist").glob("*.whl"))) as archive:
        names = set(archive.namelist())
    assert "trackio/__init__.py" in names
    assert "trackio/frontend_templates/page.html" in names
    frontend = {name for name in names if name.startswith("trackio/frontend/")}
    assert frontend == {
        "trackio/frontend/dist/index.html",
        "trackio/frontend/dist/assets/dashboard.js",
    }
