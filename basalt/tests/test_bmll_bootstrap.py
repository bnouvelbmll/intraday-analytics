from __future__ import annotations

from pathlib import Path

from basalt.configuration import BMLLJobConfig
from basalt.executors.bmll.backend import _default_bootstrap_content, ensure_default_bootstrap


def test_default_bootstrap_handles_missing_project_root():
    content = _default_bootstrap_content(Path("/does/not/exist"), [])
    assert "if [ -d \"$PROJECT_ROOT\" ]; then" in content
    assert "PROJECT_ROOT not found" in content
    assert "find_spec('basalt')" in content
    assert "cd \"$HOME\"" in content
    assert "BASALT_DIST=all python -m pip install -e ." in content
    assert "findmnt /home /home/bmll /home/bmll/user" in content
    assert "mount | grep -E '/home|bmll' || true" in content


def test_ensure_default_bootstrap_rewrites_legacy_file(tmp_path):
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir()
    bootstrap = jobs_dir / "bootstrap.sh"
    bootstrap.write_text(
        "#!/usr/bin/env bash\ncd /home/bmll/user/basalt\n",
        encoding="utf-8",
    )
    cfg = BMLLJobConfig(jobs_dir=str(jobs_dir), project_root=str(tmp_path))
    _area, _path, _args = ensure_default_bootstrap(cfg)
    text = bootstrap.read_text(encoding="utf-8")
    assert "BASALT_DIST=all python -m pip install -e ." in text
