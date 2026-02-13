from __future__ import annotations

from pathlib import Path

from basalt.executors.bmll.backend import _default_bootstrap_content


def test_default_bootstrap_handles_missing_project_root():
    content = _default_bootstrap_content(Path("/does/not/exist"), [])
    assert "if [ -d \"$PROJECT_ROOT\" ]; then" in content
    assert "PROJECT_ROOT not found" in content
    assert "import basalt" in content
    assert "cd \"$HOME\"" in content
