"""Shared pytest fixtures.

The whole test suite is network-free: Ollama is mocked, HTTP is mocked, and
the Temporal pieces run against the in-process test environment. Each test
runs in a temp working dir with a copy of the client profile, so nothing
ever touches clients/brew-lane or the real outbox.
"""
import json
import os
import shutil
import sys

import pytest

# Repo root on the path so tests import social_hustle / web directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("MOCK_LLM", "1")
os.environ.setdefault("BSKY_HANDLE", "")
os.environ.setdefault("BSKY_APP_PASSWORD", "")

SLUG = "brew-lane"


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """A temp cwd containing clients/<slug>/brand.json (copy of the real one)."""
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "clients", SLUG, "brand.json")
    d = tmp_path / "clients" / SLUG
    d.mkdir(parents=True)
    shutil.copy(src, d / "brand.json")
    monkeypatch.chdir(tmp_path)
    yield tmp_path


@pytest.fixture
def brand(workdir):
    import social_hustle as sh
    return sh._load_brand(f"clients/{SLUG}/brand.json")