"""Tests for the database module."""

import json
import tempfile
from pathlib import Path

import pytest

from self_review.db import (
    Commit,
    Summary,
    get_commits_by_period,
    get_summary,
    init_db,
    save_summary,
    upsert_commit,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    init_db(db_path)
    yield db_path
    db_path.unlink(missing_ok=True)


def test_init_db(temp_db):
    """Test database initialization."""
    assert temp_db.exists()


def test_upsert_commit(temp_db):
    """Test inserting and updating commits."""
    commit = Commit(
        hash="abc123",
        repo="test-repo",
        author="Test Author",
        date="2025-01-15 10:00:00 -0800",
        message="Test commit message",
        files_json=json.dumps(["file1.py", "file2.py"]),
    )

    # First insert should return True (new)
    assert upsert_commit(commit, temp_db) is True

    # Second insert should return False (exists)
    assert upsert_commit(commit, temp_db) is False


def test_get_commits_by_period(temp_db):
    """Test querying commits by date range."""
    commits = [
        Commit(
            hash="hash1",
            repo="repo1",
            author="John Doe",
            date="2025-01-15 10:00:00 -0800",
            message="Q1 commit",
            files_json="[]",
        ),
        Commit(
            hash="hash2",
            repo="repo1",
            author="John Doe",
            date="2025-04-15 10:00:00 -0800",
            message="Q2 commit",
            files_json="[]",
        ),
        Commit(
            hash="hash3",
            repo="repo2",
            author="Jane Doe",
            date="2025-01-20 10:00:00 -0800",
            message="Another Q1 commit",
            files_json="[]",
        ),
    ]

    for c in commits:
        upsert_commit(c, temp_db)

    # Get Q1 commits
    q1_commits = get_commits_by_period("2025-01-01", "2025-04-01", db_path=temp_db)
    assert len(q1_commits) == 2

    # Filter by author (partial match)
    john_commits = get_commits_by_period("2025-01-01", "2025-12-31", author="John", db_path=temp_db)
    assert len(john_commits) == 2

    # Filter by repo
    repo1_commits = get_commits_by_period("2025-01-01", "2025-12-31", repo="repo1", db_path=temp_db)
    assert len(repo1_commits) == 2


def test_save_and_get_summary(temp_db):
    """Test saving and retrieving summaries."""
    summary = Summary(
        period="2025-Q1",
        content="This is a test summary.",
        commit_hashes_json=json.dumps(["hash1", "hash2"]),
        generated_at="2025-01-20T10:00:00",
    )

    save_summary(summary, temp_db)

    retrieved = get_summary("2025-Q1", temp_db)
    assert retrieved is not None
    assert retrieved.period == "2025-Q1"
    assert retrieved.content == "This is a test summary."

    # Non-existent summary returns None
    assert get_summary("2024-Q4", temp_db) is None
