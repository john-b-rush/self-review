"""Git operations for fetching commit history."""

import json
import subprocess
from pathlib import Path

from self_review.db import Commit


def get_commits(
    repo_path: Path,
    author: str,
    since: str | None = None,
    until: str | None = None,
) -> list[Commit]:
    """
    Fetch commits from a git repository.

    Args:
        repo_path: Path to the git repository
        author: Git author name or email to filter by
        since: Start date (ISO format, e.g., "2024-01-01")
        until: End date (ISO format, e.g., "2024-12-31")

    Returns:
        List of Commit objects
    """
    repo_path = Path(repo_path).expanduser().resolve()
    repo_name = repo_path.name

    cmd = [
        "git",
        "log",
        "--all",
        "--author",
        author,
        "--pretty=format:%H%x1f%an%x1f%ci%x1f%B%x1e",
    ]

    if since:
        cmd.extend(["--since", since])
    if until:
        cmd.extend(["--until", until])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=repo_path,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Git error in {repo_path}: {result.stderr}")

    commits = []
    raw_commits = result.stdout.strip().split("\x1e")

    for raw in raw_commits:
        raw = raw.strip()
        if not raw:
            continue

        parts = raw.split("\x1f")
        if len(parts) < 4:
            continue

        commit_hash, author_name, date, message = parts[0], parts[1], parts[2], parts[3]

        # Get changed files for this commit
        files_result = subprocess.run(
            ["git", "show", "--pretty=", "--name-only", commit_hash],
            stdout=subprocess.PIPE,
            text=True,
            cwd=repo_path,
        )
        files = [f for f in files_result.stdout.strip().split("\n") if f]

        commits.append(
            Commit(
                hash=commit_hash.strip(),
                repo=repo_name,
                author=author_name.strip(),
                date=date.strip(),
                message=message.strip(),
                files_json=json.dumps(files),
            )
        )

    return commits
