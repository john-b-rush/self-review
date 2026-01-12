"""Claude Code integration for generating summaries."""

import json
import subprocess

from self_review.db import Commit


def generate_summary(commits: list[Commit], period: str) -> str:
    """
    Generate a summary of commits using Claude Code.

    Args:
        commits: List of commits to summarize
        period: Time period label (e.g., "2024-Q1")

    Returns:
        Generated summary text
    """
    if not commits:
        return f"No commits found for {period}."

    # Format commits for the prompt
    commit_text = format_commits_for_prompt(commits)

    prompt = f"""Analyze these git commits from {period} and generate a self-review summary.

## Commits

{commit_text}

## Instructions

Generate two sections:

1. **Summary**: A 2-3 paragraph narrative of the work done, highlighting major themes, projects, and impact.

2. **Key Accomplishments**: Bullet points of specific accomplishments, grouped by theme/project area.

Focus on impact and outcomes, not just listing changes. This is for a performance self-review."""

    # Shell out to claude
    result = subprocess.run(
        ["claude", "-p", prompt],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Claude error: {result.stderr}")

    return result.stdout.strip()


def format_commits_for_prompt(commits: list[Commit]) -> str:
    """Format commits into a readable string for the prompt."""
    lines = []
    for c in commits:
        files = json.loads(c.files_json)
        files_str = ", ".join(files[:5])
        if len(files) > 5:
            files_str += f" (+{len(files) - 5} more)"

        lines.append(f"**{c.date[:10]}** [{c.repo}]")
        lines.append(f"{c.message}")
        lines.append(f"Files: {files_str}")
        lines.append("")

    return "\n".join(lines)
