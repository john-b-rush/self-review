"""Claude Code integration for generating summaries."""

import json
import subprocess

from self_review.db import CommentGiven, Commit, PullRequest, ReviewGiven


def generate_summary(
    commits: list[Commit],
    period: str,
    prs: list[PullRequest] | None = None,
    reviews: list[ReviewGiven] | None = None,
    comments: list[CommentGiven] | None = None,
    slack_stats: dict | None = None,
) -> str:
    """
    Generate a summary of work using Claude Code.

    Args:
        commits: List of commits to summarize
        period: Time period label (e.g., "2024-Q1")
        prs: Optional list of PRs authored
        reviews: Optional list of reviews given
        comments: Optional list of comments given
        slack_stats: Optional dict with Slack reaction stats

    Returns:
        Generated summary text
    """
    sections = []

    # Commits section
    if commits:
        commit_text = format_commits_for_prompt(commits)
        sections.append(f"## Git Commits ({len(commits)} total)\n\n{commit_text}")

    # PRs authored section
    if prs:
        pr_text = format_prs_for_prompt(prs)
        sections.append(f"## Pull Requests Authored ({len(prs)} total)\n\n{pr_text}")

    # Reviews given section
    if reviews:
        review_text = format_reviews_for_prompt(reviews)
        sections.append(f"## Code Reviews Given ({len(reviews)} total)\n\n{review_text}")

    # Comments section (only include substantive ones)
    if comments:
        substantive = [c for c in comments if len(c.body) > 50]
        if substantive:
            comment_text = format_comments_for_prompt(substantive)
            sections.append(
                f"## Substantive PR Comments ({len(substantive)} of {len(comments)} total)\n\n{comment_text}"
            )

    # Slack reactions section
    if slack_stats and slack_stats.get("total", 0) > 0:
        slack_text = format_slack_stats_for_prompt(slack_stats)
        sections.append(
            f"## Slack Engagement ({slack_stats['total']} reactions)\n\n{slack_text}"
        )

    if not sections:
        return f"No activity found for {period}."

    all_content = "\n\n".join(sections)

    prompt = f"""Analyze this work activity from {period} and generate a self-review summary.

{all_content}

## Instructions

Generate a performance self-review with these sections:

1. **Summary**: A 2-3 paragraph narrative of the work done, highlighting major themes, projects, and impact.

2. **Key Accomplishments**: Bullet points of specific accomplishments, grouped by theme/project area. Include both code contributions AND collaboration/review work.

3. **Team Contributions**: Highlight significant code review activity, feedback given to teammates, and cross-team collaboration.

4. **Slack Engagement** (if data present): Summarize patterns in emoji reactions - celebrating team wins, supporting colleagues, engaging across different channels. Note top channels and what they represent (e.g., #customerwins = celebrating customer success, #dev-announce = staying engaged with engineering updates).

Focus on impact and outcomes. For code reviews, note patterns like pushing for better testing, code quality improvements, or architectural guidance."""

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


def format_prs_for_prompt(prs: list[PullRequest]) -> str:
    """Format PRs into a readable string for the prompt."""
    lines = []
    for pr in prs:
        status = pr.state
        if pr.merged_at:
            status = "MERGED"

        lines.append(f"**{pr.created_at[:10]}** [{pr.repo}] #{pr.number} ({status})")
        lines.append(f"{pr.title}")
        lines.append(f"+{pr.additions}/-{pr.deletions} in {pr.changed_files} files")

        # Include review feedback received (non-empty)
        reviews = json.loads(pr.reviews_json)
        feedback = [r for r in reviews if r.get("body")]
        if feedback:
            lines.append("Reviews received:")
            for r in feedback[:3]:  # Limit to 3
                body_preview = r["body"][:100].replace("\n", " ")
                lines.append(f"  - {r['author']} ({r['state']}): {body_preview}")

        lines.append("")

    return "\n".join(lines)


def format_reviews_for_prompt(reviews: list[ReviewGiven]) -> str:
    """Format reviews given into a readable string for the prompt."""
    lines = []

    # Group by state for summary
    by_state = {}
    for r in reviews:
        by_state.setdefault(r.state, []).append(r)

    lines.append("Summary:")
    for state, items in sorted(by_state.items()):
        lines.append(f"  - {state}: {len(items)}")
    lines.append("")

    # Show substantive reviews (with body text)
    substantive = [r for r in reviews if len(r.body) > 30]
    if substantive:
        lines.append("Notable review feedback given:")
        for r in substantive[:15]:  # Limit
            body_preview = r.body[:150].replace("\n", " ")
            lines.append(
                f"- **{r.submitted_at[:10]}** #{r.pr_number} ({r.pr_author}): {r.pr_title[:50]}"
            )
            lines.append(f"  [{r.state}] {body_preview}")
            lines.append("")

    return "\n".join(lines)


def format_comments_for_prompt(comments: list[CommentGiven]) -> str:
    """Format comments into a readable string for the prompt."""
    lines = []

    # Sort by length to show most substantive first
    sorted_comments = sorted(comments, key=lambda c: len(c.body), reverse=True)

    for c in sorted_comments[:15]:  # Limit
        body_preview = c.body[:200].replace("\n", " ")
        lines.append(f"- **{c.created_at[:10]}** #{c.pr_number} ({c.pr_author}): {c.pr_title[:50]}")
        lines.append(f"  {body_preview}")
        lines.append("")

    return "\n".join(lines)


def format_slack_stats_for_prompt(stats: dict) -> str:
    """Format Slack reaction stats into a readable string for the prompt."""
    lines = []

    lines.append(f"Total reactions given: {stats['total']}")
    lines.append("")

    if stats.get("by_emoji"):
        lines.append("Top emojis used:")
        for emoji, count in stats["by_emoji"][:3]:
            lines.append(f"  :{emoji}: {count}")
        lines.append("")

    if stats.get("by_channel"):
        lines.append("Most active channels:")
        for channel, count in stats["by_channel"][:3]:
            lines.append(f"  #{channel}: {count}")

    return "\n".join(lines)
