"""CLI for self-review."""

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import typer
import yaml

from self_review import db, git, review

app = typer.Typer(help="Generate self-review summaries from git commit history.")

CONFIG_PATH = Path("config.yaml")


def load_config(config_path: Path = CONFIG_PATH) -> dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        typer.echo(f"Config file not found: {config_path}", err=True)
        typer.echo("Create a config.yaml with 'author', 'repos', and 'year' keys.")
        raise typer.Exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


@app.command()
def fetch(
    config: Path = typer.Option(CONFIG_PATH, "--config", "-c", help="Path to config file"),
) -> None:
    """Fetch commits from all configured repos and cache them."""
    cfg = load_config(config)
    author = cfg["author"]
    repos = cfg["repos"]
    year = cfg.get("year", datetime.now().year)

    since = f"{year}-01-01"
    until = f"{year + 1}-01-01"

    db.init_db()

    total_new = 0
    for repo_path in repos:
        repo_path = Path(repo_path).expanduser()
        typer.echo(f"Fetching from {repo_path}...")

        try:
            commits = git.get_commits(repo_path, author, since=since, until=until)
            new_count = 0
            for commit in commits:
                if db.upsert_commit(commit):
                    new_count += 1
            typer.echo(f"  Found {len(commits)} commits, {new_count} new")
            total_new += new_count
        except Exception as e:
            typer.echo(f"  Error: {e}", err=True)

    typer.echo(f"\nTotal: {total_new} new commits cached")


@app.command(name="review")
def review_cmd(
    quarter: str = typer.Option(None, "--quarter", "-q", help="Quarter to review (Q1, Q2, Q3, Q4)"),
    all_year: bool = typer.Option(False, "--all", "-a", help="Review entire year"),
    config: Path = typer.Option(CONFIG_PATH, "--config", "-c", help="Path to config file"),
    force: bool = typer.Option(False, "--force", "-f", help="Force regeneration"),
) -> None:
    """Generate a review summary for a quarter or full year."""
    cfg = load_config(config)
    author = cfg["author"]
    year = cfg.get("year", datetime.now().year)

    db.init_db()

    if all_year:
        periods = [f"{year}"]
        date_ranges = [(f"{year}-01-01", f"{year + 1}-01-01")]
    elif quarter:
        q = quarter.upper()
        periods = [f"{year}-{q}"]
        quarter_ranges = {
            "Q1": (f"{year}-01-01", f"{year}-04-01"),
            "Q2": (f"{year}-04-01", f"{year}-07-01"),
            "Q3": (f"{year}-07-01", f"{year}-10-01"),
            "Q4": (f"{year}-10-01", f"{year + 1}-01-01"),
        }

        if q not in quarter_ranges:
            typer.echo(f"Invalid quarter: {quarter}. Use Q1, Q2, Q3, or Q4.", err=True)
            raise typer.Exit(1)

        start, end = quarter_ranges[q]
        date_ranges = [(start, end)]
    else:
        # Default: all quarters
        periods = [f"{year}-Q1", f"{year}-Q2", f"{year}-Q3", f"{year}-Q4"]
        date_ranges = [
            (f"{year}-01-01", f"{year}-04-01"),
            (f"{year}-04-01", f"{year}-07-01"),
            (f"{year}-07-01", f"{year}-10-01"),
            (f"{year}-10-01", f"{year + 1}-01-01"),
        ]

    for period, (start, end) in zip(periods, date_ranges, strict=False):
        typer.echo(f"\n{'=' * 60}")
        typer.echo(f"Generating review for {period}...")

        # Check cache
        if not force:
            existing = db.get_summary(period)
            if existing:
                typer.echo(f"Using cached summary (generated {existing.generated_at})")
                typer.echo(existing.content)
                continue

        commits = db.get_commits_by_period(start, end, author=author)
        typer.echo(f"Found {len(commits)} commits")

        if not commits:
            typer.echo("No commits found for this period.")
            continue

        summary_text = review.generate_summary(commits, period)
        typer.echo(summary_text)

        # Cache the summary
        summary = db.Summary(
            period=period,
            content=summary_text,
            commit_hashes_json=json.dumps([c.hash for c in commits]),
            generated_at=datetime.now(UTC).isoformat(),
        )
        db.save_summary(summary)


@app.command()
def export(
    output: Path = typer.Option(Path("commits.json"), "--output", "-o", help="Output file"),
    config: Path = typer.Option(CONFIG_PATH, "--config", "-c", help="Path to config file"),
) -> None:
    """Export cached commits to JSON."""
    cfg = load_config(config)
    author = cfg["author"]
    year = cfg.get("year", datetime.now().year)

    db.init_db()

    commits = db.get_commits_by_period(
        f"{year}-01-01",
        f"{year + 1}-01-01",
        author=author,
    )

    data = [
        {
            "hash": c.hash,
            "repo": c.repo,
            "author": c.author,
            "date": c.date,
            "message": c.message,
            "files": json.loads(c.files_json),
        }
        for c in commits
    ]

    with open(output, "w") as f:
        json.dump(data, f, indent=2)

    typer.echo(f"Exported {len(commits)} commits to {output}")


@app.command()
def init(
    config: Path = typer.Option(CONFIG_PATH, "--config", "-c", help="Config file path"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing config"),
) -> None:
    """Initialize a new config.yaml from the example template."""
    if config.exists() and not force:
        typer.echo(f"Config already exists: {config}", err=True)
        typer.echo("Use --force to overwrite, or edit the existing file.")
        raise typer.Exit(1)

    current_year = datetime.now().year
    example_config = f"""\
# Self-review configuration

# Your git author name or email (partial match supported)
author: "your-name"

# Year to review
year: {current_year}

# List of git repositories to include
# Use `self-review discover --update` to auto-populate this list
repos:
  - ~/repos/project-1
  - ~/repos/project-2
"""

    with open(config, "w") as f:
        f.write(example_config)

    typer.echo(f"Created {config}")
    typer.echo("\nNext steps:")
    typer.echo("  1. Edit config.yaml with your author name")
    typer.echo("  2. Run: self-review discover --author 'your-name' --update")
    typer.echo("  3. Run: self-review fetch")
    typer.echo("  4. Run: self-review review")


@app.command()
def discover(
    scan_path: Path = typer.Option(
        Path("~/repos"), "--path", "-p", help="Directory to scan for git repos"
    ),
    org: str | None = typer.Option(None, "--org", "-o", help="GitHub org to filter by (optional)"),
    author: str = typer.Option(..., "--author", "-a", help="Author name/email to search for"),
    year: int | None = typer.Option(
        None, "--year", "-y", help="Year to check commits (default: current year)"
    ),
    update_config: bool = typer.Option(
        False, "--update", "-u", help="Update config.yaml with discovered repos"
    ),
) -> None:
    """Discover repos with your commits, optionally filtered by GitHub org."""
    scan_path = scan_path.expanduser()

    if not scan_path.exists():
        typer.echo(f"Path not found: {scan_path}", err=True)
        raise typer.Exit(1)

    # Default to current year if not specified
    if year is None:
        year = datetime.now().year

    since = f"{year}-01-01"
    until = f"{year + 1}-01-01"

    results: list[tuple[int, str, str, str]] = []  # (count, name, remote, path)
    seen_remotes: set[str] = set()  # Track unique remotes to skip worktrees

    org_msg = f"{org} " if org else ""
    typer.echo(
        f"Scanning {scan_path} for {org_msg}repos with commits from '{author}' in {year}...\n"
    )

    for repo_dir in sorted(scan_path.iterdir()):
        if not repo_dir.is_dir():
            continue
        git_dir = repo_dir / ".git"
        if not git_dir.exists():
            continue

        # Get remote origin
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_dir), "remote", "get-url", "origin"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                continue
            remote = result.stdout.strip()
        except Exception:
            continue

        # Check if it's the target org (if specified)
        if org and org.lower() not in remote.lower():
            continue

        # Skip if we've seen this remote (it's a worktree)
        if remote in seen_remotes:
            continue
        seen_remotes.add(remote)

        # Count commits from author in year (search all branches)
        try:
            result = subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_dir),
                    "log",
                    "--all",
                    f"--author={author}",
                    f"--since={since}",
                    f"--until={until}",
                    "--oneline",
                ],
                capture_output=True,
                text=True,
            )
            count = len([line for line in result.stdout.strip().split("\n") if line])
        except Exception:
            count = 0

        if count > 0:
            results.append((count, repo_dir.name, remote, str(repo_dir)))

    # Sort by commit count descending
    results.sort(key=lambda x: -x[0])

    if not results:
        typer.echo(f"No {org_msg}repos found with commits from '{author}' in {year}.")
        raise typer.Exit(0)

    # Print results
    typer.echo(f"{'Commits':<10} {'Repo':<30} {'Remote'}")
    typer.echo("-" * 80)
    total = 0
    for count, name, remote, _path in results:
        # Shorten remote for display
        short_remote = remote.replace("git@github.com:", "").replace(".git", "")
        typer.echo(f"{count:<10} {name:<30} {short_remote}")
        total += count

    typer.echo("-" * 80)
    typer.echo(f"{total:<10} {'TOTAL':<30} {len(results)} repos")

    if update_config:
        typer.echo("\nUpdating config.yaml...")
        config_path = Path("config.yaml")
        if config_path.exists():
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
        else:
            cfg = {}

        cfg["author"] = author
        cfg["year"] = year
        cfg["repos"] = [path for _, _, _, path in results]

        with open(config_path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

        typer.echo(f"Updated config.yaml with {len(results)} repos.")


if __name__ == "__main__":
    app()
