# self-review

Generate self-review summaries from your git commit history using Claude.

Stop dreading performance review season. This tool scans your commits across multiple repos, caches them locally, and uses AI to generate quarterly and yearly summaries highlighting your accomplishments.

## Features

- **Multi-repo support** — Scan commits across all your projects
- **Smart discovery** — Auto-find repos by GitHub org with worktree deduplication
- **SQLite caching** — Fetch once, review anytime
- **Quarterly breakdowns** — Generate Q1-Q4 summaries or full-year reviews
- **Claude-powered** — Uses Claude CLI to generate meaningful summaries

## Installation

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/yourname/self-review.git
cd self-review
uv sync
```

You'll also need [Claude CLI](https://github.com/anthropics/claude-cli) installed and authenticated.

## Quick Start

```bash
# 1. Discover repos with your commits
self-review discover --author "your-name" --org "your-company" --update

# 2. Fetch and cache commits
self-review fetch

# 3. Generate reviews
self-review review --quarter Q1    # Single quarter
self-review review                  # All quarters
self-review review --all            # Full year summary
```

## Configuration

Copy `config.example.yaml` to `config.yaml`:

```yaml
author: "your-name"    # Partial match on git author
year: 2025
repos:
  - ~/repos/project-1
  - ~/repos/project-2
```

Or auto-generate with discover:

```bash
self-review discover --author "jane" --org "acme-corp" --update
```

## Commands

### `discover`

Scan a directory for git repos and find ones with your commits.

```bash
self-review discover --author "jane" --path ~/code --year 2025
self-review discover --author "jane" --org "acme" --update  # Filter by org & save to config
```

### `fetch`

Fetch commits from configured repos and cache to SQLite.

```bash
self-review fetch
self-review fetch --config my-config.yaml
```

### `review`

Generate AI summaries using Claude.

```bash
self-review review                  # All quarters separately
self-review review --quarter Q2     # Specific quarter
self-review review --all            # Full year summary
self-review review --force          # Regenerate (ignore cache)
```

### `export`

Export cached commits to JSON for use with other tools.

```bash
self-review export -o commits.json
```

## How It Works

1. **Fetch** pulls commit metadata (hash, date, message, files) from git and stores in SQLite
2. **Review** queries the cache, formats commits as a prompt, and shells out to `claude -p`
3. Summaries are cached so re-running is instant (use `--force` to regenerate)

## License

MIT
