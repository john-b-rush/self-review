"""SQLite database for caching commits and summaries."""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class Commit:
    """Represents a git commit."""

    hash: str
    repo: str
    author: str
    date: str
    message: str
    files_json: str  # JSON list of changed files


@dataclass
class Summary:
    """Represents a generated summary for a time period."""

    period: str  # e.g., "2024-Q1", "2024"
    content: str
    commit_hashes_json: str  # JSON list of commit hashes included
    generated_at: str


DB_PATH = Path("self_review.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Get a database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    """Initialize the database schema."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commits (
            hash TEXT PRIMARY KEY,
            repo TEXT NOT NULL,
            author TEXT NOT NULL,
            date TEXT NOT NULL,
            message TEXT NOT NULL,
            files_json TEXT DEFAULT '[]',
            fetched_at TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_commits_repo ON commits(repo)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_commits_date ON commits(date)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_commits_author ON commits(author)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period TEXT NOT NULL,
            content TEXT NOT NULL,
            commit_hashes_json TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            UNIQUE(period)
        )
    """)

    conn.commit()
    conn.close()


def upsert_commit(commit: Commit, db_path: Path = DB_PATH) -> bool:
    """Insert or update a commit. Returns True if new."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT hash FROM commits WHERE hash = ?", (commit.hash,))
    is_new = cursor.fetchone() is None

    cursor.execute(
        """
        INSERT INTO commits (hash, repo, author, date, message, files_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(hash) DO UPDATE SET
            repo = excluded.repo,
            author = excluded.author,
            date = excluded.date,
            message = excluded.message,
            files_json = excluded.files_json,
            fetched_at = excluded.fetched_at
        """,
        (
            commit.hash,
            commit.repo,
            commit.author,
            commit.date,
            commit.message,
            commit.files_json,
            datetime.now(UTC).isoformat(),
        ),
    )

    conn.commit()
    conn.close()
    return is_new


def get_commits_by_period(
    start_date: str,
    end_date: str,
    author: str | None = None,
    repo: str | None = None,
    db_path: Path = DB_PATH,
) -> list[Commit]:
    """Get commits within a date range."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = "SELECT * FROM commits WHERE date >= ? AND date < ?"
    params: list = [start_date, end_date]

    if author:
        query += " AND author LIKE ?"
        params.append(f"%{author}%")

    if repo:
        query += " AND repo = ?"
        params.append(repo)

    query += " ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [
        Commit(
            hash=row["hash"],
            repo=row["repo"],
            author=row["author"],
            date=row["date"],
            message=row["message"],
            files_json=row["files_json"],
        )
        for row in rows
    ]


def save_summary(summary: Summary, db_path: Path = DB_PATH) -> None:
    """Save or update a summary."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO summaries (period, content, commit_hashes_json, generated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(period) DO UPDATE SET
            content = excluded.content,
            commit_hashes_json = excluded.commit_hashes_json,
            generated_at = excluded.generated_at
        """,
        (
            summary.period,
            summary.content,
            summary.commit_hashes_json,
            summary.generated_at,
        ),
    )

    conn.commit()
    conn.close()


def get_summary(period: str, db_path: Path = DB_PATH) -> Summary | None:
    """Get a summary by period."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM summaries WHERE period = ?", (period,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return None

    return Summary(
        period=row["period"],
        content=row["content"],
        commit_hashes_json=row["commit_hashes_json"],
        generated_at=row["generated_at"],
    )
