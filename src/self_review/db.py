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


@dataclass
class PullRequest:
    """Represents a PR authored by the user."""

    number: int
    repo: str  # owner/repo format
    title: str
    state: str  # MERGED, OPEN, CLOSED
    created_at: str
    merged_at: str | None
    additions: int
    deletions: int
    changed_files: int
    reviews_json: str  # JSON list of reviews received


@dataclass
class ReviewGiven:
    """Represents a review the user gave on someone else's PR."""

    pr_number: int
    repo: str
    pr_title: str
    pr_author: str
    state: str  # APPROVED, CHANGES_REQUESTED, COMMENTED
    body: str
    submitted_at: str


@dataclass
class CommentGiven:
    """Represents a comment the user left on someone else's PR."""

    pr_number: int
    repo: str
    pr_title: str
    pr_author: str
    body: str
    created_at: str


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

    # PR tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pull_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER NOT NULL,
            repo TEXT NOT NULL,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            merged_at TEXT,
            additions INTEGER DEFAULT 0,
            deletions INTEGER DEFAULT 0,
            changed_files INTEGER DEFAULT 0,
            reviews_json TEXT DEFAULT '[]',
            fetched_at TEXT NOT NULL,
            UNIQUE(repo, number)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_prs_repo ON pull_requests(repo)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_prs_created ON pull_requests(created_at)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews_given (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_number INTEGER NOT NULL,
            repo TEXT NOT NULL,
            pr_title TEXT NOT NULL,
            pr_author TEXT NOT NULL,
            state TEXT NOT NULL,
            body TEXT,
            submitted_at TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(repo, pr_number, submitted_at)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_reviews_submitted ON reviews_given(submitted_at)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments_given (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_number INTEGER NOT NULL,
            repo TEXT NOT NULL,
            pr_title TEXT NOT NULL,
            pr_author TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(repo, pr_number, created_at, body)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_comments_created ON comments_given(created_at)
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


def upsert_pull_request(pr: PullRequest, db_path: Path = DB_PATH) -> bool:
    """Insert or update a pull request. Returns True if new."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM pull_requests WHERE repo = ? AND number = ?",
        (pr.repo, pr.number),
    )
    is_new = cursor.fetchone() is None

    cursor.execute(
        """
        INSERT INTO pull_requests (number, repo, title, state, created_at, merged_at,
                                   additions, deletions, changed_files, reviews_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(repo, number) DO UPDATE SET
            title = excluded.title,
            state = excluded.state,
            merged_at = excluded.merged_at,
            additions = excluded.additions,
            deletions = excluded.deletions,
            changed_files = excluded.changed_files,
            reviews_json = excluded.reviews_json,
            fetched_at = excluded.fetched_at
        """,
        (
            pr.number,
            pr.repo,
            pr.title,
            pr.state,
            pr.created_at,
            pr.merged_at,
            pr.additions,
            pr.deletions,
            pr.changed_files,
            pr.reviews_json,
            datetime.now(UTC).isoformat(),
        ),
    )

    conn.commit()
    conn.close()
    return is_new


def upsert_review_given(review: ReviewGiven, db_path: Path = DB_PATH) -> bool:
    """Insert or update a review given. Returns True if new."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM reviews_given WHERE repo = ? AND pr_number = ? AND submitted_at = ?",
        (review.repo, review.pr_number, review.submitted_at),
    )
    is_new = cursor.fetchone() is None

    cursor.execute(
        """
        INSERT INTO reviews_given (pr_number, repo, pr_title, pr_author, state, body,
                                   submitted_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(repo, pr_number, submitted_at) DO UPDATE SET
            pr_title = excluded.pr_title,
            pr_author = excluded.pr_author,
            state = excluded.state,
            body = excluded.body,
            fetched_at = excluded.fetched_at
        """,
        (
            review.pr_number,
            review.repo,
            review.pr_title,
            review.pr_author,
            review.state,
            review.body,
            review.submitted_at,
            datetime.now(UTC).isoformat(),
        ),
    )

    conn.commit()
    conn.close()
    return is_new


def upsert_comment_given(comment: CommentGiven, db_path: Path = DB_PATH) -> bool:
    """Insert or update a comment given. Returns True if new."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM comments_given WHERE repo = ? AND pr_number = ? AND created_at = ? AND body = ?",
        (comment.repo, comment.pr_number, comment.created_at, comment.body),
    )
    is_new = cursor.fetchone() is None

    cursor.execute(
        """
        INSERT INTO comments_given (pr_number, repo, pr_title, pr_author, body,
                                    created_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(repo, pr_number, created_at, body) DO UPDATE SET
            pr_title = excluded.pr_title,
            pr_author = excluded.pr_author,
            fetched_at = excluded.fetched_at
        """,
        (
            comment.pr_number,
            comment.repo,
            comment.pr_title,
            comment.pr_author,
            comment.body,
            comment.created_at,
            datetime.now(UTC).isoformat(),
        ),
    )

    conn.commit()
    conn.close()
    return is_new


def get_prs_by_period(
    start_date: str,
    end_date: str,
    repo: str | None = None,
    db_path: Path = DB_PATH,
) -> list[PullRequest]:
    """Get PRs created within a date range."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = "SELECT * FROM pull_requests WHERE created_at >= ? AND created_at < ?"
    params: list = [start_date, end_date]

    if repo:
        query += " AND repo = ?"
        params.append(repo)

    query += " ORDER BY created_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [
        PullRequest(
            number=row["number"],
            repo=row["repo"],
            title=row["title"],
            state=row["state"],
            created_at=row["created_at"],
            merged_at=row["merged_at"],
            additions=row["additions"],
            deletions=row["deletions"],
            changed_files=row["changed_files"],
            reviews_json=row["reviews_json"],
        )
        for row in rows
    ]


def get_reviews_by_period(
    start_date: str,
    end_date: str,
    repo: str | None = None,
    db_path: Path = DB_PATH,
) -> list[ReviewGiven]:
    """Get reviews given within a date range."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = "SELECT * FROM reviews_given WHERE submitted_at >= ? AND submitted_at < ?"
    params: list = [start_date, end_date]

    if repo:
        query += " AND repo = ?"
        params.append(repo)

    query += " ORDER BY submitted_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [
        ReviewGiven(
            pr_number=row["pr_number"],
            repo=row["repo"],
            pr_title=row["pr_title"],
            pr_author=row["pr_author"],
            state=row["state"],
            body=row["body"] or "",
            submitted_at=row["submitted_at"],
        )
        for row in rows
    ]


def get_comments_by_period(
    start_date: str,
    end_date: str,
    repo: str | None = None,
    db_path: Path = DB_PATH,
) -> list[CommentGiven]:
    """Get comments given within a date range."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = "SELECT * FROM comments_given WHERE created_at >= ? AND created_at < ?"
    params: list = [start_date, end_date]

    if repo:
        query += " AND repo = ?"
        params.append(repo)

    query += " ORDER BY created_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [
        CommentGiven(
            pr_number=row["pr_number"],
            repo=row["repo"],
            pr_title=row["pr_title"],
            pr_author=row["pr_author"],
            body=row["body"],
            created_at=row["created_at"],
        )
        for row in rows
    ]
