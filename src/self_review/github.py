"""Fetch PR data from GitHub using the gh CLI."""

import json
import subprocess
from dataclasses import dataclass

from self_review.db import CommentGiven, PullRequest, ReviewGiven

# Bot accounts to filter out
BOT_USERS = frozenset(
    {
        "copilot-pull-request-reviewer",
        "dependabot",
        "dependabot[bot]",
        "github-actions",
        "github-actions[bot]",
        "renovate",
        "renovate[bot]",
        "codecov",
        "codecov[bot]",
        "snyk-bot",
        "sonarcloud[bot]",
        "mergify",
        "mergify[bot]",
    }
)


def is_bot(username: str) -> bool:
    """Check if a username is a known bot."""
    lower = username.lower()
    return lower in BOT_USERS or lower.endswith("[bot]")


@dataclass
class FetchResult:
    """Result of fetching PR data."""

    prs_authored: list[PullRequest]
    reviews_given: list[ReviewGiven]
    comments_given: list[CommentGiven]


def _run_gh_query(query: str, variables: dict | None = None) -> dict:
    """Run a GraphQL query via gh CLI."""
    cmd = ["gh", "api", "graphql", "-f", f"query={query}"]

    if variables:
        for key, value in variables.items():
            if value is not None:
                cmd.extend(["-f", f"{key}={value}"])

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def fetch_prs_authored(
    repo: str,
    author: str,
    start_date: str,
    end_date: str,
) -> list[PullRequest]:
    """Fetch PRs authored by the user in a date range (with pagination)."""
    search_query = f"repo:{repo} author:{author} created:{start_date}..{end_date} is:pr"

    query = """
    query($searchQuery: String!, $cursor: String) {
      search(query: $searchQuery, type: ISSUE, first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          ... on PullRequest {
            number
            title
            state
            createdAt
            mergedAt
            additions
            deletions
            changedFiles
            reviews(first: 50) {
              nodes {
                author { login }
                state
                body
                submittedAt
              }
            }
          }
        }
      }
    }
    """

    prs = []
    cursor = None

    while True:
        try:
            data = _run_gh_query(query, {"searchQuery": search_query, "cursor": cursor})
        except subprocess.CalledProcessError:
            break

        search_data = data.get("data", {}).get("search", {})

        for node in search_data.get("nodes", []):
            if not node:
                continue

            # Filter out bot reviews
            reviews = [
                {
                    "author": r["author"]["login"] if r.get("author") else "unknown",
                    "state": r["state"],
                    "body": r.get("body", ""),
                    "submitted_at": r["submittedAt"],
                }
                for r in node.get("reviews", {}).get("nodes", [])
                if r and r.get("author") and not is_bot(r["author"]["login"])
            ]

            prs.append(
                PullRequest(
                    number=node["number"],
                    repo=repo,
                    title=node["title"],
                    state=node["state"],
                    created_at=node["createdAt"],
                    merged_at=node.get("mergedAt"),
                    additions=node.get("additions", 0),
                    deletions=node.get("deletions", 0),
                    changed_files=node.get("changedFiles", 0),
                    reviews_json=json.dumps(reviews),
                )
            )

        # Check for more pages
        page_info = search_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return prs


def fetch_reviews_given(
    repo: str,
    author: str,
    start_date: str,
    end_date: str,
) -> list[ReviewGiven]:
    """Fetch reviews given by the user on other people's PRs (with pagination)."""
    search_query = (
        f"repo:{repo} reviewed-by:{author} -author:{author} created:{start_date}..{end_date} is:pr"
    )

    query = """
    query($searchQuery: String!, $cursor: String) {
      search(query: $searchQuery, type: ISSUE, first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          ... on PullRequest {
            number
            title
            author { login }
            reviews(first: 50) {
              nodes {
                author { login }
                state
                body
                submittedAt
              }
            }
          }
        }
      }
    }
    """

    reviews = []
    cursor = None

    while True:
        try:
            data = _run_gh_query(query, {"searchQuery": search_query, "cursor": cursor})
        except subprocess.CalledProcessError:
            break

        search_data = data.get("data", {}).get("search", {})

        for node in search_data.get("nodes", []):
            if not node:
                continue

            pr_author = (
                node.get("author", {}).get("login", "unknown") if node.get("author") else "unknown"
            )

            # Get reviews by this author
            for review in node.get("reviews", {}).get("nodes", []):
                if not review or not review.get("author"):
                    continue

                reviewer = review["author"]["login"]
                if reviewer.lower() != author.lower():
                    continue

                # Skip if the review timestamp is outside our date range
                submitted = review.get("submittedAt") or ""
                if not submitted or submitted < start_date or submitted >= end_date:
                    continue

                reviews.append(
                    ReviewGiven(
                        pr_number=node["number"],
                        repo=repo,
                        pr_title=node["title"],
                        pr_author=pr_author,
                        state=review["state"],
                        body=review.get("body", ""),
                        submitted_at=submitted,
                    )
                )

        # Check for more pages
        page_info = search_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return reviews


def fetch_comments_given(
    repo: str,
    author: str,
    start_date: str,
    end_date: str,
) -> list[CommentGiven]:
    """Fetch comments given by the user on other people's PRs (with pagination)."""
    search_query = (
        f"repo:{repo} commenter:{author} -author:{author} created:{start_date}..{end_date} is:pr"
    )

    query = """
    query($searchQuery: String!, $cursor: String) {
      search(query: $searchQuery, type: ISSUE, first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          ... on PullRequest {
            number
            title
            author { login }
            comments(first: 100) {
              nodes {
                author { login }
                body
                createdAt
              }
            }
            reviewThreads(first: 50) {
              nodes {
                comments(first: 50) {
                  nodes {
                    author { login }
                    body
                    createdAt
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    comments = []
    seen = set()  # Dedupe by (pr_number, created_at, body)
    cursor = None

    while True:
        try:
            data = _run_gh_query(query, {"searchQuery": search_query, "cursor": cursor})
        except subprocess.CalledProcessError:
            break

        search_data = data.get("data", {}).get("search", {})

        for node in search_data.get("nodes", []):
            if not node:
                continue

            pr_author = (
                node.get("author", {}).get("login", "unknown") if node.get("author") else "unknown"
            )

            # Get regular PR comments by this author
            for comment in node.get("comments", {}).get("nodes", []):
                if not comment or not comment.get("author"):
                    continue

                commenter = comment["author"]["login"]
                if commenter.lower() != author.lower():
                    continue

                created = comment.get("createdAt") or ""
                if not created or created < start_date or created >= end_date:
                    continue

                body = comment.get("body") or ""
                key = (node["number"], created, body)
                if key in seen:
                    continue
                seen.add(key)

                comments.append(
                    CommentGiven(
                        pr_number=node["number"],
                        repo=repo,
                        pr_title=node["title"],
                        pr_author=pr_author,
                        body=body,
                        created_at=created,
                    )
                )

            # Get review thread comments by this author
            for thread in node.get("reviewThreads", {}).get("nodes", []):
                if not thread:
                    continue

                for comment in thread.get("comments", {}).get("nodes", []):
                    if not comment or not comment.get("author"):
                        continue

                    commenter = comment["author"]["login"]
                    if commenter.lower() != author.lower():
                        continue

                    created = comment.get("createdAt") or ""
                    if not created or created < start_date or created >= end_date:
                        continue

                    body = comment.get("body") or ""
                    key = (node["number"], created, body)
                    if key in seen:
                        continue
                    seen.add(key)

                    comments.append(
                        CommentGiven(
                            pr_number=node["number"],
                            repo=repo,
                            pr_title=node["title"],
                            pr_author=pr_author,
                            body=body,
                            created_at=created,
                        )
                    )

        # Check for more pages
        page_info = search_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return comments


def fetch_all_pr_data(
    repo: str,
    author: str,
    start_date: str,
    end_date: str,
) -> FetchResult:
    """Fetch all PR-related data for a repo and author."""
    return FetchResult(
        prs_authored=fetch_prs_authored(repo, author, start_date, end_date),
        reviews_given=fetch_reviews_given(repo, author, start_date, end_date),
        comments_given=fetch_comments_given(repo, author, start_date, end_date),
    )
