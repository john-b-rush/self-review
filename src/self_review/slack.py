"""Fetch Slack reaction data using xoxc token + cookie auth."""

import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime

from self_review.db import SlackReaction


def _slack_ts_to_iso(ts: str) -> str:
    """Convert Slack timestamp (e.g., '1234567890.123456') to ISO date."""
    try:
        unix_ts = float(ts.split(".")[0])
        dt = datetime.fromtimestamp(unix_ts, tz=UTC)
        return dt.isoformat()
    except (ValueError, IndexError):
        return ""


def _make_slack_request(
    endpoint: str,
    token: str,
    cookie: str,
    params: dict | None = None,
) -> dict:
    """Make a request to the Slack API."""
    base_url = "https://slack.com/api"
    url = f"{base_url}/{endpoint}"

    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{url}?{query}"

    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Cookie", f"d={cookie}")

    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode())


def test_auth(token: str, cookie: str) -> dict | None:
    """Test if the token/cookie are valid. Returns user info or None."""
    try:
        data = _make_slack_request("auth.test", token, cookie)
        if data.get("ok"):
            return {
                "user": data.get("user"),
                "user_id": data.get("user_id"),
                "team": data.get("team"),
                "team_id": data.get("team_id"),
            }
    except Exception:
        pass
    return None


def get_my_channels(token: str, cookie: str) -> list[dict]:
    """Get all channels the authenticated user is a member of."""
    channels = []
    cursor = None

    while True:
        params = {
            "types": "public_channel,private_channel",
            "limit": "200",
            "exclude_archived": "true",
        }
        if cursor:
            params["cursor"] = cursor

        data = _make_slack_request("users.conversations", token, cookie, params)

        if not data.get("ok"):
            break

        channels.extend(data.get("channels", []))

        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    return channels


def fetch_reactions(
    token: str,
    cookie: str,
    user_id: str,
    start_date: str,
    end_date: str,
    progress_callback=None,
    on_reaction=None,
) -> list[SlackReaction]:
    """
    Fetch reactions given by the authenticated user by scanning channel history.

    Args:
        token: Slack xoxc- token
        cookie: Slack xoxd- cookie value
        user_id: The authenticated user's Slack user ID
        start_date: ISO date string (e.g., "2025-01-01")
        end_date: ISO date string (e.g., "2026-01-01")
        progress_callback: Optional callback(channel_name, reactions_found)
        on_reaction: Optional callback(SlackReaction) called for each reaction found.
                     Use this to save incrementally so data isn't lost on interrupt.

    Returns:
        List of SlackReaction objects within the date range
    """
    channels = get_my_channels(token, cookie)
    reactions = []

    for ch in channels:
        ch_id = ch["id"]
        ch_name = ch.get("name", ch_id)

        channel_reactions = 0
        cursor = None
        reached_start = False

        # Paginate through message history until we reach start_date
        while not reached_start:
            params = {"channel": ch_id, "limit": "200"}
            if cursor:
                params["cursor"] = cursor

            data = _make_slack_request("conversations.history", token, cookie, params)

            if not data.get("ok"):
                # Channel not accessible (archived, no permission, etc.)
                break

            messages = data.get("messages", [])
            if not messages:
                break

            for msg in messages:
                msg_ts = msg.get("ts", "")
                reacted_at = _slack_ts_to_iso(msg_ts)

                if not reacted_at:
                    continue

                # Stop if we've gone past the start date
                if reacted_at < start_date:
                    reached_start = True
                    break

                # Skip if after end date
                if reacted_at >= end_date:
                    continue

                # Check each reaction for my user ID
                for r in msg.get("reactions", []):
                    if user_id in r.get("users", []):
                        reaction = SlackReaction(
                            emoji=r["name"],
                            channel_id=ch_id,
                            channel_name=ch_name,
                            message_ts=msg_ts,
                            message_user=msg.get("user", "unknown"),
                            message_text=(msg.get("text") or "")[:200],
                            reacted_at=reacted_at,
                        )
                        reactions.append(reaction)
                        channel_reactions += 1

                        if on_reaction:
                            on_reaction(reaction)

            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        if progress_callback and channel_reactions > 0:
            progress_callback(ch_name, channel_reactions)

    return reactions
