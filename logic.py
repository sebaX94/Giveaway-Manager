from __future__ import annotations
import asyncio
import random
import re
import time
from collections import deque
from typing import Optional

from config import log


# ---------------------------------------------------------------------------
# Giveaway-Detection
# ---------------------------------------------------------------------------
class GiveawayDetector:
    def __init__(self, channel: dict):
        self.channel_id = channel["id"]
        self.triggers = [t.lower() for t in
                         (channel.get("trigger_words") or [])]
        self.window = int(channel.get("window_seconds") or 60)
        self.threshold = int(channel.get("min_unique_users") or 5)
        self.cooldown = int(channel.get("cooldown_seconds") or 300)

        self._buffer: deque[tuple[float, str, str]] = deque()
        self._last_detection = 0.0

    def reset(self):
        self._buffer.clear()
        self._last_detection = 0.0

    def _match_trigger(self, text: str) -> Optional[str]:
        t = (text or "").strip().lower()
        for trig in self.triggers:
            if t == trig or t.startswith(trig + " "):
                return trig
        return None

    def feed(self, user_id: str, user_login: str, text: str
             ) -> Optional[dict]:
        trig = self._match_trigger(text)
        if not trig:
            return None

        now = time.time()
        while self._buffer and now - self._buffer[0][0] > self.window:
            self._buffer.popleft()

        if not any(uid == user_id for _, uid, _ in self._buffer):
            self._buffer.append((now, user_id, user_login))

        unique = len(self._buffer)
        if unique < self.threshold:
            return None
        if now - self._last_detection < self.cooldown:
            return None

        self._last_detection = now
        sample = [u[2] for u in self._buffer][:25]
        self._buffer.clear()

        return {
            "channel_id": self.channel_id,
            "trigger_word": trig,
            "unique_users": unique,
            "window_seconds": self.window,
            "sample_users": sample,
        }


# ---------------------------------------------------------------------------
# Winner-Parser
# ---------------------------------------------------------------------------
_WINNER_PATTERNS = [
    re.compile(
        r"congratulation[s]?[^@\w]*@?([A-Za-z0-9_]{3,25})", re.I),
    re.compile(
        r"@?([A-Za-z0-9_]{3,25})\s+(?:has\s+)?won\s+the\s+giveaway",
        re.I),
    re.compile(
        r"@?([A-Za-z0-9_]{3,25})\s+(?:has\s+)?won\b", re.I),
    re.compile(r"winner[:\s]+@?([A-Za-z0-9_]{3,25})", re.I),
]


def extract_winner(text: str) -> Optional[str]:
    if not text:
        return None
    for pat in _WINNER_PATTERNS:
        m = pat.search(text)
        if m:
            candidate = m.group(1).lower()
            # kleine Filter für offensichtliche Nicht-Namen
            if candidate in ("the", "you", "we", "a", "an"):
                continue
            return candidate
    return None


def is_announcer(user_login: str, announcers: list[str]) -> bool:
    return (user_login or "").lower() in \
           {a.lower() for a in (announcers or [])}


# ---------------------------------------------------------------------------
# Rate Limiter + Sender
# ---------------------------------------------------------------------------
class RateLimiter:
    """Token-Bucket pro Account (Twitch: 20 Nachrichten / 30 s)."""

    def __init__(self, max_per_window: int = 20, window: float = 30.0):
        self.max = max_per_window
        self.window = window
        self._history: dict[int, deque[float]] = {}

    async def acquire(self, account_id: int):
        while True:
            now = time.time()
            h = self._history.setdefault(account_id, deque())
            while h and now - h[0] > self.window:
                h.popleft()
            if len(h) < self.max:
                h.append(now)
                return
            wait = self.window - (now - h[0]) + 0.1
            await asyncio.sleep(max(wait, 0.5))


class MessageSender:
    def __init__(self, helix, rate_limiter: RateLimiter):
        self.helix = helix
        self.rl = rate_limiter

    async def send(self, account: dict, broadcaster_id: str,
                   message: str) -> tuple[bool, str]:
        await self.rl.acquire(account["id"])
        return await self.helix.send_chat_message(
            account["access_token"], broadcaster_id,
            account["user_id"], message,
        )

    async def send_delayed(self, accounts: list[dict], broadcaster_id: str,
                           message: str,
                           min_delay: float = 2.0,
                           max_delay: float = 6.0) -> list[dict]:
        results = []
        for i, acc in enumerate(accounts):
            if i > 0:
                await asyncio.sleep(random.uniform(min_delay, max_delay))
            ok, reason = await self.send(acc, broadcaster_id, message)
            results.append({"account_id": acc["id"],
                            "login": acc["login"],
                            "ok": ok, "reason": reason})
            log.info("Chat-Send [%s -> %s]: %s",
                     acc["login"], broadcaster_id,
                     "OK" if ok else reason)
        return results