from __future__ import annotations
import json
import time
from typing import Optional

import aiosqlite

from config import (
    DB_PATH,
    DEFAULT_TRIGGER_WORDS,
    DEFAULT_WINDOW_SECONDS,
    DEFAULT_MIN_UNIQUE_USERS,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_ANNOUNCER_LOGINS,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    login TEXT UNIQUE NOT NULL,
    user_id TEXT NOT NULL,
    display_name TEXT,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at REAL NOT NULL,
    scopes TEXT,
    enabled INTEGER DEFAULT 1,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    login TEXT UNIQUE NOT NULL,
    display_name TEXT,
    trigger_words TEXT,
    window_seconds INTEGER,
    min_unique_users INTEGER,
    cooldown_seconds INTEGER,
    announcer_logins TEXT,
    auto_join INTEGER DEFAULT 0,
    enabled INTEGER DEFAULT 1,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS giveaways (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    detected_at REAL NOT NULL,
    trigger_word TEXT,
    unique_users INTEGER,
    sample_users TEXT,
    status TEXT DEFAULT 'detected',
    winner_login TEXT,
    winner_is_us INTEGER DEFAULT 0,
    closed_at REAL
);

CREATE TABLE IF NOT EXISTS participations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    giveaway_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    sent_at REAL NOT NULL,
    success INTEGER,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_giveaways_channel
    ON giveaways(channel_id, detected_at DESC);
"""


class Database:
    def __init__(self, path: str = str(DB_PATH)):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def init(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    # ---------------- Accounts ----------------
    async def add_account(self, login, user_id, display_name,
                          access_token, refresh_token,
                          expires_at, scopes) -> int:
        await self._conn.execute(
            """INSERT INTO accounts
                 (login, user_id, display_name, access_token, refresh_token,
                  expires_at, scopes, enabled, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
               ON CONFLICT(login) DO UPDATE SET
                 user_id=excluded.user_id,
                 display_name=excluded.display_name,
                 access_token=excluded.access_token,
                 refresh_token=excluded.refresh_token,
                 expires_at=excluded.expires_at,
                 scopes=excluded.scopes,
                 enabled=1""",
            (login.lower(), user_id, display_name, access_token,
             refresh_token, expires_at, json.dumps(scopes), time.time()),
        )
        await self._conn.commit()
        row = await self.get_account_by_login(login)
        return row["id"]

    async def get_account_by_login(self, login: str) -> Optional[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM accounts WHERE login = ?", (login.lower(),))
        r = await cur.fetchone()
        return dict(r) if r else None

    async def get_account_by_id(self, account_id: int) -> Optional[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM accounts WHERE id = ?", (account_id,))
        r = await cur.fetchone()
        return dict(r) if r else None

    async def all_accounts(self, only_enabled: bool = True) -> list[dict]:
        q = ("SELECT * FROM accounts" +
             (" WHERE enabled = 1" if only_enabled else "") +
             " ORDER BY id")
        cur = await self._conn.execute(q)
        return [dict(r) for r in await cur.fetchall()]

    async def first_valid_account(self) -> Optional[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM accounts WHERE enabled = 1 ORDER BY id LIMIT 1")
        r = await cur.fetchone()
        return dict(r) if r else None

    async def update_account_tokens(self, account_id, access_token,
                                    refresh_token, expires_at, scopes):
        await self._conn.execute(
            """UPDATE accounts SET access_token=?, refresh_token=?,
                 expires_at=?, scopes=? WHERE id=?""",
            (access_token, refresh_token, expires_at,
             json.dumps(scopes), account_id))
        await self._conn.commit()

    async def set_account_enabled(self, account_id: int, enabled: bool):
        await self._conn.execute(
            "UPDATE accounts SET enabled = ? WHERE id = ?",
            (1 if enabled else 0, account_id))
        await self._conn.commit()

    async def delete_account(self, account_id: int):
        await self._conn.execute(
            "DELETE FROM accounts WHERE id = ?", (account_id,))
        await self._conn.commit()

    # ---------------- Channels ----------------
    async def upsert_channel(self, channel_id, login, display_name):
        await self._conn.execute(
            """INSERT INTO channels
                 (id, login, display_name, trigger_words, window_seconds,
                  min_unique_users, cooldown_seconds, announcer_logins,
                  auto_join, enabled, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?)
               ON CONFLICT(id) DO UPDATE SET
                 login=excluded.login,
                 display_name=excluded.display_name""",
            (channel_id, login.lower(), display_name,
             json.dumps(DEFAULT_TRIGGER_WORDS),
             DEFAULT_WINDOW_SECONDS,
             DEFAULT_MIN_UNIQUE_USERS,
             DEFAULT_COOLDOWN_SECONDS,
             json.dumps(DEFAULT_ANNOUNCER_LOGINS),
             time.time()),
        )
        await self._conn.commit()

    async def all_channels(self) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM channels WHERE enabled = 1")
        return [self._to_channel(dict(r)) for r in await cur.fetchall()]

    async def get_channel(self, channel_id: str) -> Optional[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM channels WHERE id = ?", (channel_id,))
        r = await cur.fetchone()
        return self._to_channel(dict(r)) if r else None

    async def delete_channel(self, channel_id: str):
        await self._conn.execute(
            "DELETE FROM channels WHERE id = ?", (channel_id,))
        await self._conn.commit()

    async def set_channel_auto_join(self, channel_id: str, auto_join: bool):
        await self._conn.execute(
            "UPDATE channels SET auto_join = ? WHERE id = ?",
            (1 if auto_join else 0, channel_id))
        await self._conn.commit()

    async def update_channel_settings(self, channel_id: str, **fields):
        allowed = {"trigger_words", "window_seconds", "min_unique_users",
                   "cooldown_seconds", "announcer_logins"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k in ("trigger_words", "announcer_logins"):
                v = json.dumps(v)
            sets.append(f"{k} = ?")
            params.append(v)
        if not sets:
            return
        params.append(channel_id)
        await self._conn.execute(
            f"UPDATE channels SET {', '.join(sets)} WHERE id = ?", params)
        await self._conn.commit()

    @staticmethod
    def _to_channel(row: dict) -> dict:
        for k in ("trigger_words", "announcer_logins"):
            if isinstance(row.get(k), str):
                try:
                    row[k] = json.loads(row[k])
                except Exception:
                    row[k] = []
        return row

    # ---------------- Giveaways ----------------
    async def create_giveaway(self, channel_id, trigger, unique,
                              sample_users) -> int:
        cur = await self._conn.execute(
            """INSERT INTO giveaways
                 (channel_id, detected_at, trigger_word, unique_users,
                  sample_users, status)
               VALUES (?, ?, ?, ?, ?, 'detected')""",
            (channel_id, time.time(), trigger, unique,
             json.dumps(sample_users)))
        await self._conn.commit()
        return cur.lastrowid

    async def set_giveaway_status(self, giveaway_id: int, status: str):
        await self._conn.execute(
            "UPDATE giveaways SET status = ? WHERE id = ?",
            (status, giveaway_id))
        await self._conn.commit()

    async def mark_giveaway_winner(self, giveaway_id, winner_login, is_us):
        await self._conn.execute(
            """UPDATE giveaways
               SET winner_login=?, winner_is_us=?, status='closed',
                   closed_at=?
               WHERE id=?""",
            (winner_login, 1 if is_us else 0, time.time(), giveaway_id))
        await self._conn.commit()

    async def log_participation(self, giveaway_id, account_id,
                                success, reason):
        await self._conn.execute(
            """INSERT INTO participations
                 (giveaway_id, account_id, sent_at, success, reason)
               VALUES (?, ?, ?, ?, ?)""",
            (giveaway_id, account_id, time.time(),
             1 if success else 0, reason))
        await self._conn.commit()

    async def recent_giveaways(self, limit: int = 20) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT * FROM giveaways ORDER BY detected_at DESC LIMIT ?",
            (limit,))
        return [dict(r) for r in await cur.fetchall()]