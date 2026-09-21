from __future__ import annotations
import asyncio
import json
import time
from typing import Awaitable, Callable, Optional

import httpx
import websockets

from config import (
    OAUTH_BASE, HELIX_BASE, EVENTSUB_URL, SCOPES,
    TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, log,
)


# ---------------------------------------------------------------------------
# Device Code Auth
# ---------------------------------------------------------------------------
class DeviceCodeAuth:
    """Headless OAuth über Twitch Device Code Flow."""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def start(self) -> dict:
        r = await self.client.post(
            f"{OAUTH_BASE}/device",
            data={"client_id": TWITCH_CLIENT_ID,
                  "scopes": " ".join(SCOPES)},
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Device-Code-Start fehlgeschlagen: HTTP {r.status_code} "
                f"{r.text[:300]}")
        return r.json()

    async def poll_until_done(self, device_code: str, interval: int,
                              expires_in: int) -> dict:
        deadline = time.time() + expires_in
        current_interval = interval
        while time.time() < deadline:
            await asyncio.sleep(current_interval)
            r = await self.client.post(
                f"{OAUTH_BASE}/token",
                data={
                    "client_id": TWITCH_CLIENT_ID,
                    "scopes": " ".join(SCOPES),
                    "device_code": device_code,
                    "grant_type":
                        "urn:ietf:params:oauth:grant-type:device_code",
                },
            )
            if r.status_code == 200:
                return r.json()
            try:
                msg = r.json().get("message", "")
            except Exception:
                msg = r.text[:200]
            if msg == "authorization_pending":
                continue
            if msg == "slow_down":
                current_interval += 2
                continue
            if msg in ("expired_token", "access_denied"):
                raise RuntimeError(f"Device-Code abgebrochen: {msg}")
            raise RuntimeError(f"Device-Code-Fehler: {msg}")
        raise TimeoutError("Device-Code-Flow abgelaufen.")


# ---------------------------------------------------------------------------
# Helix
# ---------------------------------------------------------------------------
class HelixClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    def _headers(self, token: str) -> dict:
        return {"Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}"}

    async def get_authenticated_user(self, token: str) -> Optional[dict]:
        r = await self.client.get(f"{HELIX_BASE}/users",
                                  headers=self._headers(token))
        if r.status_code != 200:
            return None
        data = r.json().get("data", [])
        return data[0] if data else None

    async def get_user_by_login(self, token: str,
                                login: str) -> Optional[dict]:
        r = await self.client.get(
            f"{HELIX_BASE}/users",
            headers=self._headers(token),
            params={"login": login.lower()},
        )
        if r.status_code != 200:
            return None
        data = r.json().get("data", [])
        return data[0] if data else None

    async def send_chat_message(self, token, broadcaster_id,
                                sender_id, message) -> tuple[bool, str]:
        r = await self.client.post(
            f"{HELIX_BASE}/chat/messages",
            headers={**self._headers(token),
                     "Content-Type": "application/json"},
            json={"broadcaster_id": broadcaster_id,
                  "sender_id": sender_id,
                  "message": message[:500]},
        )
        if r.status_code == 401:
            return False, "token_invalid"
        if r.status_code == 429:
            return False, "rate_limit"
        if not r.ok:
            try:
                body = r.json()
                return False, f"HTTP {r.status_code}: {body}"
            except Exception:
                return False, f"HTTP {r.status_code}: {r.text[:200]}"
        data = r.json().get("data", [{}])[0]
        if data.get("is_sent"):
            return True, "ok"
        return False, f"drop: {data.get('drop_reason')}"

    async def refresh_token(self, refresh_token: str) -> Optional[dict]:
        r = await self.client.post(
            f"{OAUTH_BASE}/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
            },
        )
        if r.status_code != 200:
            log.warning("Token-Refresh HTTP %s: %s",
                        r.status_code, r.text[:200])
            return None
        return r.json()

    async def subscribe_eventsub(self, token, session_id,
                                 sub_type, channel_id,
                                 user_id) -> tuple[bool, str]:
        payload = {
            "type": sub_type,
            "version": "1",
            "condition": {
                "broadcaster_user_id": channel_id,
                "user_id": user_id,
            },
            "transport": {"method": "websocket",
                          "session_id": session_id},
        }
        r = await self.client.post(
            f"{HELIX_BASE}/eventsub/subscriptions",
            headers={**self._headers(token),
                     "Content-Type": "application/json"},
            json=payload,
        )
        if r.status_code in (202, 409):
            return True, "ok"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"


# ---------------------------------------------------------------------------
# EventSub WebSocket Listener
# ---------------------------------------------------------------------------
ChatCallback = Callable[[str, dict], Awaitable[None]]


class EventSubListener:
    """Eine WebSocket-Verbindung für alle Kanäle.

    `on_message(channel_id, event)` wird für jede Nachricht aufgerufen.
    `event["_sub_type"]` zeigt an, welches Subscription-Event kam.
    """

    def __init__(self, listener_login: str, listener_user_id: str,
                 get_token: Callable[[], Awaitable[str]],
                 on_message: ChatCallback):
        self.listener_login = listener_login
        self.listener_user_id = listener_user_id
        self._get_token = get_token
        self.on_message = on_message

        self.channels: dict[str, dict] = {}
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._session_id: Optional[str] = None

    def set_channels(self, channels: list[dict]):
        self.channels = {c["id"]: c for c in channels}

    async def start(self):
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="eventsub")

    async def stop(self):
        self._stop.set()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self):
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("EventSub-Verbindung verloren: %s", e)
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    return

    async def _connect_and_listen(self):
        log.info("EventSub verbinde…")
        async with websockets.connect(
            EVENTSUB_URL, ping_interval=20, ping_timeout=20,
        ) as ws:
            self._ws = ws
            welcome = json.loads(await ws.recv())
            self._session_id = welcome["payload"]["session"]["id"]
            log.info("EventSub session=%s", self._session_id)

            token = await self._get_token()
            async with httpx.AsyncClient(timeout=15) as client:
                helix = HelixClient(client)
                for ch in self.channels.values():
                    ok, reason = await helix.subscribe_eventsub(
                        token, self._session_id,
                        "channel.chat.message",
                        ch["id"], self.listener_user_id,
                    )
                    if ok:
                        log.info("Subscribed: %s", ch["login"])
                    else:
                        log.warning("Subscribe fehlgeschlagen für %s: %s "
                                    "(Listener muss Moderator sein!)",
                                    ch["login"], reason)

            async for raw in ws:
                if self._stop.is_set():
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                mtype = msg.get("metadata", {}).get("message_type")
                if mtype == "notification":
                    sub = msg["metadata"].get("subscription_type", "")
                    event = msg.get("payload", {}).get("event", {})
                    event["_sub_type"] = sub
                    channel_id = event.get("broadcaster_user_id")
                    if channel_id:
                        try:
                            await self.on_message(channel_id, event)
                        except Exception:
                            log.exception("on_message-Fehler")
                elif mtype == "session_reconnect":
                    log.info("EventSub-Reconnect angefordert.")
                    break
                elif mtype == "revocation":
                    log.warning("Subscription widerrufen: %s", msg)