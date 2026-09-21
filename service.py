from __future__ import annotations
import asyncio
import time
from typing import Optional

import httpx

from config import N8N_WEBHOOK_URL, N8N_WEBHOOK_SECRET, log
from db import Database
from twitch import HelixClient, EventSubListener
from logic import (
    GiveawayDetector, extract_winner, is_announcer,
    MessageSender, RateLimiter,
)


class Notifier:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def emit(self, event: str, data: dict):
        payload = {"event": event, "ts": time.time(), **data}
        if not N8N_WEBHOOK_URL:
            log.info("[n8n OFF] %s %s", event, data)
            return
        headers = {}
        if N8N_WEBHOOK_SECRET:
            headers["X-Webhook-Secret"] = N8N_WEBHOOK_SECRET
        try:
            await self.client.post(N8N_WEBHOOK_URL, headers=headers,
                                   json=payload, timeout=10)
            log.info("[n8n] %s gesendet.", event)
        except Exception as e:
            log.warning("n8n-Webhook fehlgeschlagen: %s", e)


class Service:
    def __init__(self):
        self.db: Optional[Database] = None
        self.http: Optional[httpx.AsyncClient] = None
        self.helix: Optional[HelixClient] = None
        self.sender: Optional[MessageSender] = None
        self.notifier: Optional[Notifier] = None

        self.listener: Optional[EventSubListener] = None
        self.detectors: dict[str, GiveawayDetector] = {}
        self.active_giveaway: dict[str, int] = {}
        self.listener_login: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None

    # ----- Lifecycle -----
    async def start(self):
        self.db = Database()
        await self.db.init()
        self.http = httpx.AsyncClient(timeout=20)
        self.helix = HelixClient(self.http)
        self.sender = MessageSender(self.helix, RateLimiter())
        self.notifier = Notifier(self.http)

        await self.refresh_all_tokens()
        await self.start_listener_if_possible()

        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self):
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.listener:
            await self.listener.stop()
        if self.http:
            await self.http.aclose()
        if self.db:
            await self.db.close()

    async def _refresh_loop(self):
        from config import TOKEN_REFRESH_INTERVAL
        while True:
            try:
                await asyncio.sleep(TOKEN_REFRESH_INTERVAL)
                await self.refresh_all_tokens()
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("Token-Refresh-Loop Fehler")

    # ----- Token -----
    async def refresh_all_tokens(self):
        for acc in await self.db.all_accounts():
            if acc["expires_at"] - time.time() > 300:
                continue
            data = await self.helix.refresh_token(acc["refresh_token"])
            if not data:
                continue
            await self.db.update_account_tokens(
                acc["id"], data["access_token"],
                data.get("refresh_token", acc["refresh_token"]),
                time.time() + data.get("expires_in", 3600),
                data.get("scope", []),
            )
            log.info("Token erneuert: %s", acc["login"])

    async def _get_listener_token(self) -> str:
        acc = await self.db.get_account_by_login(self.listener_login)
        if not acc:
            raise RuntimeError("Listener-Account nicht gefunden.")
        if acc["expires_at"] - time.time() < 300:
            data = await self.helix.refresh_token(acc["refresh_token"])
            if data:
                await self.db.update_account_tokens(
                    acc["id"], data["access_token"],
                    data.get("refresh_token", acc["refresh_token"]),
                    time.time() + data.get("expires_in", 3600),
                    data.get("scope", []),
                )
                acc = await self.db.get_account_by_login(self.listener_login)
        return acc["access_token"]

    # ----- Listener -----
    async def start_listener_if_possible(self):
        account = await self.db.first_valid_account()
        if not account:
            log.info("Kein Account vorhanden – Listener bleibt aus.")
            return
        channels = await self.db.all_channels()
        if not channels:
            log.info("Keine Kanäle vorhanden – Listener bleibt aus.")
            return

        self.listener_login = account["login"]
        self.listener = EventSubListener(
            listener_login=account["login"],
            listener_user_id=account["user_id"],
            get_token=self._get_listener_token,
            on_message=self._on_chat_event,
        )
        self.listener.set_channels(channels)
        for ch in channels:
            self.detectors[ch["id"]] = GiveawayDetector(ch)
        await self.listener.start()

    async def restart_listener(self):
        if self.listener:
            await self.listener.stop()
            self.listener = None
        self.detectors.clear()
        await self.start_listener_if_possible()

    # ----- Channels -----
    async def add_channel(self, login: str) -> dict:
        acc = await self.db.first_valid_account()
        if not acc:
            raise RuntimeError("Kein gültiger Account vorhanden.")
        # Account-Token ggf. erneuern
        if acc["expires_at"] - time.time() < 300:
            data = await self.helix.refresh_token(acc["refresh_token"])
            if data:
                await self.db.update_account_tokens(
                    acc["id"], data["access_token"],
                    data.get("refresh_token", acc["refresh_token"]),
                    time.time() + data.get("expires_in", 3600),
                    data.get("scope", []),
                )
                acc = await self.db.get_account_by_login(acc["login"])

        info = await self.helix.get_user_by_login(
            acc["access_token"], login)
        if not info:
            raise RuntimeError(f"Kanal '{login}' nicht gefunden.")
        await self.db.upsert_channel(
            info["id"], info["login"],
            info.get("display_name", info["login"]),
        )
        ch = await self.db.get_channel(info["id"])
        self.detectors[ch["id"]] = GiveawayDetector(ch)

        # Listener updaten
        if self.listener:
            await self.restart_listener()
        else:
            await self.start_listener_if_possible()
        return ch

    async def remove_channel(self, channel_id: str):
        await self.db.delete_channel(channel_id)
        self.detectors.pop(channel_id, None)
        self.active_giveaway.pop(channel_id, None)
        if self.listener:
            await self.restart_listener()

    # ----- Event Handling -----
    async def _on_chat_event(self, channel_id: str, event: dict):
        user_login = (event.get("chatter_user_login") or "").lower()
        user_id = event.get("chatter_user_id") or ""
        text = (event.get("message") or {}).get("text", "") or ""

        channel = await self.db.get_channel(channel_id)
        if not channel:
            return

        # 1) Winner-Erkennung
        sub_type = event.get("_sub_type", "")
        is_announcement = (sub_type == "channel.chat.notification"
                           and event.get("notice_type") == "announcement")
        if is_announcer(user_login, channel.get("announcer_logins", [])) \
                or is_announcement:
            winner = extract_winner(text)
            if winner:
                await self._handle_winner(channel, winner, text)
                return

        # 2) Giveaway-Detection
        det = self.detectors.get(channel_id)
        if not det:
            det = GiveawayDetector(channel)
            self.detectors[channel_id] = det
        result = det.feed(user_id, user_login, text)
        if result:
            await self._handle_giveaway_detected(channel, result)

    async def _handle_giveaway_detected(self, channel: dict, det: dict):
        log.info("Giveaway erkannt in %s: %s (%d unique)",
                 channel["login"], det["trigger_word"], det["unique_users"])
        giveaway_id = await self.db.create_giveaway(
            channel["id"], det["trigger_word"],
            det["unique_users"], det["sample_users"],
        )
        self.active_giveaway[channel["id"]] = giveaway_id

        await self.notifier.emit("giveaway_detected", {
            "channel_id": channel["id"],
            "channel_login": channel["login"],
            "giveaway_id": giveaway_id,
            "trigger_word": det["trigger_word"],
            "unique_users": det["unique_users"],
            "sample_users": det["sample_users"],
            "auto_join": bool(channel.get("auto_join")),
        })

        if channel.get("auto_join"):
            await self.join_giveaway(channel["id"], det["trigger_word"])

    async def join_giveaway(self, channel_id: str,
                            trigger_word: str = "!join") -> dict:
        channel = await self.db.get_channel(channel_id)
        if not channel:
            return {"ok": False, "reason": "channel_not_found"}

        giveaway_id = self.active_giveaway.get(channel_id)
        if not giveaway_id:
            giveaway_id = await self.db.create_giveaway(
                channel_id, trigger_word, 0, [])
            self.active_giveaway[channel_id] = giveaway_id

        accounts = await self.db.all_accounts()
        if not accounts:
            return {"ok": False, "reason": "no_accounts"}

        results = await self.sender.send_delayed(
            accounts, channel_id, trigger_word,
            min_delay=2.0, max_delay=6.0,
        )
        for r, acc in zip(results, accounts):
            await self.db.log_participation(
                giveaway_id, acc["id"], r["ok"], r["reason"])

        await self.db.set_giveaway_status(giveaway_id, "joined")
        await self.notifier.emit("joined", {
            "channel_id": channel_id,
            "giveaway_id": giveaway_id,
            "trigger_word": trigger_word,
            "results": results,
        })
        return {"ok": True, "giveaway_id": giveaway_id,
                "results": results}

    async def _handle_winner(self, channel: dict, winner_login: str,
                             raw_text: str):
        log.info("Winner in %s: %s", channel["login"], winner_login)

        accounts = await self.db.all_accounts(only_enabled=False)
        winner_is_us = any(a["login"] == winner_login for a in accounts)

        giveaway_id = self.active_giveaway.pop(channel["id"], None)
        if giveaway_id:
            await self.db.mark_giveaway_winner(
                giveaway_id, winner_login, winner_is_us)

        det = self.detectors.get(channel["id"])
        if det:
            det.reset()

        await self.notifier.emit("winner_announced", {
            "channel_id": channel["id"],
            "channel_login": channel["login"],
            "giveaway_id": giveaway_id,
            "winner_login": winner_login,
            "is_us": winner_is_us,
            "raw_text": raw_text,
        })