from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from config import API_HOST, API_PORT, API_TOKEN, log
from service import Service
from twitch import DeviceCodeAuth, HelixClient


service = Service()
_pending_polls: dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await service.start()
    try:
        yield
    finally:
        await service.stop()


app = FastAPI(title="Twitch Giveaway Service", version="1.0.0",
              lifespan=lifespan)


# ---------------------------------------------------------------------------
# Auth für n8n
# ---------------------------------------------------------------------------
async def require_token(authorization: str | None = Header(None)):
    if not API_TOKEN:
        return
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class AddChannelBody(BaseModel):
    login: str


class JoinBody(BaseModel):
    trigger_word: str = "!join"


class ChannelSettingsBody(BaseModel):
    trigger_words: list[str] | None = None
    window_seconds: int | None = None
    min_unique_users: int | None = None
    cooldown_seconds: int | None = None
    announcer_logins: list[str] | None = None


# ---------------------------------------------------------------------------
# Health / Info
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {
        "ok": True,
        "listener_running": service.listener is not None,
        "listener_login": service.listener_login,
        "channels": list(service.detectors.keys()),
    }


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------
@app.get("/accounts", dependencies=[Depends(require_token)])
async def list_accounts():
    rows = await service.db.all_accounts(only_enabled=False)
    return [
        {
            "id": r["id"],
            "login": r["login"],
            "display_name": r["display_name"],
            "user_id": r["user_id"],
            "enabled": bool(r["enabled"]),
            "expires_at": r["expires_at"],
            "valid": r["expires_at"] > __import__("time").time() + 60,
        }
        for r in rows
    ]


@app.post("/accounts/start-auth", dependencies=[Depends(require_token)])
async def start_auth():
    async with httpx.AsyncClient(timeout=20) as client:
        auth = DeviceCodeAuth(client)
        try:
            data = await auth.start()
        except Exception as e:
            raise HTTPException(400, str(e))

        device_code = data["device_code"]
        interval = int(data.get("interval", 5))
        expires_in = int(data.get("expires_in", 1800))

        async def _poll():
            try:
                token = await auth.poll_until_done(
                    device_code, interval, expires_in)
                # User-Info holen
                async with httpx.AsyncClient(timeout=20) as c2:
                    helix = HelixClient(c2)
                    user = await helix.get_authenticated_user(
                        token["access_token"])
                if not user:
                    raise RuntimeError(
                        "Authentifizierter User konnte nicht geladen werden.")
                import time as _t
                acc_id = await service.db.add_account(
                    user["login"], user["id"],
                    user.get("display_name", user["login"]),
                    token["access_token"], token["refresh_token"],
                    _t.time() + token.get("expires_in", 3600),
                    token.get("scope", []),
                )
                log.info("Account authentifiziert: %s", user["login"])
                await service.notifier.emit("auth_completed", {
                    "account_id": acc_id,
                    "login": user["login"],
                    "display_name": user.get("display_name"),
                })
                # Listener starten, falls noch nicht läuft
                if not service.listener:
                    await service.start_listener_if_possible()
            except Exception as e:
                log.warning("Device-Auth-Fehler: %s", e)
                await service.notifier.emit("auth_failed",
                                            {"error": str(e)})
            finally:
                _pending_polls.pop(device_code, None)

        _pending_polls[device_code] = asyncio.create_task(_poll())
        return {
            "user_code": data["user_code"],
            "verification_uri": data["verification_uri"],
            "expires_in": expires_in,
            "interval": interval,
        }


@app.post("/accounts/{account_id}/enable",
          dependencies=[Depends(require_token)])
async def enable_account(account_id: int, enable: bool = True):
    await service.db.set_account_enabled(account_id, enable)
    return {"ok": True}


@app.delete("/accounts/{account_id}",
            dependencies=[Depends(require_token)])
async def delete_account(account_id: int):
    await service.db.delete_account(account_id)
    return {"ok": True}


@app.post("/accounts/refresh", dependencies=[Depends(require_token)])
async def refresh_accounts():
    await service.refresh_all_tokens()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------
@app.get("/channels", dependencies=[Depends(require_token)])
async def list_channels():
    return await service.db.all_channels()


@app.post("/channels", dependencies=[Depends(require_token)])
async def add_channel(body: AddChannelBody):
    login = body.login.strip().lstrip("@").lower()
    if not login:
        raise HTTPException(400, "login fehlt")
    try:
        ch = await service.add_channel(login)
    except Exception as e:
        raise HTTPException(400, str(e))
    return ch


@app.delete("/channels/{channel_id}",
            dependencies=[Depends(require_token)])
async def remove_channel(channel_id: str):
    await service.remove_channel(channel_id)
    return {"ok": True}


@app.post("/channels/{channel_id}/auto-join",
          dependencies=[Depends(require_token)])
async def set_auto_join(channel_id: str, enable: bool = True):
    await service.db.set_channel_auto_join(channel_id, enable)
    return {"ok": True}


@app.post("/channels/{channel_id}/settings",
          dependencies=[Depends(require_token)])
async def update_settings(channel_id: str, body: ChannelSettingsBody):
    fields = {k: v for k, v in body.model_dump().items()
              if v is not None}
    await service.db.update_channel_settings(channel_id, **fields)
    # Detector neu bauen
    ch = await service.db.get_channel(channel_id)
    if ch:
        from logic import GiveawayDetector
        service.detectors[channel_id] = GiveawayDetector(ch)
    return await service.db.get_channel(channel_id)


@app.get("/channels/{channel_id}/state",
         dependencies=[Depends(require_token)])
async def channel_state(channel_id: str):
    ch = await service.db.get_channel(channel_id)
    if not ch:
        raise HTTPException(404, "channel_not_found")
    return {
        "channel": ch,
        "active_giveaway_id": service.active_giveaway.get(channel_id),
        "listener_running": service.listener is not None,
        "listener_login": service.listener_login,
    }


# ---------------------------------------------------------------------------
# Aktionen
# ---------------------------------------------------------------------------
@app.post("/channels/{channel_id}/join",
          dependencies=[Depends(require_token)])
async def manual_join(channel_id: str, body: JoinBody):
    return await service.join_giveaway(channel_id, body.trigger_word)


@app.post("/listener/restart", dependencies=[Depends(require_token)])
async def restart_listener():
    await service.restart_listener()
    return {"ok": True}


@app.get("/giveaways/recent", dependencies=[Depends(require_token)])
async def recent_giveaways(limit: int = 20):
    return await service.db.recent_giveaways(limit)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host=API_HOST, port=API_PORT,
                log_level="info")