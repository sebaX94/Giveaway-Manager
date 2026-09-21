from __future__ import annotations
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "service.db"
LOG_FILE = LOG_DIR / "service.log"

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "").strip()
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "").strip()

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()
N8N_WEBHOOK_SECRET = os.getenv("N8N_WEBHOOK_SECRET", "").strip()

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8080"))
API_TOKEN = os.getenv("API_TOKEN", "").strip()

DEFAULT_WINDOW_SECONDS = int(os.getenv("DEFAULT_WINDOW_SECONDS", "60"))
DEFAULT_MIN_UNIQUE_USERS = int(os.getenv("DEFAULT_MIN_UNIQUE_USERS", "5"))
DEFAULT_COOLDOWN_SECONDS = int(os.getenv("DEFAULT_COOLDOWN_SECONDS", "300"))

SCOPES = ["user:read:chat", "user:write:chat"]

OAUTH_BASE = "https://id.twitch.tv/oauth2"
HELIX_BASE = "https://api.twitch.tv/helix"
EVENTSUB_URL = "wss://eventsub.wss.twitch.tv/ws"

DEFAULT_TRIGGER_WORDS = ["!join", "!giveaway", "!enter", "!teilnehmen", "!give"]
DEFAULT_ANNOUNCER_LOGINS = ["nightbot", "streamelements", "fossabot"]

TOKEN_REFRESH_INTERVAL = 30 * 60  # 30 Minuten

# --- Logging ---
log = logging.getLogger("giveaway")
log.setLevel(logging.INFO)
_fmt = logging.Formatter(
    "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if not log.handlers:
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(_fmt)
    log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(_fmt)
    log.addHandler(ch)