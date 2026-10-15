from dataclasses import dataclass
import math
import os
from pathlib import Path
import re


def load_env(path: Path) -> None:
    """Read simple KEY=value files without executing shell syntax."""
    if not path.exists():
        return
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ValueError(f"Invalid env entry at line {number}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    channels: tuple[str, ...]
    session_path: str
    trading_bot: str
    state_db: str
    rpc_url: str
    website_urls: tuple[str, ...]
    website_poll: float
    x_username: str
    x_bearer: str
    x_poll: float
    ignored: frozenset[str]

    @classmethod
    def from_env(cls) -> "Config":
        env = os.environ.get
        config = cls(
            api_id=int(env("TG_API_ID", "") or 0),
            api_hash=env("TG_API_HASH", ""),
            channels=split_csv(env("TG_CHANNELS", "@token_1win")),
            session_path=env("TG_SESSION", "data/telegram"),
            trading_bot=env("TRADING_BOT", ""),
            state_db=env("STATE_DB", "data/state.sqlite3"),
            rpc_url=env("RPC_URL", ""),
            website_urls=split_csv(env("WEBSITE_URLS", "https://1wintoken.com/")),
            website_poll=float(env("WEBSITE_POLL_SECONDS", "1")),
            x_username=env("X_USERNAME", "1winToken").lstrip("@"),
            x_bearer=env("X_BEARER_TOKEN", ""),
            x_poll=float(env("X_POLL_SECONDS", "15")),
            ignored=frozenset(split_csv(env(
                "IGNORE_ADDRESSES", "So11111111111111111111111111111111111111112"
            ))),
        )
        for interval in (config.website_poll, config.x_poll):
            if not math.isfinite(interval) or interval < 0.25:
                raise ValueError("Polling/wait intervals must be finite and >= 0.25 s")
        for url in (*config.website_urls, *((config.rpc_url,) if config.rpc_url else ())):
            if not url.startswith(("http://", "https://")):
                raise ValueError("Sources/RPC must use HTTP(S)")
        return config

    def validate_live(self) -> None:
        if not self.rpc_url:
            raise ValueError("LIVE requires RPC_URL for mint verification")
        if not (self.api_id and self.api_hash and self.trading_bot):
            raise ValueError("LIVE requires TG_API_ID, TG_API_HASH, TRADING_BOT")
