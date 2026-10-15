import asyncio
import time

import aiohttp

TOKEN_PROGRAMS = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
}


class ApiError(RuntimeError):
    def __init__(self, status: int, retry_after: float = 0):
        self.status = status
        self.retry_after = retry_after
        super().__init__(f"HTTP {status}")


def retry_delay(headers) -> float:
    retry = headers.get("Retry-After", "")
    reset = headers.get("x-rate-limit-reset", "")
    delay = float(retry) if retry.isdigit() else 0
    if reset.isdigit():
        delay = max(delay, int(reset) - time.time())
    return max(0, delay)


async def json_request(http, method: str, url: str, **kwargs):
    async with http.request(method, url, **kwargs) as response:
        if response.status >= 400:
            # Do not include responses/headers/credential-bearing URLs in errors.
            raise ApiError(response.status, retry_delay(response.headers))
        payload = await response.json(content_type=None)
        if not isinstance(payload, dict):
            raise ValueError("Expected JSON object")
        return payload


class Rpc:
    """Read-only mint validation; never signs or sends blockchain transactions."""
    def __init__(self, http: aiohttp.ClientSession, url: str):
        self.http = http
        self.url = url

    async def is_mint(self, address: str) -> bool:
        for attempt in range(5):
            payload = await json_request(self.http, "POST", self.url, json={
                "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                "params": [address, {"encoding": "jsonParsed", "commitment": "processed"}],
            })
            if "error" in payload or "result" not in payload:
                raise RuntimeError("RPC mint lookup failed")
            value = payload["result"].get("value")
            if value is not None:
                data = value.get("data")
                parsed = data.get("parsed", {}) if isinstance(data, dict) else {}
                return value.get("owner") in TOKEN_PROGRAMS and parsed.get("type") == "mint"
            if attempt < 4:
                await asyncio.sleep(0.2)
        raise RuntimeError("Mint not visible yet")


class TelegramExecutor:
    def __init__(self, client, target):
        self.client = client
        self.target = target

    async def execute(self, address: str, checkpoint) -> str:
        checkpoint("telegram-send-pending")
        message = await self.client.send_message(self.target, address, parse_mode=None, link_preview=False)
        # Confirms Telegram delivery; the trading bot's purchase is a separate action.
        return f"telegram-message:{message.id}"
