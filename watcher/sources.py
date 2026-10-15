import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
import time
from urllib.parse import quote, urlsplit

import aiohttp
from telethon import events
from telethon.tl.functions.channels import JoinChannelRequest

from .core import Observation, html_text
from .network import ApiError, json_request, retry_delay

LOG = logging.getLogger(__name__)
MAX_PAGE_BYTES = 2 * 1024 * 1024


async def bounded_text(response) -> str:
    chunks = []
    size = 0
    async for chunk in response.content.iter_chunked(65536):
        size += len(chunk)
        if size > MAX_PAGE_BYTES:
            raise ValueError("Source exceeds 2 MiB limit")
        chunks.append(chunk)
    return b"".join(chunks).decode(response.charset or "utf-8", errors="replace")


def document_text(body: str, content_type: str) -> str:
    if "json" in content_type:
        # Decode escaped JSON strings, then inspect values rather than key names.
        def values(item):
            if isinstance(item, str):
                yield item
            elif isinstance(item, list):
                for child in item:
                    yield from values(child)
            elif isinstance(item, dict):
                for child in item.values():
                    yield from values(child)
        return "\n".join(values(json.loads(body)))
    return html_text(body) if "html" in content_type else body


async def watch_website(http, pipeline, url: str, interval: float, *, once: bool = False):
    source = "web:" + hashlib.sha256(url.encode()).hexdigest()[:16]
    label = urlsplit(url).hostname
    headers = {"Cache-Control": "no-cache"}
    failures = 0
    while True:
        started = time.monotonic()
        delay = interval
        try:
            async with http.get(url, headers=headers) as response:
                if response.status == 304:
                    failures = 0
                else:
                    if response.status >= 400:
                        raise ApiError(response.status, retry_delay(response.headers))
                    body = await bounded_text(response)
                    text = document_text(body, response.headers.get("Content-Type", ""))
                    baseline = pipeline.store.checkpoint(source) is None
                    await pipeline.process(Observation(source, text), baseline=baseline)
                    pipeline.store.set_checkpoint(source, "initialized")
                    if baseline:
                        LOG.info("Website baseline saved: %s (existing addresses ignored)", label)
                    for field, header in (("ETag", "If-None-Match"), ("Last-Modified", "If-Modified-Since")):
                        if response.headers.get(field):
                            headers[header] = response.headers[field]
                        else:
                            headers.pop(header, None)
                    failures = 0
        except (aiohttp.ClientError, TimeoutError, ApiError, ValueError, RuntimeError) as exc:
            failures += 1
            delay = max(interval, min(60, 2 ** min(failures, 6)), getattr(exc, "retry_after", 0))
            LOG.warning("Website %s unavailable: %s; retry in %.1fs", label, type(exc).__name__, delay)
            if once:
                raise
        if once:
            return
        await asyncio.sleep(max(0, delay - (time.monotonic() - started)))


def telegram_text(message) -> str:
    parts = [message.raw_text or ""]
    for entity in message.entities or []:
        url = getattr(entity, "url", None)
        if url:
            parts.append(url)
    for row in getattr(message.reply_markup, "rows", []) or []:
        for button in row.buttons:
            url = getattr(button, "url", None)
            if url:
                parts.append(url)
    return "\n".join(parts)


async def watch_telegram(client, pipeline, channels: tuple[str, ...]):
    armed_at = int(datetime.now(timezone.utc).timestamp())
    entities = [await client.get_entity(channel) for channel in channels]

    async def on_message(event):
        message = event.message
        changed_at = message.edit_date or message.date
        if changed_at and changed_at.timestamp() < armed_at:
            return  # No history replay when joining/reconnecting.
        try:
            await pipeline.process(Observation(f"tg:{event.chat_id}", telegram_text(message)))
        except Exception as exc:
            LOG.error("Telegram event processing failed: %s", type(exc).__name__)

    new_event = events.NewMessage(chats=entities, incoming=True)
    edited_event = events.MessageEdited(chats=entities, incoming=True)
    client.add_event_handler(on_message, new_event)
    client.add_event_handler(on_message, edited_event)
    try:
        for entity in entities:
            await client(JoinChannelRequest(entity))
        LOG.info("Telegram listening to %d channel(s), including message edits", len(entities))
        await client.run_until_disconnected()
        raise RuntimeError("Telegram disconnected")
    finally:
        client.remove_event_handler(on_message, new_event)
        client.remove_event_handler(on_message, edited_event)


def post_text(post: dict) -> str:
    parts = [post.get("text", "")]
    note = post.get("note_tweet") or post.get("note_post") or {}
    parts.append(note.get("text", ""))
    for entities in (post.get("entities", {}), note.get("entities", {})):
        for url in entities.get("urls", []):
            parts.extend(url.get(field, "") for field in ("expanded_url", "unwound_url"))
    return "\n".join(parts)


async def watch_x(http, pipeline, username: str, bearer: str, interval: float):
    source = f"x:{username.lower()}"
    headers = {"Authorization": f"Bearer {bearer}"}
    user_id = None
    failures = 0
    while True:
        delay = interval
        started = time.monotonic()
        try:
            if user_id is None:
                user = await json_request(
                    http, "GET", f"https://api.x.com/2/users/by/username/{quote(username, safe='')}", headers=headers
                )
                user_id = user["data"]["id"]
            since = pipeline.store.checkpoint(source)
            params = {"max_results": "100", "post.fields": "entities,note_post", "exclude": "retweets"}
            if since and since != "0":
                params["since_id"] = since
            posts = []
            while True:
                payload = await json_request(
                    http, "GET", f"https://api.x.com/2/users/{user_id}/tweets", headers=headers, params=params
                )
                if payload.get("errors"):
                    raise RuntimeError("X returned partial errors; checkpoint not advanced")
                posts.extend(payload.get("data", []))
                token = payload.get("meta", {}).get("next_token")
                if since is None or not token:
                    break  # On first startup only baseline the latest batch.
                params["pagination_token"] = token
            for post in sorted(posts, key=lambda item: int(item["id"])):
                await pipeline.process(Observation(source, post_text(post)), baseline=since is None)
            newest = max((int(post["id"]) for post in posts), default=int(since or "0"))
            pipeline.store.set_checkpoint(source, str(newest))
            if since is None:
                LOG.info("X baseline saved for @%s (existing posts ignored)", username)
            failures = 0
        except (aiohttp.ClientError, TimeoutError, ApiError, ValueError, KeyError, RuntimeError) as exc:
            failures += 1
            delay = max(interval, min(60, 2 ** min(failures, 6)), getattr(exc, "retry_after", 0))
            LOG.warning("X unavailable: %s; retry in %.1fs", type(exc).__name__, delay)
        await asyncio.sleep(max(0, delay - (time.monotonic() - started)))
