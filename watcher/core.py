import asyncio
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
import logging
from pathlib import Path
import re
import sqlite3
import time
from typing import Awaitable, Callable, Protocol
from urllib.parse import unquote

LOG = logging.getLogger(__name__)
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
ADDRESS_PATTERN = re.compile(r"(?<![A-Za-z0-9])[1-9A-HJ-NP-Za-km-z]{32,44}(?![A-Za-z0-9])")


def is_address(value: str) -> bool:
    if not 32 <= len(value) <= 44 or any(c not in ALPHABET for c in value):
        return False
    number = 0
    for char in value:
        number = number * 58 + ALPHABET.index(char)
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return leading_zeroes + (number.bit_length() + 7) // 8 == 32


def extract_addresses(text: str) -> tuple[str, ...]:
    text = unescape(unquote(text))
    return tuple(dict.fromkeys(
        match.group() for match in ADDRESS_PATTERN.finditer(text) if is_address(match.group())
    ))


class PageText(HTMLParser):
    """Visible text + links, excluding executable/style assets and random script IDs."""
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1
        if not self.hidden:
            for name, value in attrs:
                if value and (name in ("href", "content", "value", "data-address", "data-mint")):
                    self.parts.append(value)

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.hidden:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def html_text(html: str) -> str:
    parser = PageText()
    parser.feed(html)
    return "\n".join(parser.parts)


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS seen (
                source TEXT NOT NULL, address TEXT NOT NULL,
                PRIMARY KEY (source, address)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (source TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS actions (
                address TEXT PRIMARY KEY, source TEXT NOT NULL, status TEXT NOT NULL,
                slot TEXT UNIQUE, receipt TEXT, created REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deferred (
                address TEXT PRIMARY KEY, source TEXT NOT NULL, live INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0
            );
        """)
        self.db.commit()

    def seen(self, source: str) -> set[str]:
        return {row[0] for row in self.db.execute("SELECT address FROM seen WHERE source=?", (source,))}

    def mark_seen(self, source: str, addresses) -> None:
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO seen VALUES (?, ?)", ((source, a) for a in addresses))

    def checkpoint(self, source: str) -> str | None:
        row = self.db.execute("SELECT value FROM checkpoints WHERE source=?", (source,)).fetchone()
        return row[0] if row else None

    def set_checkpoint(self, source: str, value: str) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?, ?)", (source, value))

    def reserve(self, address: str, source: str, live: bool) -> bool:
        # A unique LIVE slot permits a single buy attempt across all three sources.
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO actions VALUES (?, ?, ?, ?, NULL, ?)",
                (address, source, "reserved" if live else "dry_run", "launch" if live else None, time.time()),
            )
        return cursor.rowcount == 1

    def enqueue(self, address: str, source: str, live: bool) -> None:
        # Source checkpoint can safely advance: the candidate is now durable.
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)", (source, address))
            self.db.execute(
                "INSERT OR IGNORE INTO deferred(address,source,live) "
                "SELECT ?,?,? WHERE NOT EXISTS (SELECT 1 FROM actions WHERE address=?)",
                (address, source, int(live), address),
            )

    def queued(self, live: bool) -> list[tuple[str, str]]:
        return self.db.execute(
            "SELECT address,source FROM deferred WHERE live=? AND retry_at<=?",
            (int(live), time.time()),
        ).fetchall()

    def queued_address(self, address: str, live: bool) -> bool:
        return self.db.execute(
            "SELECT 1 FROM deferred WHERE address=? AND live=?", (address, int(live))
        ).fetchone() is not None

    def discard(self, address: str) -> None:
        with self.db:
            self.db.execute("DELETE FROM deferred WHERE address=?", (address,))

    def defer(self, address: str) -> None:
        row = self.db.execute("SELECT attempts FROM deferred WHERE address=?", (address,)).fetchone()
        if row:
            delay = min(30, 0.5 * 2 ** min(row[0], 6))
            with self.db:
                self.db.execute(
                    "UPDATE deferred SET attempts=attempts+1,retry_at=? WHERE address=?",
                    (time.time() + delay, address),
                )

    def update(self, address: str, status: str, receipt: str | None = None) -> None:
        with self.db:
            self.db.execute(
                "UPDATE actions SET status=?, receipt=COALESCE(?, receipt) WHERE address=?",
                (status, receipt, address),
            )

    def unfinished(self) -> list[tuple]:
        return self.db.execute(
            "SELECT address, status, receipt FROM actions WHERE status IN ('reserved','broadcasting','unknown')"
        ).fetchall()

    def close(self) -> None:
        self.db.close()


class Executor(Protocol):
    async def execute(self, address: str, checkpoint: Callable[[str], None]) -> str: ...


@dataclass(frozen=True)
class Observation:
    source: str
    text: str


class Pipeline:
    def __init__(self, store: Store, *, live: bool = False, executor: Executor | None = None,
                 verify: Callable[[str], Awaitable[bool]] | None = None,
                 ignored: frozenset[str] = frozenset()):
        self.store = store
        self.live = live
        self.executor = executor
        self.verify = verify
        self.ignored = ignored
        self.pending: set[str] = set()
        self.ambiguities: set[tuple] = set()

    async def process(self, observation: Observation, *, baseline: bool = False) -> None:
        addresses = tuple(a for a in extract_addresses(observation.text) if a not in self.ignored)
        if baseline:
            self.store.mark_seen(observation.source, addresses)
            return
        seen = self.store.seen(observation.source)
        fresh = tuple(a for a in addresses if a not in seen)
        if len(fresh) > 1:
            ambiguity = (observation.source, fresh)
            if ambiguity not in self.ambiguities:
                LOG.error("Ambiguous announcement from %s: %d new addresses; no execution", observation.source, len(fresh))
                self.ambiguities.add(ambiguity)
            return
        if not fresh:
            return
        address = fresh[0]
        self.store.enqueue(address, observation.source, self.live)
        await self.attempt(address, observation.source)

    async def attempt(self, address: str, source: str) -> None:
        if address in self.pending or not self.store.queued_address(address, self.live):
            return
        self.pending.add(address)
        try:
            if self.live:
                if self.verify is None or self.executor is None:
                    raise RuntimeError("LIVE pipeline requires verifier and executor")
                try:
                    valid = await self.verify(address)
                except Exception as exc:
                    self.store.defer(address)
                    LOG.warning("Mint lookup deferred: %s; candidate saved", type(exc).__name__)
                    return
                if not valid:
                    self.store.discard(address)
                    LOG.warning("Candidate is not a mint: %s", address)
                    return
            # No await between check/reserve: races across asyncio sources cannot double-send.
            if not self.store.reserve(address, source, self.live):
                self.store.discard(address)
                return
            self.store.discard(address)
            if not self.live:
                LOG.info("DRY-RUN candidate=%s source=%s (mint not verified; no send)", address, source)
                return
            LOG.info("LIVE candidate=%s source=%s", address, source)
            try:
                receipt = await self.executor.execute(
                    address, lambda value: self.store.update(address, "broadcasting", value)
                )
            except asyncio.CancelledError:
                self.store.update(address, "unknown")
                raise
            except Exception as exc:
                # Never auto-retry an uncertain Telegram send or blockchain broadcast.
                self.store.update(address, "unknown")
                LOG.error("Execution stopped: %s; LIVE slot remains locked; inspect journal", type(exc).__name__)
            else:
                self.store.update(address, "sent", receipt)
                LOG.info("Execution completed; LIVE slot remains locked")
        finally:
            self.pending.discard(address)

    async def retry_loop(self) -> None:
        while True:
            await asyncio.gather(*(self.attempt(address, source) for address, source in self.store.queued(self.live)))
            await asyncio.sleep(0.25)
