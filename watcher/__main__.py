import argparse
import asyncio
import getpass
import logging
import os
from pathlib import Path

import aiohttp
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from .config import Config, load_env
from .core import Observation, Pipeline, Store, extract_addresses
from .network import Rpc, TelegramExecutor
from .sources import watch_telegram, watch_website, watch_x

LOG = logging.getLogger(__name__)


def telegram_client(config, *, request_retries=0):
    Path(config.session_path).parent.mkdir(parents=True, exist_ok=True)
    # Explicitly disable application retries for a send that could trigger a purchase.
    return TelegramClient(
        config.session_path, config.api_id, config.api_hash,
        request_retries=request_retries, connection_retries=5, retry_delay=1, flood_sleep_threshold=0,
    )


async def login(config):
    if not config.api_id or not config.api_hash:
        raise ValueError("Set TG_API_ID and TG_API_HASH locally before login")
    client = telegram_client(config, request_retries=3)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            phone = getpass.getpass("Telegram phone (hidden): ")
            sent = await client.send_code_request(phone)
            try:
                await client.sign_in(phone=phone, code=getpass.getpass("Telegram login code (hidden): "),
                                     phone_code_hash=sent.phone_code_hash)
            except SessionPasswordNeededError:
                await client.sign_in(password=getpass.getpass("Telegram 2FA password (hidden): "))
        LOG.info("Telegram session saved locally")
    finally:
        await client.disconnect()


async def run(config, *, live: bool, website_once: bool = False):
    if live:
        config.validate_live()
    store = Store(config.state_db)
    client = None
    tasks = []
    try:
        if store.unfinished():
            LOG.warning("Journal has an unfinished execution; automatic resend is blocked")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8),
            headers={"User-Agent": "SolanaAnnouncementWatcher/0.1"},
        ) as http:
            executor = None
            if config.api_id and config.api_hash and not website_once:
                client = telegram_client(config)
                await client.connect()
                if not await client.is_user_authorized():
                    raise ValueError("Telegram is not logged in; run: python -m watcher login")
                if live:
                    target = await client.get_entity(config.trading_bot)
                    if not getattr(target, "bot", False):
                        raise ValueError("TRADING_BOT must resolve to a Telegram bot")
                    executor = TelegramExecutor(client, target)
            pipeline = Pipeline(
                store, live=live, executor=executor,
                verify=Rpc(http, config.rpc_url).is_mint if config.rpc_url else None,
                ignored=config.ignored,
            )
            LOG.info("Mode: %s", "LIVE; one send per launch" if live else "DRY-RUN; sending disabled")
            for url in config.website_urls:
                tasks.append(asyncio.create_task(watch_website(
                    http, pipeline, url, config.website_poll, once=website_once,
                )))
            if client and config.channels:
                tasks.append(asyncio.create_task(watch_telegram(client, pipeline, config.channels)))
            elif not website_once:
                LOG.warning("Telegram source disabled: credentials not configured")
            if config.x_bearer and not website_once:
                tasks.append(asyncio.create_task(watch_x(
                    http, pipeline, config.x_username, config.x_bearer, config.x_poll,
                )))
            elif not website_once:
                LOG.warning("X source disabled: X_BEARER_TOKEN not configured")
            if not tasks:
                raise ValueError("No sources enabled")
            if not website_once:
                tasks.append(asyncio.create_task(pipeline.retry_loop()))
            try:
                if website_once:
                    await asyncio.gather(*tasks)
                else:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                    raise RuntimeError("Source unexpectedly stopped")
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if client:
            await client.disconnect()
        store.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Watch announcements and forward one Solana mint to a Telegram bot")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("login", help="Interactive local Telegram login; no messages sent")
    runner = commands.add_parser("run", help="Start configured sources in dry-run by default")
    runner.add_argument("--live", action="store_true", help="Enable sending a CA to the configured trading bot")
    commands.add_parser("website-once", help="Fetch website once without Telegram login or sending")
    commands.add_parser("status", help="Inspect local execution journal; no network requests")
    extraction = commands.add_parser("extract", help="Offline address extraction")
    extraction.add_argument("text")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    try:
        if args.command == "extract":
            for address in extract_addresses(args.text):
                print(address)
            return
        load_env(args.env_file)
        config = Config.from_env()
        if args.command == "status":
            store = Store(config.state_db)
            try:
                rows = store.db.execute("SELECT address,status,receipt FROM actions ORDER BY created").fetchall()
                print(f"Actions: {len(rows)}; queued candidates: {store.db.execute('SELECT count(*) FROM deferred').fetchone()[0]}")
                for address, status, receipt in rows:
                    print(f"{status} {address} {receipt or '-'}")
            finally:
                store.close()
        elif args.command == "login":
            asyncio.run(login(config))
        else:
            asyncio.run(run(config, live=getattr(args, "live", False), website_once=args.command == "website-once"))
    except KeyboardInterrupt:
        LOG.info("Stopped")
    except ValueError:
        LOG.error("Invalid configuration/input; check .env, Telegram login and required fields")
        raise SystemExit(1)
    except Exception as exc:
        LOG.error("Stopped: %s (details suppressed to keep credentials private)", type(exc).__name__)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
