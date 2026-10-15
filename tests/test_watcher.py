import asyncio
from dataclasses import replace
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from watcher.config import Config, load_env
from watcher.core import Observation, Pipeline, Store, extract_addresses, html_text, is_address
from watcher.network import Rpc, TOKEN_PROGRAMS, TelegramExecutor
from watcher.sources import document_text, post_text, telegram_text, watch_website, watch_x

# Synthetic public keys, never sent to an actual bot/network.
A = "11111111111111111111111111111112"
B = "11111111111111111111111111111113"


class ExtractionTests(unittest.TestCase):
    def test_base58_size_boundaries_and_duplicates(self):
        self.assertTrue(is_address(A))
        self.assertFalse(is_address("1" * 44))
        self.assertFalse(is_address("O" * 32))
        self.assertEqual(extract_addresses(f"CA: {A}\nhttps://example.test/token/{A}"), (A,))
        self.assertEqual(extract_addresses("z" + A + "z"), ())

    def test_html_and_json(self):
        html = f'<style>{B}</style><script>{B}</script><a href="/token/{A}">Buy</a>'
        self.assertEqual(extract_addresses(html_text(html)), (A,))
        self.assertEqual(extract_addresses(document_text('{"mint":"' + A + '"}', "application/json")), (A,))

    def test_hidden_telegram_urls_and_x_long_posts(self):
        message = SimpleNamespace(raw_text="CA", entities=[SimpleNamespace(url=f"https://test/{A}")], reply_markup=None)
        self.assertEqual(extract_addresses(telegram_text(message)), (A,))
        self.assertEqual(extract_addresses(post_text({"text": "truncated", "note_post": {"text": A}})), (A,))

    def test_env_file_is_not_executed_and_does_not_override_environment(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"TG_API_ID": "123"}, clear=True):
            path = Path(folder) / ".env"
            path.write_text('TG_API_ID=456\nTG_API_HASH="$(touch should-not-exist)"\n')
            load_env(path)
            self.assertEqual(os.environ["TG_API_ID"], "123")
            self.assertEqual(os.environ["TG_API_HASH"], "$(touch should-not-exist)")
            self.assertFalse((Path(folder) / "should-not-exist").exists())

    def test_live_requires_account_bot_and_rpc(self):
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_env()
        with self.assertRaises(ValueError):
            config.validate_live()
        replace(config, api_id=1, api_hash="test", trading_bot="@test_bot", rpc_url="https://example.test").validate_live()


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = str(Path(self.folder.name) / "state.sqlite3")
        self.store = Store(self.path)
        self.executor = SimpleNamespace(execute=AsyncMock(return_value="telegram-message:1"))
        self.verify = AsyncMock(return_value=True)
        self.pipeline = Pipeline(self.store, live=True, executor=self.executor, verify=self.verify)

    async def asyncTearDown(self):
        self.store.close()
        self.folder.cleanup()

    async def test_three_sources_race_sends_once(self):
        await asyncio.gather(*(self.pipeline.process(Observation(source, A)) for source in ("tg", "web", "x")))
        self.executor.execute.assert_awaited_once()
        self.assertEqual(self.executor.execute.call_args.args[0], A)

    async def test_two_different_mints_still_one_launch_send(self):
        await asyncio.gather(self.pipeline.process(Observation("tg", A)), self.pipeline.process(Observation("web", B)))
        self.executor.execute.assert_awaited_once()

    async def test_startup_baseline_and_later_new_address(self):
        await self.pipeline.process(Observation("web", A), baseline=True)
        await self.pipeline.process(Observation("web", A))
        self.executor.execute.assert_not_awaited()
        await self.pipeline.process(Observation("web", f"{A} {B}"))
        self.assertEqual(self.executor.execute.call_args.args[0], B)

    async def test_restart_after_delivery_does_not_send_again(self):
        await self.pipeline.process(Observation("tg", A))
        self.store.close()
        self.store = Store(self.path)
        restarted = Pipeline(self.store, live=True, executor=self.executor, verify=self.verify)
        await restarted.process(Observation("x", A))
        self.executor.execute.assert_awaited_once()

    async def test_timeout_and_restart_lock_the_launch(self):
        self.executor.execute.side_effect = TimeoutError
        await self.pipeline.process(Observation("tg", A))
        self.assertEqual(self.store.unfinished()[0][1], "unknown")
        self.store.close()
        self.store = Store(self.path)
        restarted = Pipeline(self.store, live=True, executor=self.executor, verify=self.verify)
        await restarted.process(Observation("x", A))
        await restarted.process(Observation("web", B))
        self.executor.execute.assert_awaited_once()

    async def test_crash_after_reservation_does_not_send(self):
        self.store.reserve(A, "tg", True)
        await self.pipeline.process(Observation("x", A))
        await self.pipeline.process(Observation("web", B))
        self.executor.execute.assert_not_awaited()

    async def test_rpc_failure_survives_restart_and_source_checkpoint(self):
        self.verify.side_effect = RuntimeError("temporarily not visible")
        await self.pipeline.process(Observation("web", A))
        self.store.set_checkpoint("web", "etag-already-advanced")
        self.assertTrue(self.store.queued_address(A, True))
        self.store.close()
        self.store = Store(self.path)
        self.verify.side_effect = None
        restarted = Pipeline(self.store, live=True, executor=self.executor, verify=self.verify)
        await restarted.attempt(A, "web")
        self.executor.execute.assert_awaited_once()
        self.assertFalse(self.store.queued_address(A, True))

    async def test_non_mint_is_discarded(self):
        self.verify.return_value = False
        await self.pipeline.process(Observation("tg", A))
        self.executor.execute.assert_not_awaited()
        self.assertFalse(self.store.queued_address(A, True))

    async def test_ambiguous_post_can_be_clarified_later(self):
        await self.pipeline.process(Observation("tg", f"{A} {B}"))
        self.executor.execute.assert_not_awaited()
        await self.pipeline.process(Observation("tg", A))
        self.executor.execute.assert_awaited_once()

    async def test_dry_run_is_not_replayed_live(self):
        dry = Pipeline(self.store)
        await dry.process(Observation("web", A))
        await self.pipeline.process(Observation("tg", A))
        self.executor.execute.assert_not_awaited()

    async def test_cancel_during_send_is_unknown(self):
        self.executor.execute.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self.pipeline.process(Observation("tg", A))
        self.assertEqual(self.store.unfinished()[0][1], "unknown")


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_checks_mint_type_and_program_owner(self):
        rpc = Rpc(None, "https://example.test")
        mint = {"result": {"value": {"owner": next(iter(TOKEN_PROGRAMS)), "data": {"parsed": {"type": "mint"}}}}}
        with patch("watcher.network.json_request", AsyncMock(return_value=mint)):
            self.assertTrue(await rpc.is_mint(A))
            mint["result"]["value"]["data"]["parsed"]["type"] = "account"
            self.assertFalse(await rpc.is_mint(A))
            mint["result"]["value"]["data"]["parsed"]["type"] = "mint"
            mint["result"]["value"]["owner"] = "not-a-token-program"
            self.assertFalse(await rpc.is_mint(A))

    async def test_missing_mint_defers_instead_of_rejecting(self):
        rpc = Rpc(None, "https://example.test")
        with patch("watcher.network.json_request", AsyncMock(return_value={"result": {"value": None}})), \
             patch("watcher.network.asyncio.sleep", AsyncMock()):
            with self.assertRaises(RuntimeError):
                await rpc.is_mint(A)

    async def test_sender_sends_only_address(self):
        client = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(id=123)))
        checkpoints = []
        receipt = await TelegramExecutor(client, "target").execute(A, checkpoints.append)
        client.send_message.assert_awaited_once_with("target", A, parse_mode=None, link_preview=False)
        self.assertEqual(receipt, "telegram-message:123")
        self.assertEqual(checkpoints, ["telegram-send-pending"])

    async def test_x_baseline_then_new_post(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(str(Path(folder) / "state.sqlite3"))
            pipeline = Pipeline(store)
            calls = AsyncMock(side_effect=[
                {"data": {"id": "42"}},
                {"data": [{"id": "100", "text": A}]},
                {"data": [{"id": "101", "text": B}]},
                asyncio.CancelledError,
            ])
            try:
                with patch("watcher.sources.json_request", calls), patch("watcher.sources.asyncio.sleep", AsyncMock()):
                    with self.assertRaises(asyncio.CancelledError):
                        await watch_x(None, pipeline, "test", "test-bearer", 1)
                self.assertEqual(store.checkpoint("x:test"), "101")
                self.assertEqual(store.db.execute("SELECT address FROM actions").fetchall(), [(B,)])
                self.assertEqual(calls.call_args_list[2].kwargs["params"]["since_id"], "100")
            finally:
                store.close()

    async def test_real_http_website_baseline_then_update(self):
        from aiohttp import ClientSession, web
        state = {"body": f"<p>{A}</p>"}
        async def page(request):
            return web.Response(text=state["body"], content_type="text/html")
        app = web.Application()
        app.router.add_get("/", page)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            with tempfile.TemporaryDirectory() as folder:
                store = Store(str(Path(folder) / "state.sqlite3"))
                try:
                    pipeline = Pipeline(store)
                    async with ClientSession() as http:
                        url = f"http://127.0.0.1:{port}/"
                        await watch_website(http, pipeline, url, 1, once=True)
                        self.assertEqual(store.db.execute("SELECT count(*) FROM actions").fetchone()[0], 0)
                        state["body"] = f"<p>{A}</p><p>{B}</p>"
                        await watch_website(http, pipeline, url, 1, once=True)
                        self.assertEqual(store.db.execute("SELECT address FROM actions").fetchall(), [(B,)])
                finally:
                    store.close()
        finally:
            await runner.cleanup()


if __name__ == "__main__":
    unittest.main()
