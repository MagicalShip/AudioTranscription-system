"""Isolated worker tests; teammates' unfinished infrastructure is replaced explicitly."""
import asyncio
from contextlib import nullcontext
import importlib.util
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
class FakeMessage:
    def __init__(self, body, **kwargs):
        self.body = body
        self.__dict__.update(kwargs)

stubs = {
    'aio_pika': types.SimpleNamespace(DeliveryMode=types.SimpleNamespace(PERSISTENT=2), ExchangeType=types.SimpleNamespace(DIRECT='direct'), Message=FakeMessage),
    'prometheus_client': types.SimpleNamespace(Counter=Mock(), Gauge=Mock(), Histogram=Mock(), start_http_server=Mock()),
    'common.config': types.SimpleNamespace(MAX_RETRIES=2, RETRY_BASE_SECONDS=0, RABBITMQ_URL='unused'),
    'common.database': types.SimpleNamespace(connection=Mock(), wait_for_postgres=Mock()),
    'common.storage': types.SimpleNamespace(download_file=Mock(), wait_for_storage=Mock()),
}
def load_isolated(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

with patch.dict(sys.modules, stubs):
    messaging = load_isolated('_test_messaging', 'common/messaging.py')
    with patch.dict(sys.modules, {'common.messaging': messaging}):
        worker = load_isolated('_test_worker', 'transcription_service/main.py')

PAYLOAD = dict(event_type='segment.ready', job_id='job', object_name='audio.wav',
               segment_number=0, start_ms=0, end_ms=1000, audio_seconds=1)
ROW = dict(job_id='job', segment_number=0, start_ms=0, end_ms=1000,
           text='stored', model_version='test', processing_seconds=1)

class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_drains_thread(self):
        done = threading.Event()
        def slow():
            time.sleep(.04)
            done.set()
        with self.assertRaises(TimeoutError):
            await worker.run_blocking(slow, timeout=.001)
        self.assertTrue(done.is_set())

    async def exercise(self, cached=None, failure=None, invalid=False, retry=True, download_error=None):
        message = Mock(processed=False, ack=AsyncMock(), nack=AsyncMock())
        transcriber = Mock(model_version='test')
        transcriber.transcribe.return_value = 'new'
        transcriber.transcribe.side_effect = failure
        with (patch.object(worker, 'decode', return_value={} if invalid else PAYLOAD),
              patch.object(worker, 'cached_result', return_value=cached),
              patch.object(worker, 'save_result', return_value=ROW),
              patch.object(worker, 'download_file', side_effect=download_error),
              patch.object(worker.tempfile, 'TemporaryDirectory', return_value=nullcontext('unused')),
              patch.object(worker, 'publish', new_callable=AsyncMock) as publish,
              patch.object(worker, 'dead_letter_invalid', new_callable=AsyncMock) as dead,
              patch.object(worker, 'retry_or_dead_letter', new_callable=AsyncMock, return_value=retry)):
            await worker.handle_message(message, Mock(), Mock(), transcriber)
            return message, transcriber, publish, dead

    async def test_cached_result_skips_inference(self):
        message, model, publish, _ = await self.exercise(cached=ROW)
        model.transcribe.assert_not_called()
        self.assertEqual(publish.call_args.args[2]['text'], 'stored')
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()

    async def test_conflicting_insert_publishes_stored_result(self):
        message, _, publish, _ = await self.exercise()
        self.assertEqual(publish.call_args.args[2]['text'], 'stored')
        message.ack.assert_awaited_once()

    async def test_invalid_message_goes_to_dead_letter(self):
        message, model, _, dead = await self.exercise(invalid=True)
        dead.assert_awaited_once()
        model.transcribe.assert_not_called()
        message.ack.assert_awaited_once()

    async def test_model_failure_retries_without_terminal_event(self):
        message, _, publish, _ = await self.exercise(failure=RuntimeError('model'))
        publish.assert_not_awaited()
        message.ack.assert_awaited_once()

    async def test_exhausted_failure_publishes_terminal_event(self):
        message, _, publish, _ = await self.exercise(failure=RuntimeError('model'), retry=False)
        self.assertEqual(publish.call_args.args[1], 'segment.failed')
        message.ack.assert_awaited_once()

    async def test_publish_failure_requeues_without_ack(self):
        message = Mock(processed=False, ack=AsyncMock(), nack=AsyncMock())
        with (patch.object(worker, 'decode', return_value=PAYLOAD),
              patch.object(worker, 'cached_result', return_value=ROW),
              patch.object(worker, 'publish', new_callable=AsyncMock, side_effect=RuntimeError('broker'))):
            await worker.handle_message(message, Mock(), Mock(), Mock(model_version='test'))
        message.nack.assert_awaited_once_with(requeue=True)
        message.ack.assert_not_awaited()

    async def test_database_failure_requeues(self):
        message = Mock(processed=False, ack=AsyncMock(), nack=AsyncMock())
        with (patch.object(worker, 'decode', return_value=PAYLOAD),
              patch.object(worker, 'cached_result', side_effect=RuntimeError('database'))):
            await worker.handle_message(message, Mock(), Mock(), Mock(model_version='test'))
        message.nack.assert_awaited_once_with(requeue=True)
        message.ack.assert_not_awaited()

    async def test_retry_backoff_and_dead_letter(self):
        for attempt in range(3):
            message = Mock(headers={'retry_count': attempt})
            with (patch.object(messaging, 'publish', new_callable=AsyncMock) as publish,
                  patch.object(messaging.asyncio, 'sleep', new_callable=AsyncMock) as sleep,
                  patch.object(messaging, 'RETRY_BASE_SECONDS', 2)):
                result = await messaging.retry_or_dead_letter(Mock(), message, 'segment.ready', PAYLOAD)
                self.assertEqual(result, attempt < 2)
                if attempt < 2:
                    sleep.assert_awaited_once_with(2 * 2**attempt)
                    self.assertEqual(publish.call_args.kwargs['headers']['retry_count'], attempt + 1)
                else:
                    sleep.assert_not_awaited()
                self.assertEqual(publish.call_args.args[1], 'segment.ready' if attempt < 2 else 'transcription.segments')

    async def test_repeated_cancellation_drains_thread(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        def work():
            started.set()
            release.wait(2)
            finished.set()
        task = asyncio.create_task(worker.run_blocking(work))
        while not started.is_set():
            await asyncio.sleep(.001)
        try:
            task.cancel()
            await asyncio.sleep(.01)
            task.cancel()
            await asyncio.sleep(.01)
            self.assertFalse(task.done())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(finished.is_set())

    async def test_missing_audio_uses_bounded_retry(self):
        message, model, publish, _ = await self.exercise(download_error=FileNotFoundError('missing'))
        model.transcribe.assert_not_called()
        publish.assert_not_awaited()
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()

    async def test_failure_metric_does_not_prevent_ack(self):
        with patch.object(worker, 'RESULTS') as results:
            results.labels.side_effect = RuntimeError('metrics')
            message, _, _, _ = await self.exercise(failure=RuntimeError('model'), retry=False)
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()

    async def test_dead_letter_preserves_raw_body(self):
        message = Mock(body=b'not-json', content_type='application/json',
                       correlation_id='job', message_id='event')
        exchange = Mock(publish=AsyncMock())
        channel = Mock(get_exchange=AsyncMock(return_value=exchange))
        await messaging.dead_letter_invalid(channel, message, 'transcription.segments', 'invalid')
        sent = exchange.publish.call_args.args[0]
        self.assertEqual(sent.body, b'not-json')
        self.assertEqual(sent.correlation_id, 'job')
        self.assertEqual(exchange.publish.call_args.kwargs['routing_key'], 'transcription.segments')

class ValidationTests(unittest.TestCase):
    def test_invalid_duration_and_indices(self):
        for field, value in [('audio_seconds', float('nan')), ('audio_seconds', 0),
                             ('segment_number', True), ('end_ms', 0), ('job_id', '')]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                worker.validate_segment(dict(PAYLOAD, **{field: value}))

    def test_model_initializes_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from transcription_service.transcriber import Transcriber
        constructor = Mock()
        def build(*args, **kwargs):
            time.sleep(.02)
            return Mock(transcribe=Mock(return_value=([], None)))
        constructor.side_effect = build
        with (patch.dict('os.environ', {'TRANSCRIPTION_PROVIDER': 'faster_whisper'}),
              patch.dict(sys.modules, {'faster_whisper': types.SimpleNamespace(WhisperModel=constructor)})):
            model = Transcriber()
            with ThreadPoolExecutor(3) as pool:
                list(pool.map(lambda n: model.transcribe(Path('unused'), n), range(3)))
        self.assertEqual(constructor.call_count, 1)

if __name__ == '__main__':
    unittest.main()
