import os
import queue
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from alert_queue import AlertQueue


class QueueRegression(unittest.TestCase):
    def test_queue_full_releases_claim(self):
        q = AlertQueue(lambda _: None)
        event = {'symbol': 'BTCUSDT', 'route': 'test', 'evaluation_time_ms': 1}
        with patch.object(q.q, 'put_nowait', side_effect=queue.Full):
            self.assertFalse(q.enqueue(event))
        self.assertTrue(q.enqueue(event), 'full queue must not poison dedup')

    def test_duplicate_is_blocked(self):
        q = AlertQueue(lambda _: None)
        event = {'symbol': 'BTCUSDT', 'route': 'test', 'evaluation_time_ms': 2}
        self.assertTrue(q.enqueue(event))
        self.assertFalse(q.enqueue(event))

    def test_explicit_callback_failure_is_not_success(self):
        q = AlertQueue(lambda _: {'ok': False})
        q.max_retry = 0
        q.enqueue({'symbol': 'BTCUSDT', 'evaluation_time_ms': 3})
        q.start()
        q.q.join()
        q.stop()
        self.assertEqual(q.status()['success'], 0)
        self.assertEqual(q.status()['failure'], 1)

    def test_callback_success(self):
        q = AlertQueue(lambda _: {'ok': True})
        q.enqueue({'symbol': 'BTCUSDT', 'evaluation_time_ms': 4})
        q.start()
        q.q.join()
        q.stop()
        self.assertEqual(q.status()['success'], 1)


if __name__ == '__main__':
    unittest.main()
