import logging
from unittest import TestCase, main
import time
from Queue import Queue, Full
import random

from steamboat.steamboat import SteamBoat
from steamboat.cabin import CabinBuilder
from steamboat.degredation_strategy import DegredationStrategy
from steamboat.thread_pool_executor import ThreadPoolExecutor

LOGGER = logging.getLogger(__name__)


class TestDegredationStrategy(DegredationStrategy):
    def on_submit_task_error(self, exc, f, a, kw):
        LOGGER.error("submit task error: %s(%s)" % (exc.__class__, str(exc)))

    def on_window_half_open(self, f, a, kw):
        LOGGER.error("window was half open while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_window_closed(self, f, a, kw):
        LOGGER.error("window was closed while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_timeout_reached(self, f, a, kw):
        LOGGER.error("timeout was reached while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_exception(self, exc, f, a, kw):
        LOGGER.error("expcetion: [%s] was raised while invoking %s(*%s, **%s)"
            % (exc, f.__name__, a, kw))


class SteamBoatTest(TestCase):
    def setUp(self):
        def reject_handler(queue, item):
            raise Full
        self._thread_pool_executor = ThreadPoolExecutor(
            3, Queue(2), reject_handler)

        strategy = TestDegredationStrategy()

        cabin = CabinBuilder() \
            .with_name("cabin") \
            .with_executor(self._thread_pool_executor) \
            .with_open_length(1) \
            .with_half_open_length(0.4) \
            .with_closed_length(0.2) \
            .with_failure_ratio_threshold(0.95) \
            .with_failure_count_threshold(1) \
            .with_half_failure_count_threshold(1) \
            .build()
        steamboat = SteamBoat()
        steamboat.set_default_cabin(cabin, strategy)
        self._steamboat = steamboat

    def tearDown(self):
        self._thread_pool_executor.shutdown()

    def testSteamBoat(self):
        @self._steamboat.push_into_cabin("cabin")
        def test_func():
            time.sleep(random.random())

        fs = []
        for ind in range(100):
            fs.append(test_func())

        for f in fs:
            exc = f.exception()
            if exc is not None:
                LOGGER.error(exc)
            else:
                LOGGER.info(f.result())

        ar = self._steamboat.submit_task("cabin",
            lambda : time.sleep(random.random()))
        ar.result()
        LOGGER.info(ar.time_info)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, 
        format="[%(asctime)s] [%(filename)s:%(lineno)d] %(msg)s",
        datefmt="%F %T")
    main()

