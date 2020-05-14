import logging
import datetime
import time
from unittest import TestCase, main
from Queue import Queue, Full

from steamboat.thread_pool_executor import ThreadPoolExecutor
from steamboat.cabin import CabinBuilder

LOGGER = logging.getLogger(__name__)


class CabinTest(TestCase):
    def setUp(self):
        def reject_handler(queue, task_item):
            raise Full

        self._thread_pool_executor = ThreadPoolExecutor(
            3, Queue(6), reject_handler)

        self._cabin = CabinBuilder() \
            .with_name("cabin") \
            .with_executor(self._thread_pool_executor) \
            .with_timeout(0.5) \
            .with_open_length(10) \
            .with_closed_length(2) \
            .with_half_open_length(3) \
            .with_failure_ratio_threshold(0.8) \
            .with_failure_count_threshold(5) \
            .with_half_failure_count_threshold(2) \
            .with_half_open_probability(0.5) \
            .build()

    def testTimeout(self):
        def sleep_for_a_moment(seconds):
            time.sleep(seconds)
        ars = []
        for _ in range(10):
            ar = self._cabin.execute(sleep_for_a_moment, 0.6)
            ars.append(ar)
        for ar in ars:
            LOGGER.info("time info: %s" % ar.time_info)
            exc = ar.exception()
            LOGGER.info("exception: %s(%s)" % (exc.__class__, str(exc)))

    def testCabin(self):
        def err_func(count):
            raise RuntimeError("err func %d" % count)

        def func(count):
            return "this is %d" % count

        LOGGER.info("start time: %s" % datetime.datetime.now())
        futures = []
        for ind in range(10):
            future = self._cabin.execute(err_func, ind)
            futures.append(future)
        for future in futures:
            LOGGER.info(future)
            try:
                LOGGER.info(future.result())
            except:
                LOGGER.error(future.exception())

        time.sleep(0.2)

        futures = []
        for ind in range(10):
            future = self._cabin.execute(func, ind)
            futures.append(future)

        for future in futures:
            LOGGER.info(future)
            try:
                LOGGER.info(future.result())
            except:
                LOGGER.error(future.exception())

    def tearDown(self):
        self._thread_pool_executor.shutdown()
        self._cabin.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")
    main()
