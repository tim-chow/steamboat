import logging
import datetime
import time
from unittest import TestCase, main
from Queue import Queue, Full

from steamboat.thread_pool_executor import ThreadPoolExecutor
from steamboat.cabin import CabinBuilder

LOGGER = logging.getLogger(__name__)


class CabinTest(TestCase):
    def testCabin(self):
        def reject_handler(queue, item):
            raise Full

        thread_pool_executor = ThreadPoolExecutor(
            3, Queue(6), reject_handler)

        cabin_test = CabinBuilder() \
            .with_name("cabin-test") \
            .with_executor(thread_pool_executor) \
            .with_open_length(10) \
            .with_closed_length(2) \
            .with_half_open_length(3) \
            .with_failure_ratio_threshold(0.8) \
            .with_failure_count_threshold(5) \
            .with_half_failure_count_threshold(2) \
            .with_half_open_probability(0.5) \
            .build()

        def err_func(count):
            raise RuntimeError("err_func: %d" % count)

        def func(count):
            return "this is %d" % count

        LOGGER.info("start time: %s" % datetime.datetime.now())
        futures = []
        for ind in range(10):
            future = cabin_test.execute(err_func, ind)
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
            future = cabin_test.execute(func, ind)
            futures.append(future)

        for future in futures:
            LOGGER.info(future)
            try:
                LOGGER.info(future.result())
            except:
                LOGGER.error(future.exception())

        thread_pool_executor.shutdown()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, 
        format="[%(asctime)s] [%(filename)s:%(lineno)d] %(msg)s",
        datefmt="%F %T")
    main()

