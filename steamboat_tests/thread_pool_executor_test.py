import logging
from Queue import Queue
import time
import random
import unittest

from steamboat.thread_pool_executor import ThreadPoolExecutor

LOGGER = logging.getLogger(__name__)


class ThreadPoolExecutorTest(unittest.TestCase):
    def testThreadPoolExecutor(self):
        def reject_handler(queue, task_item):
            LOGGER.debug("reject handler is called")
            queue.put(task_item)

        def func(ind):
            time.sleep(random.random())
            return "this is %d" % ind

        executor = ThreadPoolExecutor(6, Queue(4), reject_handler)
        futures = []
        for ind in range(15):
            future = executor.submit_task(func, ind)
            futures.append(future)

        for future in futures:
            LOGGER.info(future)
            try:
                LOGGER.info(future.result())
            except Exception:
                LOGGER.error(future.exception())

        executor.shutdown()

        future = executor.submit_task(func, 100)
        LOGGER.info(future.exception())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")
    unittest.main()
