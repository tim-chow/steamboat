# coding: utf8

import time
import random

from tornado.ioloop import IOLoop
from tornado.queues import Queue, QueueFull
from tornado.gen import coroutine, sleep

from steamboat.tornado_coroutine_executor import TornadoCoroutineExecutor
from example_base import *


def create_executor():
    # 创建Executor
    def reject_handler(queue, item):
        raise QueueFull()

    tce = TornadoCoroutineExecutor(
        core_pool_size=3,
        queue=Queue(6),
        reject_handler=reject_handler)
    return tce


@coroutine
def test():
    tce = create_executor()
    cabin = create_cabin(tce)
    steamboat = create_steamboat(cabin, TestDegradationStrategy())

    @steamboat.push_into_cabin("cabin")
    @coroutine
    def download(url):
        t = random.random()
        time.sleep(t)
        LOGGER.info("downloading %s using %fs", url, t)

    url = "https://www.timd.cn/"
    ars = []
    for i in range(100):
        ar = download(url)
        ars.append(ar)
        yield sleep(0.05)
    yield ars
    for ar in ars:
        LOGGER.info(ar.time_info)
    yield tce.shutdown()
    cabin.shutdown()
    LOGGER.info("stop IOLoop")
    IOLoop.instance().stop()


if __name__ == "__main__":
    test()
    IOLoop.instance().start()
