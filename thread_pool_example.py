# coding: utf8

from Queue import Queue, Full
import time
import random

from steamboat.thread_pool_executor import ThreadPoolExecutor
from example_base import *


def create_executor():
    # 创建Executor
    def reject_handler(queue, item):
        raise Full()

    tpe = ThreadPoolExecutor(
        core_pool_size=3,
        queue=Queue(6),
        reject_handler=reject_handler)
    return tpe


def test():
    tpe = create_executor()
    cabin = create_cabin(tpe)
    steamboat = create_steamboat(cabin, TestDegradationStrategy())

    @steamboat.push_into_cabin("cabin")
    def download(url):
        t = random.random()
        time.sleep(t)
        LOGGER.info("downloading %s using %fs", url, t)

    url = "https://www.timd.cn/"
    ars = []
    for i in range(100):
        ar = download(url)
        ars.append(ar)
        time.sleep(0.05)
    for ar in ars:
        try:
            ar.result()
        except:
            LOGGER.error(ar.exception())
        LOGGER.info(ar.time_info)
    tpe.shutdown()
    cabin.shutdown()


if __name__ == "__main__":
    test()
