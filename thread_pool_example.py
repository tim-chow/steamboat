# coding: utf8

from Queue import Queue, Full
import time
import requests
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
    steamboat = create_steamboat(cabin, TestDegredationStrategy())

    @steamboat.push_into_cabin("cabin")
    def download(url):
        resp = requests.get(url)
        LOGGER.info("status code is: %d" % resp.status_code)

    url = "http://n.sinaimg.cn/test/320/w640h480/20190429/aabb-hwfpcxm9388795.jpg"
    ars = []
    for i in range(100):
        ar = download(url)
        ars.append(ar)
        time.sleep(0.05)
    for ar in ars:
        ar.result()
        LOGGER.info(ar.time_info)
    tpe.shutdown()
    cabin.shutdown()

if __name__ == "__main__":
    test()

