# coding: utf8

from tornado.ioloop import IOLoop
from tornado.queues import Queue, QueueFull
from tornado.gen import coroutine, sleep
from tornado.httpclient import AsyncHTTPClient
from steamboat.tornado_coroutine_executor import TornadoCoroutineExecutor
from example_base import *

# AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

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
    steamboat = create_steamboat(cabin, TestDegredationStrategy())

    @steamboat.push_into_cabin("cabin")
    @coroutine
    def download(url):
        client = AsyncHTTPClient()
        resp = yield client.fetch(url)
        LOGGER.info("status code is: %d" % resp.code)

    url = "http://n.sinaimg.cn/test/320/w640h480/20190429/aabb-hwfpcxm9388795.jpg"
    ars = []
    for i in range(100):
        ar = download(url)
        ars.append(ar)
        yield sleep(0.05)
    yield ars
    for ar in ars:
        LOGGER.info(ar.time_info)
    yield tce.shutdown()
    LOGGER.info("stop IOLoop")
    IOLoop.instance().stop()

if __name__ == "__main__":
    test()
    IOLoop.instance().start()

