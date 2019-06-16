# coding: utf8

import logging
import uuid
import tornado.gen as gen
from tornado.queues import QueueEmpty, QueueFull
from tornado.locks import Condition
from tornado.concurrent import is_future, Future
from .executor import *

LOGGER = logging.getLogger(__name__)


class TornadoCoroutineExecutor(Executor):
    def __init__(self,
                 core_pool_size,
                 queue,
                 reject_handler,
                 coroutine_pool_name=None):
        self._core_pool_size = core_pool_size
        self._queue = queue
        self._reject_handler = reject_handler
        self._coroutine_pool_name = coroutine_pool_name or \
            'tornado-coroutine-pool-%s' % uuid.uuid1().hex
        self._core_coroutines_condition = Condition()
        self._core_coroutines = {}
        self._core_coroutines_wait_condition = Condition()
        self._shutting_down = False
        self._shuted_down = False
        self._initialize_core_coroutines()

    def _initialize_core_coroutines(self):
        for ind in range(self._core_pool_size):
            self._core_coroutines[ind] = self._core_coroutine_run(ind)
            LOGGER.info("core coroutine: %s is intialized" %
                self._get_coroutine_name(ind))

    def _get_coroutine_name(self, ind):
        return '%s:%d' % (self._coroutine_pool_name, ind)

    @gen.coroutine
    def _core_coroutine_run(self, ind):
        coroutine_name = self._get_coroutine_name(ind)
        while not self._shutting_down and not self._shuted_down:
            try:
                task_item = self._queue.get_nowait()
            except QueueEmpty:
                LOGGER.debug("coroutine: %s will enter into waiting pool" %
                    coroutine_name)
                yield self._core_coroutines_wait_condition.wait()
                LOGGER.debug("coroutine: %s was woken up from waiting pool" %
                    coroutine_name)
                continue

            try:
                result = yield task_item.function(
                    *task_item.args,
                    **task_item.kwargs)
                task_item.future.set_result(result)
            except Exception as ex:
                task_item.future.set_exception(ex)

        LOGGER.info("coroutine: %s is stopped" % coroutine_name)
        self._core_coroutines.pop(ind)
        if not self._core_coroutines:
            LOGGER.info("all coroutines in %s are stopped" %
                self._coroutine_pool_name)
            self._core_coroutines_condition.notify_all()

    def submit_task(self, function, *args, **kwargs):
        future = Future()
        if self._shutting_down or self._shuted_down:
            future.set_exception(ShutedDownError(self._coroutine_pool_name))
            return future
        if not gen.is_coroutine_function(function):
            future.set_exception(RuntimeError(
                "function must be tornado coroutine function"))
            return future

        is_full = False
        task_item = TaskItem(function, args, kwargs, future)
        try:
            self._queue.put_nowait(task_item)
        except QueueFull:
            is_full = True

        if is_full:
            return self._reject_handler(self._queue, task_item)
        else:
            self._core_coroutines_wait_condition.notify()
            return future

    @gen.coroutine
    def shutdown(self, wait_time=None):
        if self._shutting_down or self._shuted_down:
            raise gen.Return()

        self._shutting_down = True

        LOGGER.info("begin to notify all coroutines")
        self._core_coroutines_wait_condition.notify_all()
        if self._core_coroutines:
            yield self._core_coroutines_condition.wait(wait_time)

        while True:
            try:
                task_item = self._queue.get_nowait()
            except QueueEmpty:
                break
            else:
                task_item.future.set_exception(
                    ShutedDownError(self._coroutine_pool_name))

        self._shutting_down = False
        self._shuted_down = True

