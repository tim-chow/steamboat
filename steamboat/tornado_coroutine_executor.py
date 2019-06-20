# coding: utf8

import logging
import uuid
import tornado.gen as gen
from tornado.queues import QueueEmpty, QueueFull
from tornado.locks import Condition
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

            async_result = task_item.async_result
            async_result.set_time_info("consumed_from_queue_at")
            time_info_key = "executed_completion_at"
            try:
                result = yield task_item.function(
                    *task_item.args,
                    **task_item.kwargs)
                async_result.set_time_info(time_info_key).set_result(result)
            except Exception as ex:
                async_result.set_time_info(time_info_key).set_exception(ex)

        LOGGER.info("coroutine: %s is stopped" % coroutine_name)
        self._core_coroutines.pop(ind)
        if not self._core_coroutines:
            LOGGER.info("all coroutines in %s are stopped" %
                self._coroutine_pool_name)
            self._core_coroutines_condition.notify_all()

    def submit_task(self, function, *args, **kwargs):
        async_result = AsyncResult()
        if self._shutting_down or self._shuted_down:
            async_result.set_exception(ShutedDownError(self._coroutine_pool_name))
            return async_result
        if not gen.is_coroutine_function(function):
            async_result.set_exception(RuntimeError(
                "function must be tornado coroutine function"))
            return async_result

        is_full = False
        task_item = TaskItem(function, args, kwargs, async_result)
        try:
            self._queue.put_nowait(task_item)
            async_result.set_time_info("submitted_to_queue_at")
        except QueueFull:
            is_full = True

        if is_full:
            return self._reject_handler(self._queue, task_item)
        else:
            self._core_coroutines_wait_condition.notify()
            return async_result

    @gen.coroutine
    def shutdown(self, wait_time=None):
        if self._shutting_down or self._shuted_down:
            raise gen.Return()

        self._shutting_down = True
        self._shuted_down = False

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
                task_item.async_result.set_exception(
                    ShutedDownError(self._coroutine_pool_name))

        self._shutting_down = False
        self._shuted_down = True

