# coding: utf8

import logging
import uuid

import tornado.gen as gen
from tornado.queues import QueueEmpty, QueueFull
from tornado.locks import Condition

from .executor import *

LOGGER = logging.getLogger(__name__)


class TornadoCoroutineExecutor(Executor):
    def __init__(
            self,
            core_pool_size,
            queue,
            reject_handler,
            coroutine_pool_name=None):
        self._core_pool_size = core_pool_size
        self._queue = queue
        self._reject_handler = reject_handler
        self._coroutine_pool_name = coroutine_pool_name or \
            'tornado-coroutine-pool-%s' % uuid.uuid1().hex
        self._core_coroutine_condition = Condition()
        self._core_coroutines = {}
        self._core_coroutine_wait_condition = Condition()
        self._shutting_down = False
        self._shut_down = False
        self._initialize_core_coroutines()

    def _initialize_core_coroutines(self):
        for coroutine_id in range(self._core_pool_size):
            self._core_coroutines[coroutine_id] = self._core_coroutine_run(coroutine_id)
            LOGGER.info("core coroutine %s is initialized",
                        self._get_coroutine_name(coroutine_id))

    def _get_coroutine_name(self, coroutine_id):
        return '%s-%d' % (self._coroutine_pool_name, coroutine_id)

    @gen.coroutine
    def _core_coroutine_run(self, coroutine_id):
        coroutine_name = self._get_coroutine_name(coroutine_id)
        while not self._shutting_down and not self._shut_down:
            try:
                task_item = self._queue.get_nowait()
            except QueueEmpty:
                if self._shutting_down or self._shut_down:
                    break
                LOGGER.debug("coroutine %s will enter into waiting pool",
                             coroutine_name)
                yield self._core_coroutine_wait_condition.wait()
                LOGGER.debug("coroutine %s was woken up",
                             coroutine_name)
                continue

            async_result = task_item.async_result
            async_result.set_time_info("consumed_from_queue_at")
            try:
                if not async_result.set_running_or_notify_cancel():
                    continue
            except RuntimeError:
                continue
            time_info_key = "executed_completion_at"
            try:
                result = yield task_item.function(
                    *task_item.args,
                    **task_item.kwargs)
                async_result.set_time_info(time_info_key).set_result(result)
            except Exception as ex:
                async_result.set_time_info(time_info_key).set_exception(ex)

        LOGGER.info("coroutine %s is stopped",
                    coroutine_name)
        self._core_coroutines.pop(coroutine_id)
        if not self._core_coroutines:
            LOGGER.info("all coroutines in %s are stopped",
                        self._coroutine_pool_name)
            self._core_coroutine_condition.notify_all()

    def submit_task(self, func, *args, **kwargs):
        async_result = AsyncResult()
        if self._shutting_down or self._shut_down:
            async_result.set_exception(ShutDownError(self._coroutine_pool_name))
            return async_result
        if not gen.is_coroutine_function(func):
            async_result.set_exception(RuntimeError(
                "function must be tornado coroutine function"))
            return async_result

        is_full = False
        task_item = TaskItem(func, args, kwargs, async_result)
        try:
            self._queue.put_nowait(task_item)
            async_result.set_time_info("submitted_to_queue_at")
        except QueueFull:
            is_full = True

        if is_full:
            self._reject_handler(self._queue, task_item)

        self._core_coroutine_wait_condition.notify()
        return async_result

    @gen.coroutine
    def shutdown(self, wait_time=None):
        if self._shutting_down or self._shut_down:
            raise gen.Return()

        self._shutting_down = True

        LOGGER.info("begin to notify all coroutines")
        self._core_coroutine_wait_condition.notify_all()
        if self._core_coroutines:
            yield self._core_coroutine_condition.wait(wait_time)

        while True:
            try:
                task_item = self._queue.get_nowait()
            except QueueEmpty:
                break
            else:
                task_item.async_result.set_exception(
                    ShutDownError(self._coroutine_pool_name))

        self._shutting_down = False
        self._shut_down = True
