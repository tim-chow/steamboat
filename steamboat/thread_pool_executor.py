# coding: utf8

import logging
import uuid
import threading
from Queue import Full, Empty
from .executor import *

LOGGER = logging.getLogger(__name__)


class ThreadPoolExecutor(Executor):
    def __init__(self,
            core_pool_size,
            queue,
            reject_handler,
            thread_pool_name=None):
        """
        @param core_pool_size int 核心线程数
        @param queue Queue 提交任务时，会将TaskItem放入到该队列，
            线程池中的核心线程会从该队列中消费消息
        @param reject_handler 回调函数 当queue满了的时候，
            再向线程池提交任务，就会使用(queue，task_item)调用该回调函数
        @param thread_pool_name string/None 线程池的名称，
            同时也是核心线程的名字的前缀
        """
        self._core_pool_size = core_pool_size
        self._queue = queue
        self._reject_handler = reject_handler
        self._thread_pool_name = thread_pool_name or \
            "thread-pool-%s" % uuid.uuid1().hex

        self._core_threads_condition = threading.Condition()
        self._core_threads = {}
        self._core_threads_wait_condition = threading.Condition()

        self._shutdown_lock = threading.Lock()
        self._shutting_down = False
        self._shuted_down = False

        self._initialize_core_threads()

    def _initialize_core_threads(self):
        for ind in range(self._core_pool_size):
            thread_name = self._get_thread_name(ind)
            core_thread = threading.Thread(
                target=self._core_thread_run,
                args=(ind, ))
            core_thread.setName(thread_name)
            core_thread.setDaemon(True)
            with self._core_threads_condition:
                self._core_threads[ind] = core_thread
            core_thread.start()
            LOGGER.debug("core thread: %s is initialized" % thread_name)

    def _get_thread_name(self, ind):
        return "%s:%d" % (self._thread_pool_name, ind)

    def _core_thread_run(self, ind):
        thread_name = self._get_thread_name(ind)
        while not self._shutting_down and not self._shuted_down:
            try:
                task_item = self._queue.get_nowait()
            except Empty:
                LOGGER.debug("thread: %s will enter into waiting pool" % thread_name)
                with self._core_threads_wait_condition:
                    if self._shutting_down or self._shuted_down:
                        break
                    self._core_threads_wait_condition.wait()
                LOGGER.debug("thread: %s was woken up from waiting pool" % thread_name)
                continue

            async_result = task_item.async_result
            async_result.set_time_info("consumed_from_queue_at")
            if not async_result.set_running_or_notify_cancel():
                continue
            time_info_key = "executed_completion_at"
            try:
                result = task_item.function(
                    *task_item.args,
                    **task_item.kwargs)
            except BaseException as ex:
                async_result.set_time_info(time_info_key).set_exception(ex)
            else:
                async_result.set_time_info(time_info_key).set_result(result)

        LOGGER.info("thread: %s is stopped" % thread_name)
        with self._core_threads_condition:
            self._core_threads.pop(ind)
            if not self._core_threads:
                LOGGER.info("all core threads in %s are stopped" % 
                    self._thread_pool_name)
                self._core_threads_condition.notify_all()

    def submit_task(self, function, *args, **kwargs):
        async_result = AsyncResult()
        if self._shutting_down or self._shuted_down:
            async_result.set_exception(ShutedDownError(self._thread_pool_name))
            return async_result

        is_full = False
        task_item = TaskItem(function, args, kwargs, async_result)
        with self._shutdown_lock:
            if self._shutting_down or self._shuted_down:
                async_result.set_exception(ShutedDownError(self._thread_pool_name))
                return async_result
            try:
                self._queue.put_nowait(task_item)
                async_result.set_time_info("submitted_to_queue_at")
            except Full:
                is_full = True

        if is_full:
            return self._reject_handler(self._queue, task_item)
        else:
            with self._core_threads_wait_condition:
                self._core_threads_wait_condition.notify()
            return async_result

    def shutdown(self, wait_time=None):
        if self._shutting_down or self._shuted_down:
            return
        with self._shutdown_lock:
            if self._shutting_down or self._shuted_down:
                return
            self._shutting_down = True
            self._shuted_down = False

        with self._core_threads_wait_condition:
            self._core_threads_wait_condition.notify_all()
        with self._core_threads_condition:
            if self._core_threads:
                self._core_threads_condition.wait(wait_time)

        while True:
            try:
                task_item = self._queue.get_nowait()
            except Empty:
                break
            else:
                task_item.async_result.set_exception(
                    ShutedDownError(self._thread_pool_name))

        self._shutting_down = False
        self._shuted_down = True

