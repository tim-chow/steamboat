# coding: utf8

import logging
import time
from functools import partial
import random
import threading
import heapq

from .window import WindowHalfOpenError, WindowClosedError, WindowStatus, Window
from .executor import *

LOGGER = logging.getLogger(__name__)


class BaseError(StandardError):
    """
    异常类的基类
    """
    pass


class SubmitTaskError(BaseError):
    """
    向 Executor 中提交任务失败
    """
    def __init__(self, exc, *a):
        super(self.__class__, self).__init__(*a)
        self._exc = exc

    @property
    def exc(self):
        return self._exc


class TimeoutReachedError(BaseError):
    """
    任务超时
    """
    def __init__(self, timeout, *a):
        super(self.__class__, self).__init__(*a)
        self._timeout = timeout

    @property
    def timeout(self):
        return self._timeout


class ShutDownError(BaseError):
    pass


class Cabin(object):
    """
    船舱对象。其中包含：
        名称
        executor
        窗口
    """
    def __init__(self,
                 name,
                 executor,
                 timeout,
                 open_length,
                 closed_length,
                 half_open_length,
                 failure_ratio_threshold,
                 failure_count_threshold,
                 half_failure_count_threshold,
                 recovery_ratio_threshold,
                 recovery_count_threshold,
                 half_open_probability):
        self._name = name
        self._executor = executor
        self._timeout = timeout
        self._window = Window(
            0,
            WindowStatus.OPEN,
            open_length,
            closed_length,
            half_open_length,
            failure_ratio_threshold,
            failure_count_threshold,
            half_failure_count_threshold,
            recovery_ratio_threshold,
            recovery_count_threshold)
        self._half_open_probability = half_open_probability

        self._shut_down_lock = threading.Lock()
        self._shut_down = False
        self._pending_task_condition = threading.Condition()
        self._pending_tasks = []
        self._completed_task_count = 0
        self._check_async_results_thread_event = threading.Event()
        self._check_async_results_thread = threading.Thread(
            target=self._check_async_results_thread_run)
        self._check_async_results_thread.setDaemon(True)
        self._check_async_results_thread.start()

    def get_name(self):
        return self._name

    def get_window(self):
        return self._window

    def execute(self, f, *a, **kw):
        cabin_async_result = AsyncResult()
        if self._shut_down:
            cabin_async_result.set_exception(ShutDownError("cabin closed"))
            return cabin_async_result

        current_timestamp = time.time()
        window_status = self._window.get_status(current_timestamp)
        if window_status is None:
            LOGGER.error("invalid timestamp %f", current_timestamp)
        elif window_status == WindowStatus.CLOSED:
            cabin_async_result.set_exception(WindowClosedError(self._name))
            return cabin_async_result
        elif window_status == WindowStatus.HALF_OPEN:
            if self._half_open_probability == 0:
                cabin_async_result.set_exception(WindowHalfOpenError(self._name))
                return cabin_async_result
            elif self._half_open_probability == 1:
                pass
            else:
                if random.random() > self._half_open_probability:
                    cabin_async_result.set_exception(WindowHalfOpenError(self._name))
                    return cabin_async_result

        cabin_async_result.set_time_info("putted_into_cabin_at")
        # 提交任务
        try:
            executor_async_result = self._executor.submit_task(f, *a, **kw)
        except Exception as exc:
            self._window.update_status(current_timestamp, 0, 0, 0, 1)
            cabin_async_result.set_exception(SubmitTaskError(exc))
            return cabin_async_result

        executor_async_result.deadline = current_timestamp + self._timeout
        # 成功提交任务之后，将 AsyncResult 对象保存到 Pending Tasks
        with self._pending_task_condition:
            if self._shut_down:
                cabin_async_result.set_exception(ShutDownError("cabin closed"))
                return cabin_async_result
            heapq.heappush(self._pending_tasks, executor_async_result)
            if len(self._pending_tasks) == 1:
                self._pending_task_condition.notify_all()

        executor_async_result.add_done_callback(
            partial(self._done_callback, cabin_async_result)
        )

        return cabin_async_result

    submit_task = execute

    def _done_callback(self, cabin_async_result, executor_async_result):
        try:
            if not cabin_async_result.set_running_or_notify_cancel():
                return
        except RuntimeError:
            return

        cabin_async_result.set_time_info("left_cabin_at")
        cabin_async_result.update_time_info(executor_async_result.time_info)

        try:
            timestamp = time.time()
            if executor_async_result.cancelled():
                cabin_async_result.set_exception(RuntimeError("unreachable"))
                return

            exc_value = executor_async_result.exception()
            if exc_value is None:
                self._window.update_status(timestamp, 1, 0, 0, 0)
                cabin_async_result.set_result(executor_async_result.result())
            else:
                self._window.update_status(timestamp, 0, 1, 0, 0)
                cabin_async_result.set_exception(exc_value)
        finally:
            with self._pending_task_condition:
                self._completed_task_count = self._completed_task_count + 1
                if self._completed_task_count / (len(self._pending_tasks) + 0.001) >= 0.5:
                    self._pending_task_condition.notify_all()

    def _check_async_results_thread_run(self):
        while True:
            with self._pending_task_condition:
                if self._shut_down:
                    break

                # 将已经完成的任务移除
                pending_tasks = self._pending_tasks
                self._pending_tasks = []
                for ar in pending_tasks:
                    if not ar.done():
                        heapq.heappush(self._pending_tasks, ar)
                self._completed_task_count = 0

                # 如果没有挂起的任务，则一直等待，直到被唤醒
                if not self._pending_tasks:
                    self._pending_task_condition.wait()
                    continue

                current_timestamp = time.time()
                while self._pending_tasks:
                    # 如果堆顶元素到达 deadline ，则弹出它，并将它取消
                    ar = self._pending_tasks[0]
                    if ar.deadline <= current_timestamp:
                        heapq.heappop(self._pending_tasks)
                        try:
                            if ar.set_running_or_notify_cancel():
                                self._window.update_status(
                                    current_timestamp,
                                    0,
                                    0,
                                    1,
                                    0)
                                ar.set_exception(TimeoutReachedError(self._timeout))
                        except RuntimeError:
                            pass
                        continue
                    # 否则，等待到堆顶元素达到超时，或被唤醒
                    self._pending_task_condition.wait(
                        current_timestamp - ar.deadline)
                    break

        self._check_async_results_thread_event.set()
        LOGGER.info("check async results thread exited")

    def shutdown(self, timeout=None):
        if self._shut_down:
            return
        with self._shut_down_lock:
            if self._shut_down:
                return
            self._shut_down = True

        LOGGER.info("begin to acquire pending task condition")
        with self._pending_task_condition:
            # 将所有未完成的任务置为失败
            while self._pending_tasks:
                ar = self._pending_tasks.pop(0)
                try:
                    if ar.set_running_or_notify_cancel():
                        ar.set_exception(ShutDownError("cabin closed"))
                except RuntimeError:
                    pass
            self._pending_task_condition.notify_all()
        self._check_async_results_thread_event.wait(timeout)


class CabinBuilder(object):
    def __init__(self):
        self._name = None
        self._executor = None
        self._timeout = 3
        self._open_length = None
        self._closed_length = None
        self._half_open_length = None
        self._failure_ratio_threshold = None
        self._failure_count_threshold = None
        self._half_failure_count_threshold = None
        self._recovery_ratio_threshold = None
        self._recovery_count_threshold = None
        self._half_open_probability = 0.5

    def with_name(self, name):
        self._name = name
        return self

    def with_executor(self, executor):
        self._executor = executor
        return self

    def with_timeout(self, timeout):
        self._timeout = timeout
        return self

    def with_open_length(self, open_length):
        self._open_length = open_length
        return self

    def with_closed_length(self, closed_length):
        self._closed_length = closed_length
        return self

    def with_half_open_length(self, half_open_length):
        self._half_open_length = half_open_length
        return self

    def with_failure_ratio_threshold(self, failure_ratio_threshold):
        self._failure_ratio_threshold = failure_ratio_threshold
        return self

    def with_failure_count_threshold(self, failure_count_threshold):
        self._failure_count_threshold = failure_count_threshold
        return self

    def with_half_failure_count_threshold(self, half_failure_count_threshold):
        self._half_failure_count_threshold = half_failure_count_threshold
        return self

    def with_recovery_ratio_threshold(self, recovery_ratio_threshold):
        self._recovery_ratio_threshold = recovery_ratio_threshold
        return self

    def with_recovery_count_threshold(self, recovery_count_threshold):
        self._recovery_count_threshold = recovery_count_threshold
        return self

    def with_half_open_probability(self, half_open_probability):
        self._half_open_probability = half_open_probability
        return self

    def build(self):
        if self._name is None:
            raise RuntimeError("missing argument name")
        if self._executor is None:
            raise RuntimeError("missing argument executor")
        if self._timeout is None:
            raise RuntimeError("missing argument timeout")
        if self._open_length is None:
            raise RuntimeError("missing argument open_length")
        if self._closed_length is None:
            raise RuntimeError("missing argument closed_length")
        if self._half_open_length is None:
            raise RuntimeError("missing argument half_open_length")
        if self._failure_ratio_threshold is None:
            raise RuntimeError("missing argument failure_ratio_threshold")
        if self._failure_count_threshold is None:
            raise RuntimeError("missing argument failure_count_threshold")
        if self._half_failure_count_threshold is None:
            raise RuntimeError("missing argument half_failure_count_threshold")
        if self._half_open_probability is None:
            raise RuntimeError("missing argument half_open_probability")

        return Cabin(
            self._name,
            self._executor,
            self._timeout,
            self._open_length,
            self._closed_length,
            self._half_open_length,
            self._failure_ratio_threshold,
            self._failure_count_threshold,
            self._half_failure_count_threshold,
            self._recovery_ratio_threshold,
            self._recovery_count_threshold,
            self._half_open_probability)
