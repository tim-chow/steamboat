# coding: utf8

import logging
import time
from functools import partial
import random
from .window import WindowHalfOpenError, WindowClosedError, WindowStatus, Window
from .executor import *

LOGGER = logging.getLogger(__name__)


class BaseError(Exception):
    """
    异常类的基类
    """
    pass


class SubmitTaskError(BaseError):
    """
    向Executor中提交任务失败
    """
    def __init__(self, exc):
        super(self.__class__, self).__init__()
        self._exc = exc

    @property
    def exc(self):
        return self._exc


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
                 open_length,
                 closed_length,
                 half_open_length,
                 failure_ratio_threshold,
                 failure_count_threshold,
                 half_failure_count_threshold,
                 half_open_probability,
                 recovery_ratio_threshold,
                 recovery_count_threshold):
        self._name = name
        self._executor = executor
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

    def _update_window(self, timestamp, success_count, failure_count):
        self._window.update_status(timestamp, success_count, failure_count)

    def get_name(self):
        return self._name

    def get_window(self):
        return self._window

    def execute(self, f, *a, **kw):
        cabin_async_result = AsyncResult()
        current_timestamp = time.time()
        window_status = self._window.get_status(current_timestamp)
        if window_status is None:
            LOGGER.error("invalid timestamp: %f" % current_timestamp)
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
            cabin_async_result.set_exception(SubmitTaskError(exc))
            return cabin_async_result

        executor_async_result.add_done_callback(
            partial(self._done_callback, cabin_async_result)
        )
        return cabin_async_result

    def _done_callback(self, cabin_async_result, executor_async_result):
        cabin_async_result.set_time_info("left_cabin_at")
        cabin_async_result.update_time_info(executor_async_result.time_info)

        timestamp = time.time()
        exc_value = executor_async_result.exception()
        if exc_value is None:
            self._window.update_status(timestamp, 1, 0)
            cabin_async_result.set_result(executor_async_result.result())
        else:
            self._window.update_status(timestamp, 0, 1)
            cabin_async_result.set_exception(exc_value)


class CabinBuilder(object):
    def __init__(self):
        self._name = None
        self._executor = None
        self._open_length = None
        self._closed_length = None
        self._half_open_length = None
        self._failure_ratio_threshold = None
        self._failure_count_threshold = None
        self._half_failure_count_threshold = None
        self._half_open_probability = 0.5
        self._recovery_ratio_threshold = None
        self._recovery_count_threshold = None

    def with_name(self, name):
        self._name = name
        return self

    def with_executor(self, executor):
        self._executor = executor
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

    def with_half_open_probability(self, half_open_probability):
        self._half_open_probability = half_open_probability
        return self

    def with_recovery_ratio_threshold(self, recovery_ratio_threshold):
        self._recovery_ratio_threshold = recovery_ratio_threshold
        return self

    def with_recovery_count_threshold(self, recovery_count_threshold):
        self._recovery_count_threshold = recovery_count_threshold
        return self

    def build(self):
        if self._name is None:
            raise RuntimeError("missing argument name")
        if self._executor is None:
            raise RuntimeError("missing argument executor")
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
            self._open_length,
            self._closed_length,
            self._half_open_length,
            self._failure_ratio_threshold,
            self._failure_count_threshold,
            self._half_failure_count_threshold,
            self._half_open_probability,
            self._recovery_ratio_threshold,
            self._recovery_count_threshold)

