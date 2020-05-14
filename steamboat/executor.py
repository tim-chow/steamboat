# coding: utf8

import time
from abc import ABCMeta, abstractmethod
import itertools
import threading

from concurrent.futures import Future

__all__ = ["BaseError", "ShutDownError", "AsyncResult", "TaskItem", "Executor"]


class BaseError(StandardError):
    """
    异常类的基类
    """
    pass


class ShutDownError(BaseError):
    """
    当 Executor 正在关闭或已经关闭时，向其中提交任务，会引发该异常；
    在关闭 Executor 时，尚未完成的 Future 也会被设置为该异常
    """
    pass


class AsyncResult(Future):
    counter = itertools.count().next
    lock = threading.Lock()

    def __init__(self, deadline=None):
        Future.__init__(self)
        self._time_info = {}
        self._id = self.generate_id()
        self._deadline = deadline

    @classmethod
    def generate_id(cls):
        with cls.lock:
            return cls.counter()

    @property
    def time_info(self):
        return self._time_info

    def set_time_info(self, key, timestamp=None):
        self._time_info[key] = timestamp or time.time()
        return self

    def update_time_info(self, time_info):
        self._time_info.update(time_info)
        return self

    @property
    def ident(self):
        return self._id

    @property
    def deadline(self):
        return self._deadline

    @deadline.setter
    def deadline(self, deadline):
        self._deadline = deadline

    def __cmp__(self, obj):
        if not isinstance(obj, self.__class__):
            return 1
        if obj.deadline == self.deadline:
            return 0
        if obj.deadline is None:
            return -1
        if self.deadline is None:
            return 1
        if obj.deadline < self.deadline:
            return 1
        return -1


class TaskItem(object):
    """
    每个 task item 包含：
        可调用对象，
        元组参数，
        关键字参数，
        用于保存任务执行结果的 AsyncResult 对象
    """
    def __init__(self, func, args, kwargs, async_result):
        self._function = func
        self._args = args
        self._kwargs = kwargs
        self._async_result = async_result

    @property
    def function(self):
        return self._function

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def async_result(self):
        return self._async_result


class Executor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit_task(self, func, *args, **kwargs):
        pass

    @abstractmethod
    def shutdown(self, wait_time=None):
        pass
