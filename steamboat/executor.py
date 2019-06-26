# coding: utf8

import time
from abc import ABCMeta, abstractmethod
import itertools
import threading
from concurrent.futures import Future

__all__ = ["BaseError", "ShutedDownError", "AsyncResult", "TaskItem", "Executor"]


class BaseError(Exception):
    """
    异常类的基类
    """
    pass


class ShutedDownError(BaseError):
    """
    当Executor正在关闭或已经关闭时，向其中提交任务，会引发该异常；
    在关闭Executor时，尚未完成的Futures，也会被设置为该异常。
    """
    pass


class AsyncResult(Future):
    counter = itertools.count().next
    lock = threading.Lock()

    def __init__(self, deadline=None, *a, **kw):
        super(self.__class__, self).__init__(*a, **kw)
        self._time_info = {}
        self._ident = self.generate_ident()
        self._deadline = deadline

    @classmethod
    def generate_ident(cls):
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
        return self._ident

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
        if obj.deadline == None:
            return -1
        if self.deadline == None:
            return 1
        if obj.deadline < self.deadline:
            return 1
        return -1


class TaskItem(object):
    """
    每个task item包含：
        可调用对象，
        元组参数，
        关键字参数，
        保存任务执行结果的AsyncResult对象
    """
    def __init__(self, function, args, kwargs, async_result):
        self._function = function
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
    def submit_task(self, function, *args, **kwargs):
        pass

    @abstractmethod
    def shutdown(self, wait_time=None):
        pass

