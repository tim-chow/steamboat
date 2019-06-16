# coding: utf8

from abc import ABCMeta, abstractmethod

__all__ = ["BaseError", "ShutedDownError", "TaskItem", "Executor"]


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


class TaskItem(object):
    """
    每个task item包含：
        可调用对象，
        元组参数，
        关键字参数，
        保存执行结果的Future对象
    """
    def __init__(self, function, args, kwargs, future):
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._future = future

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
    def future(self):
        return self._future


class Executor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit_task(self, function, *args, **kwargs):
        pass

    @abstractmethod
    def shutdown(self, wait_time=None):
        pass

