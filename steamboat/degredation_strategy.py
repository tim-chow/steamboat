# coding: utf8

from abc import ABCMeta, abstractmethod


class DegredationStrategy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def on_submit_task_error(self, exc, func, a, kw):
        pass

    @abstractmethod
    def on_window_half_open(self, func, a, kw):
        pass

    @abstractmethod
    def on_window_closed(self, func, a, kw):
        pass

    @abstractmethod
    def on_timeout_reached(self, func, a, kw):
        pass

    @abstractmethod
    def on_exception(self, exc, func, a, kw):
        pass

