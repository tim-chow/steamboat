# coding: utf8

from functools import partial
from .cabin import SubmitTaskError
from .window import WindowHalfOpenError, WindowClosedError
from .executor import *


class SteamBoat(object):
    def __init__(self):
        self._cabins = {}
        self._degredation_strategies = {}
        self._default_cabin = None
        self._default_degredation_strategy = None

    def add_cabin(self, cabin, degredation_strategy=None, ignore_if_exists=False):
        cabin_name = cabin.get_name()
        if cabin_name in self._cabins:
            if ignore_if_exists:
                return self
            raise RuntimeError("cabin: %s already exists" % cabin_name)
        self._cabins[cabin_name] = cabin
        self._degredation_strategies[cabin_name] = degredation_strategy
        return self

    def set_default_cabin(self, cabin, degredation_strategy=None):
        self._default_cabin = cabin
        self._default_degredation_strategy = degredation_strategy
        return self

    def push_into_cabin(self, cabin_name):
        cabin = self._cabins.get(cabin_name, self._default_cabin)
        if cabin == None:
            raise RuntimeError("cabin: %s does not exist" % cabin_name)

        def _inner(f):
            def _innest(*a, **kw):
                steamboat_async_result = AsyncResult()
                steamboat_async_result.set_time_info("putted_into_steamboat_at")
                cabin_async_result = cabin.execute(f, *a, **kw)
                cabin_async_result.add_done_callback(partial(
                    self._done_callback,
                    steamboat_async_result,
                    cabin_name,
                    f,
                    a,
                    kw))
                return steamboat_async_result
            return _innest
        return _inner

    def submit_task(self, cabin_name, f, *a, **kw):
        return self.push_into_cabin(cabin_name)(f)(*a, **kw)

    def _done_callback(self,
                       steamboat_async_result,
                       cabin_name,
                       f,
                       a,
                       kw,
                       cabin_async_result):
        steamboat_async_result.update_time_info(cabin_async_result.time_info)
        steamboat_async_result.set_time_info("left_steamboat_at")

        exception = cabin_async_result.exception()
        if exception == None:
            steamboat_async_result.set_result(cabin_async_result.result())
            return

        ds = self._degredation_strategies.get(
            cabin_name,
            self._default_degredation_strategy)
        if ds == None:
            steamboat_async_result.set_exception(exception)
            return

        args = f, a, kw
        if isinstance(exception, SubmitTaskError):
            method = ds.on_submit_task_error
            args = (exception.exc, ) + args
        elif isinstance(exception, WindowHalfOpenError):
            method = ds.on_window_half_open
        elif isinstance(exception, WindowClosedError):
            method = ds.on_window_closed
        else:
            method = ds.on_exception
            args = (exception, ) + args

        try:
            steamboat_async_result.set_result(method(*args))
        except Exception as ex:
            steamboat_async_result.set_exception(ex)

