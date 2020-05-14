# coding: utf8

from functools import partial

from .cabin import SubmitTaskError, TimeoutReachedError
from .window import WindowHalfOpenError, WindowClosedError
from .executor import *


class SteamBoat(object):
    def __init__(self):
        self._cabins = {}
        self._degradation_strategies = {}
        self._default_cabin = None
        self._default_degradation_strategy = None

    def add_cabin(self, cabin, degradation_strategy=None, ignore_if_exists=False):
        cabin_name = cabin.get_name()
        if cabin_name in self._cabins:
            if ignore_if_exists:
                return self
            raise RuntimeError("cabin %s already exists", cabin_name)
        self._cabins[cabin_name] = cabin
        self._degradation_strategies[cabin_name] = degradation_strategy
        return self

    def set_default_cabin(self, cabin, degradation_strategy=None):
        self._default_cabin = cabin
        self._default_degradation_strategy = degradation_strategy
        return self

    def push_into_cabin(self, cabin_name):
        cabin = self._cabins.get(cabin_name, self._default_cabin)
        if cabin is None:
            raise RuntimeError("cabin %s not exists" % cabin_name)

        def _inner(f):
            def _real_logic(*a, **kw):
                steamboat_async_result = AsyncResult()
                steamboat_async_result.set_time_info("putted_into_steamboat_at")
                cabin_async_result = cabin.submit_task(f, *a, **kw)
                cabin_async_result.add_done_callback(partial(
                    self._done_callback,
                    steamboat_async_result,
                    cabin_name,
                    f,
                    a,
                    kw,
                    True,
                    True))
                return steamboat_async_result
            return _real_logic
        return _inner

    def submit_task(self, cabin_name, f, *a, **kw):
        return self.push_into_cabin(cabin_name)(f)(*a, **kw)

    def _done_callback(self,
                       steamboat_async_result,
                       cabin_name,
                       f,
                       a,
                       kw,
                       set_running_mask,
                       execute_degradation,
                       cabin_async_result):
        if set_running_mask:
            try:
                if not steamboat_async_result.set_running_or_notify_cancel():
                    return
            except RuntimeError:
                return

        steamboat_async_result.set_time_info("left_steamboat_at")
        steamboat_async_result.update_time_info(cabin_async_result.time_info)

        if cabin_async_result.cancelled():
            steamboat_async_result.set_exception(RuntimeError("unreachable"))
            return

        exception = cabin_async_result.exception()
        if exception is None:
            steamboat_async_result.set_result(cabin_async_result.result())
            return

        if not execute_degradation:
            steamboat_async_result.set_exception(exception)
            return

        ds = self._degradation_strategies.get(
            cabin_name,
            self._default_degradation_strategy)
        if ds is None:
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
        elif isinstance(exception, TimeoutReachedError):
            method = ds.on_timeout_reached
        else:
            method = ds.on_exception
            args = (exception, ) + args

        cabin = self._cabins.get(cabin_name, self._default_cabin)
        if cabin is None:
            steamboat_async_result.set_exception(RuntimeError("unreachable"))
            return
        degradation_async_result = cabin.submit_task(method, *args)
        degradation_async_result.add_done_callback(partial(
            self._done_callback,
            steamboat_async_result,
            cabin_name,
            method,
            args,
            {},
            False,
            False
        ))
