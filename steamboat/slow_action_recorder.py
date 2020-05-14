# coding: utf8

import time
import functools


class SlowActionRecorder(object):
    def __init__(self, log_func, slow_threshold, message=None):
        self._log_func = log_func
        self._slow_threshold = slow_threshold
        self._message = message

    def _wrapper(self, fn, message):
        @functools.wraps(fn)
        def _inner(*a, **kw):
            start_time = time.time()
            try:
                return fn(*a, **kw)
            finally:
                time_elapsed = time.time() - start_time
                extra = {}
                extra["time_elapsed"] = time_elapsed
                extra["func_name"] = fn.__name__
                extra["func_args"] = a
                extra["func_kwargs"] = kw
                if time_elapsed >= self._slow_threshold:
                    self._log_func(message, extra=extra)
        return _inner

    def __call__(self, message):
        if callable(message):
            fn = message
            return self._wrapper(fn, self._message)
        return functools.partial(self._wrapper, message=message)

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        time_elapsed = time.time() - self._start_time
        if time_elapsed >= self._slow_threshold:
            self._log_func(self._message,
                extra={"time_elapsed": time_elapsed})
