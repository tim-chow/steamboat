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


if __name__ == '__main__':
    import logging

    logger1 = logging.getLogger('logger1')
    handler1 = logging.StreamHandler()
    handler1.setFormatter(logging.Formatter(
        '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s '
        '%(message)s, time elapsed: %(time_elapsed).3f seconds'))
    handler1.setLevel(logging.DEBUG)
    logger1.addHandler(handler1)
    logger1.setLevel(logging.INFO)
    logger1.propagate = 0

    # 使用上下文管理器的方式
    class MySlowActionLogRecorder(SlowActionRecorder):
        def __init__(self, *a, **kw):
            super(self.__class__, self).__init__(logger1.info, *a, **kw)

    def func1(seconds):
        time.sleep(seconds)

    # 会打印慢日志
    with MySlowActionLogRecorder(1, 'call func1 with 1'):
        func1(1)

    # 不会打印慢日志
    with MySlowActionLogRecorder(1, 'call func1 with 0.5'):
        func1(0.5)


    # 使用装饰器的方式
    logger2 = logging.getLogger('logger2')
    handler2 = logging.StreamHandler()
    handler2.setFormatter(logging.Formatter(
        '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s '
        'call %(func_name)s with args: %(func_args)s, kwargs: %(func_kwargs)s '
        'used %(time_elapsed).3f seconds'))
    handler2.setLevel(logging.DEBUG)
    logger2.addHandler(handler2)
    logger2.setLevel(logging.INFO)
    logger2.propagate = 0

    slow_action_logger = SlowActionRecorder(logger2.info, 1)

    @slow_action_logger
    def func2(seconds):
        time.sleep(seconds)

    # 会打印慢日志
    func2(1)
    # 不会打印慢日志
    func2(0.5)

