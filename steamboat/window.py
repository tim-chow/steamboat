# coding: utf8

import threading


class BaseError(Exception):
    """
    异常类的基类
    """
    pass


class WindowHalfOpenError(BaseError):
    pass


class WindowClosedError(BaseError):
    pass


class WindowStatus(object):
    """
    窗口状态：
        开放
        半开
        关闭
    """
    OPEN = 1
    HALF_OPEN = 2
    CLOSED = 3


class Window(object):
    """
    窗口对象。其中包含：
        起始位置
        窗口状态
        窗口长度
        窗口期内的总请求数
        窗口期内的失败请求数
    """
    def __init__(self,
                 start_position,
                 status,
                 open_length,
                 closed_length,
                 half_open_length,
                 failure_ratio_threshold,
                 failure_count_threshold,
                 half_failure_count_threshold,
                 recovery_ratio_threshold,
                 recovery_count_threshold):
        self._start_position = start_position
        self._status = status
        self._open_length = open_length
        self._closed_length = closed_length
        self._half_open_length = half_open_length
        self._failure_ratio_threshold = failure_ratio_threshold
        self._failure_count_threshold = failure_count_threshold
        self._half_failure_count_threshold = half_failure_count_threshold
        self._recovery_ratio_threshold = recovery_ratio_threshold
        self._recovery_count_threshold = recovery_count_threshold

        self._lock = threading.RLock()
        self._total_count = 0
        self._failure_count = 0

    def _fetch(self, position):
        with self._lock:
            # 位置在窗口左边缘的左侧时，忽略它
            if position < self._start_position:
                return
            # 位置在窗口右边缘的右侧时，分以下情况：
            if position > self._get_end_position():
                # + 1，如果窗口是半开或打开的，直接右移
                if self._status == WindowStatus.HALF_OPEN or \
                        self._status == WindowStatus.OPEN:
                    self._enter_into_open_status(position)
                    return self._status
                # + 2，如果当前窗口是关闭的，先进入到下一阶段的半开状态，
                # + + 然后，递归处理
                elif self._status == WindowStatus.CLOSED:
                    self._enter_into_half_open_status(self._get_end_position())
                    return self._fetch(position)
            # 位置在窗口内时，直接返回即可
            else:
                return self._status

    def _get_end_position(self):
        if self._status == WindowStatus.OPEN:
            return self._start_position + self._open_length
        elif self._status == WindowStatus.CLOSED:
            return self._start_position + self._closed_length
        else:
            return self._start_position + self._half_open_length

    def get_status(self, position):
        return self._fetch(position)

    def update_status(self, position, success_count, failure_count):
        with self._lock:
            status = self._fetch(position)
            if status is None:
                return
            # 当窗口处于关闭状态时，不允许更新窗口的状态信息
            if status == WindowStatus.CLOSED:
                return
            if success_count > 0:
                self._total_count = self._total_count + success_count
            if failure_count > 0:
                self._total_count = self._total_count + failure_count
                self._failure_count = self._failure_count + failure_count

            if self._total_count == 0:
                current_failure_ratio = 0
            else:
                current_failure_ratio = self._failure_count / (self._total_count + 0.)

            # 当失败率达到阈值的时候，窗口进入到CLOSED状态
            if status == WindowStatus.OPEN:
                if current_failure_ratio >= self._failure_ratio_threshold and (
                        self._failure_count_threshold is None or
                            self._failure_count >= self._failure_count_threshold):
                    self._enter_into_close_status(position)
                return
            if status == WindowStatus.HALF_OPEN:
                if current_failure_ratio >= self._failure_ratio_threshold and (
                        self._half_failure_count_threshold is None or
                            self._failure_count >= self._half_failure_count_threshold):
                    self._enter_into_close_status(position)
                elif self._recovery_ratio_threshold == None:
                    return
                if (1 - current_failure_ratio) < self._recovery_ratio_threshold:
                    return
                if self._recovery_count_threshold == None or \
                    (self._total_count - self._failure_count) >= \
                        self._recovery_count_threshold:
                            self._enter_into_open_status(position)
                return
            raise RuntimeError("unreachable")

    def _enter_into_close_status(self, position):
        self._start_position = position
        self._status = WindowStatus.CLOSED
        self._failure_count = 0
        self._total_count = 0

    def _enter_into_open_status(self, position):
        self._start_position = position
        self._status = WindowStatus.OPEN
        self._total_count = 0
        self._failure_count = 0

    def _enter_into_half_open_status(self, position):
        self._start_position = position
        self._status = WindowStatus.HALF_OPEN
        self._total_count = 0
        self._failure_count = 0

    def get_failure_count(self):
        return self._failure_count

    def get_success_count(self):
        return self._total_count - self._failure_count

