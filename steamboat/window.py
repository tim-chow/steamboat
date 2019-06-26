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
        窗口期内的统计信息（成功、失败、超时、拒绝）
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
        self._initialize_statistics()

    def _initialize_statistics(self):
        self._success_count = 0
        self._failure_count = 0
        self._timeout_count = 0
        self._rejection_count = 0

    def _fetch(self, position):
        with self._lock:
            # 位置在窗口左边缘的左侧时，忽略它
            if position < self._start_position:
                return

            end_position = self._get_end_position()
            # 位置在窗口右边缘的右侧时，分以下情况：
            if position >= end_position:
                # + 1，如果窗口是半开或打开的，直接右移
                if self._status == WindowStatus.HALF_OPEN or \
                        self._status == WindowStatus.OPEN:
                    self._enter_into_open_status(position)
                    return self._status
                # + 2，如果当前窗口是关闭的，先进入到下一阶段的半开状态，
                # + + 然后，递归处理
                elif self._status == WindowStatus.CLOSED:
                    self._enter_into_half_open_status(end_position)
                    return self._fetch(position)
                raise RuntimeError("unreachable")
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

    def update_status(self,
                      position,
                      success_count,
                      failure_count,
                      timeout_count,
                      rejection_count):
        with self._lock:
            status = self._fetch(position)
            if status is None:
                return

            # 当窗口处于关闭状态时，不允许更新窗口的状态信息
            if status == WindowStatus.CLOSED:
                return

            self._success_count = self._success_count + success_count
            self._failure_count = self._failure_count + failure_count
            self._timeout_count = self._timeout_count + timeout_count
            self._rejection_count = self._rejection_count + rejection_count

            if self._failure_count == 0:
                failure_ratio = 0.
                success_ratio = 1.
            else:
                total_count = float(self.get_total_count())
                failure_ratio = self._failure_count / total_count
                success_ratio = self._success_count / total_count

            if status == WindowStatus.OPEN:
                # 当失败率达到阈值的时候，窗口进入到CLOSED状态
                if failure_ratio >= self._failure_ratio_threshold and (
                        self._failure_count_threshold is None or
                            self._failure_count >= self._failure_count_threshold):
                    self._enter_into_close_status(position)
                    return
            elif status == WindowStatus.HALF_OPEN:
                # 当失败率达到阈值的时候，窗口进入到CLOSED状态
                if failure_ratio >= self._failure_ratio_threshold and (
                        self._half_failure_count_threshold is None or
                            self._failure_count >= self._half_failure_count_threshold):
                    self._enter_into_close_status(position)
                    return

                # 检查窗口是否应该恢复到打开状态
                if self._recovery_ratio_threshold == None or (
                        success_ratio < self._recovery_ratio_threshold):
                    return
                if self._recovery_count_threshold == None or (
                        self._success_count >= self._recovery_count_threshold):
                    self._enter_into_open_status(position)
                    return

    def _enter_into_close_status(self, position):
        self._start_position = position
        self._status = WindowStatus.CLOSED
        self._initialize_statistics()

    def _enter_into_open_status(self, position):
        self._start_position = position
        self._status = WindowStatus.OPEN
        self._initialize_statistics()

    def _enter_into_half_open_status(self, position):
        self._start_position = position
        self._status = WindowStatus.HALF_OPEN
        self._initialize_statistics()

    def get_success_count(self):
        return self._success_count

    def get_failure_count(self):
        return self._failure_count

    def get_timeout_count(self):
        return self._timeout_count

    def get_rejection_count(self):
        return self._rejection_count

    def get_total_count(self):
        # 总数量不包含拒绝数量
        return self._success_count + \
            self._failure_count + \
            self._timeout_count

