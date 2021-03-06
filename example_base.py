# coding: utf8

import logging

from steamboat.cabin import CabinBuilder
from steamboat.degradation_strategy import DegradationStrategy
from steamboat.steamboat import SteamBoat

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(filename)s:%(lineno)d %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

LOGGER = logging.getLogger(__name__)


def create_cabin(executor):
    # 创建 Cabin
    cabin = CabinBuilder() \
        .with_name("cabin") \
        .with_executor(executor) \
        .with_open_length(1) \
        .with_half_open_length(0.4) \
        .with_closed_length(0.2) \
        .with_failure_ratio_threshold(0.95) \
        .with_failure_count_threshold(1) \
        .with_half_failure_count_threshold(1) \
        .build()
    return cabin


# 创建 DegradationStrategy
class TestDegradationStrategy(DegradationStrategy):
    def on_submit_task_error(self, exc, f, a, kw):
        LOGGER.error("submit task error: %s(%s)" % (exc.__class__, str(exc)))

    def on_window_half_open(self, f, a, kw):
        LOGGER.error("window was half open while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_window_closed(self, f, a, kw):
        LOGGER.error("window was closed while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_timeout_reached(self, f, a, kw):
        LOGGER.error("timeout was reached while invoking %s(*%s, **%s)"
            % (f.__name__, a, kw))

    def on_exception(self, exc, f, a, kw):
        LOGGER.error("exception: [%s] was raised while invoking %s(*%s, **%s)"
            % (exc, f.__name__, a, kw))


def create_steamboat(cabin, degradation_strategy):
    # 创建SteamBoat
    steamboat = SteamBoat()
    steamboat.add_cabin(cabin, degradation_strategy)
    return steamboat

