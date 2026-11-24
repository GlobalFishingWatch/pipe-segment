"""Fixtures of the CLI tests."""

import logging

from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """Restores the root logger after a test.

    Running a command sets up the logging of the whole process: it replaces the handlers of
    the root logger, and --verbose leaves its level at DEBUG. That would slow down the tests
    that run after, and make Beam's Prism runner panic in them.
    """
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level

    yield

    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    for handler in handlers:
        root.addHandler(handler)

    root.setLevel(level)
