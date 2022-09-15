import logging
import sys
from pathlib import Path
from io import TextIOBase
import pytest


# Use the root logger for stdout/stderr patching.
logger = logging.getLogger()


def pytest_addoption(parser):
    """
    The image on Artifactory that corresponds to the currently
    committed Metaflow version.
    """
    parser.addoption("--image", action="store", default=None)
    parser.addoption(
        "--opsgenie-api-token", dest="opsgenie_api_token", action="store", default=None
    )
    parser.addoption("--public-dir", dest="public_dir", action="store", default='')


class StreamToLogger(TextIOBase):
    """Fake file-like stream object that redirects writes to a logger instance."""

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ""

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item):
    """Emit a log file per test to make it easier to debug in failure scenarios.

    Sourced from:  https://stackoverflow.com/a/64480499
    """
    logging_plugin = item.config.pluginmanager.get_plugin("logging-plugin")
    filename = Path(
        item.config.getoption('public_dir'),
        'pytest-logs',
        f"{item._request.node.name}.log"
    )
    logging_plugin.set_log_path(str(filename))

    # Forward logs from stdout/stderr to the logger as well.
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr =  StreamToLogger(logger, logging.ERROR)
    yield