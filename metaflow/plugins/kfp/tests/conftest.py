import pytest
from pathlib import Path


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


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item, pytestconfig):
    """Emit a log file per test to make it easier to debug in failure scenarios.

    Sourced from:  https://stackoverflow.com/a/64480499
    """
    logging_plugin = item.config.pluginmanager.get_plugin("logging-plugin")
    filename = Path(
        pytestconfig.getoption('public_dir'),
        'pytest-logs',
        f"{item._request.node.name}.log"
    )
    logging_plugin.set_log_path(str(filename))
    yield
