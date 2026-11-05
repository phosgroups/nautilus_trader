"""Gate protocol tests own their nodes instead of the generic adapter fixtures."""

import pytest


@pytest.fixture(autouse=True)
def trader():
    return None


@pytest.fixture(autouse=True)
def components():
    return None
