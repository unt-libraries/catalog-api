"""
Contains pytest fixtures shared by `base` app tests.
"""

import pytest


@pytest.fixture
def ruleset_test_obj_class():
    """
    Pytest fixture for generating objects with arbitrary attributes.
    Useful for creating mock objects to test various Rulesets.
    """
    class RulesetTestObjClass(object):
        def __init__(self, **kwargs):
            for kwarg, val in kwargs.items():
                setattr(self, kwarg, val)
    return RulesetTestObjClass
