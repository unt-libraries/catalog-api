"""
Tests the 'makefixtures' custom management.py command.
"""

import pytest

import ujson

from testmodels import models
from sierra.management.commands import makefixtures

# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def mytempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('data')


@pytest.fixture(scope='module')
def make_tmpfile(mytempdir):
    def make(data, filename):
        path = mytempdir.join(filename)
        with open(str(path), 'w') as fh:
            fh.write(data)
        return path
    return make


@pytest.fixture
def config_file_not_json(make_tmpfile):
    return make_tmpfile('this is not valid json data', 'not.json')


@pytest.fixture
def config_file_json(make_tmpfile):
    data = ('{ "string": "test", "bool": true, "array": ["one", "two"],'
            '"object": { "key": "val" } }')
    return make_tmpfile(data, 'valid.json')


@pytest.fixture
def config_file_not_valid(make_tmpfile, badconfdata_multiple):
    data = ujson.dumps(badconfdata_multiple['multi'])
    return make_tmpfile(data, 'invalid_config.json')


@pytest.fixture
def config_file_valid(make_tmpfile, confdata):
    data = ujason.dumps(confdata['multi'])
    return make_tmpfile(data, 'valid_config.json')



# TESTS

def test_readconfigfile_errors_if_file_not_json(config_file_not_json):
    """
    Command.read_config_file should raise a ValueError if the config
    file passed in is not valid json.
    """
    with pytest.raises(ValueError):
        makefixtures.Command().read_config_file(str(config_file_not_json))


def test_readconfigfile_errors_if_config_file_unreadable():
    """
    Command.read_config_file should raise an IOError if the configuration
    filename given is not a readable file.
    """
    with pytest.raises(IOError):
        makefixtures.Command().read_config_file('no_such_file.json')


def test_readconfigfile_returns_valid_config_object(config_file_json):
    """
    Command.read_config_file should return valid Python data.
    """
    conf = makefixtures.Command().read_config_file(str(config_file_json))
    assert (len(conf) == 1 and conf[0]['string'] == 'test' and
            conf[0]['bool'] == True and
            conf[0]['array'] == ['one', 'two'] and
            conf[0]['object']['key'] == 'val')


def test_command_handle_raises_err_if_confdata_invalid(config_file_not_valid):
    """
    Command.handle should raise a ConfigIsInvalid error if any of the
    provided configuration data is invalid.
    """
    with pytest.raises(makefixtures.ConfigIsInvalid):
        makefixtures.Command().handle(str(config_file_not_valid))
