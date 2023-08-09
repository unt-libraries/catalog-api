from __future__ import absolute_import

import json

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from sierra.management.commands import tracebranches
from six import StringIO


@pytest.fixture
def exp_tracebranches_results():
    return {
        'EndNode': [],
        'SelfReferentialNode': [['end'], ['parent', 'end']],
        'OneToOneNode': [],
        'ReferenceNode': [['srn', 'end'], ['srn', 'parent', 'end'], ['end'],
                          ['one'], ['throughnode_set', 'm2m', 'end']],
        'ManyToManyNode': [['end']],
        'ThroughNode': [['ref', 'srn', 'end'], ['ref', 'srn', 'parent', 'end'],
                        ['ref', 'end'], ['ref', 'one'],
                        ['ref', 'throughnode_set', 'm2m', 'end'],
                        ['m2m', 'end']]
    }


@pytest.mark.parametrize('mstr', [None, '', 'invalid', 'invalid.invalid'])
def test_tracebranches_errors_on_invalid_model(mstr):
    """
    The `tracebranches` command should raise a CommandError if the
    model string passed as the argument is invalid.
    """
    with pytest.raises(CommandError):
        call_command('tracebranches', mstr)


@pytest.mark.parametrize('mstr', [
    'EndNode',
    'SelfReferentialNode',
    'OneToOneNode',
    'ReferenceNode',
    'ManyToManyNode',
    'ThroughNode'])
def test_tracebranches_returns_correct_json(mstr, exp_tracebranches_results):
    """
    The `tracebranches` command should return the expected JSON arrays.
    """

    indent = tracebranches.INDENT
    out = StringIO()
    call_command('tracebranches', 'testmodels.{}'.format(mstr), stdout=out)
    expected = json.dumps(exp_tracebranches_results[mstr], indent=indent)
    assert out.getvalue() == '{}\n'.format(expected)
