"""
Tests the 'importapiusers' custom management.py command.
"""

from __future__ import absolute_import
import pytest
import csv

from django.core.management import call_command
from django.core.management.base import CommandError
from six import StringIO

from api.management.commands import importapiusers
from six.moves import range


# FIXTURES AND TEST DATA
# ---------------------------------------------------------------------
# External fixtures used below can be found in
# django/sierra/conftest.py:
#    make_tmpfile
#    apiuser_with_custom_defaults

pytestmark = pytest.mark.django_db


@pytest.fixture
def cmd():
    def _cmd(user_model):
        command = importapiusers.Command()
        command.user_model = user_model
        return command
    return _cmd


def gen_ubatch(number, start_index=1, fields=None, perms=None):
    """
    Utility function to generate an APIUser data batch.

    `number` is the number of users to generate.
    `fields` is the list of fieldnames to generate for each user.
    `perms` is the list of permission names.
    """
    fields = fields or ('username', 'secret_text', 'password', 'email',
                        'first_name', 'last_name')
    pdict = {p: True for p in (perms or ('first', 'second', 'third'))}
    return [
        {f: ('{}{}'.format(f[0:2], i + start_index) if f in fields else pdict)
            for f in fields + ('permissions_dict',)}
        for i in range(0, number)
    ]


def ubatch_to_csv(batch):
    """
    Utility function to convert a batch of APIUser data to CSV.
    """
    permkey = 'permissions_dict'
    fields = [k for k in batch[0].keys() if k != permkey]
    fields.extend(list(batch[0][permkey].keys()))
    return '{}\n{}'.format(','.join(fields), '\n'.join([
        ','.join([str(r.get(f, r[permkey].get(f, None))) for f in fields])
        for r in batch
    ]))


@pytest.fixture
def good_data(make_tmpfile):
    batch = gen_ubatch(5)
    csv_text = ubatch_to_csv(batch)
    fpath = make_tmpfile(csv_text, 'good_data.csv')
    return batch, csv_text, fpath


@pytest.fixture
def mixed_data(make_tmpfile):
    batch = gen_ubatch(5)
    batch[1]['username'] = ''
    batch[3]['username'] = ''
    csv_text = ubatch_to_csv(batch)
    fpath = make_tmpfile(csv_text, 'mixed_data.csv')
    return batch, csv_text, fpath


@pytest.fixture
def bad_csv(make_tmpfile):
    return make_tmpfile('\n'.join([
        'username,secret_text,password,email,first_name,last_name,first,'
        'second,third',
        'un1',
    ]), 'bad.csv')


# TESTS

def test_command_csvtobatch_unreadable_file(cmd, apiuser_with_custom_defaults):
    """
    Command.csv_to_batch should raise an error if the CSV filepath
    given does not point to an existing, readable file.
    """
    test_uclass = apiuser_with_custom_defaults()
    with pytest.raises(CommandError):
        cmd(test_uclass).csv_to_batch('no_such_file.csv')


def test_command_csvtobatch_bad_csv(cmd, apiuser_with_custom_defaults,
                                    bad_csv):
    """
    Command.csv_to_batch should raise an error if the CSV file has
    problems such as rows with missing data.
    """
    test_uclass = apiuser_with_custom_defaults()
    with pytest.raises(CommandError):
        cmd(test_uclass).csv_to_batch(str(bad_csv))


def test_command_csvtobatch_good_data(cmd, apiuser_with_custom_defaults,
                                      good_data):
    """
    Command.csv_to_batch should take a filepath str pointing to a valid
    CSV file. It should return a data structure equivalent to passing
    the CSV table to APIUser.objects.table_to_batch.
    """
    # Set up test
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_uclass = apiuser_with_custom_defaults(custom_defaults)
    exp_batch, csv_text, fpath = good_data
    table = [r.split(',') for r in csv_text.split('\n')]

    # Do the test
    test_batch = cmd(test_uclass).csv_to_batch(str(fpath))

    # Check results
    check_batch = test_uclass.objects.table_to_batch(table)
    assert exp_batch == test_batch
    assert check_batch == test_batch


def test_command_handle(cmd, apiuser_with_custom_defaults, mixed_data, mocker):
    """
    Command.handle should take a string filepath that points to a CSV
    file. Then, it should: 1) pass that filepath to `csv_to_batch`,
    returning a user batch; 2) pass that batch to
    APIUsers.objects.batch_import_users to do the batch import, which
    then returns a (created, updated, errors) tuple of lists; 3) pass
    those three lists to `compile_report` to compile a report that it
    can output to stdout.
    """
    # Set up test
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_uclass = apiuser_with_custom_defaults(custom_defaults)
    batch, csv_text, fpath = mixed_data
    mocker.patch.object(test_uclass.objects, 'batch_import_users')
    test_uclass.objects.batch_import_users.return_value = ([], [], [])
    test_cmd = cmd(test_uclass)
    mocker.patch.object(test_cmd, 'csv_to_batch')
    test_cmd.csv_to_batch.return_value = batch
    mocker.patch.object(test_cmd, 'compile_report')
    test_cmd.compile_report.return_value = ''

    # Do the test
    test_cmd.handle(file=str(fpath))

    # Check results
    test_cmd.csv_to_batch.assert_called_with(str(fpath))
    test_uclass.objects.batch_import_users.assert_called_with(batch)
    test_cmd.compile_report.assert_called_with([], [], [])


def test_importapiusers_output_sanity(cmd, apiuser_with_custom_defaults,
                                      mixed_data):
    """
    When the `importapiusers` management command is run, it should
    output a report to stdout based on whatever data was imported.
    """
    custom_defaults = {'first': False, 'second': False, 'third': False}
    test_uclass = apiuser_with_custom_defaults(custom_defaults)
    batch, csv_text, fpath = mixed_data
    out = StringIO()

    created, updated, errors = [], [], []
    for i, user in enumerate(batch):
        if user['username'] == '':
            errors.append(i+1)
        elif not updated:
            updated.append(user['username'])
            test_uclass.objects.batch_import_users([user])
        else:
            created.append(user['username'])

    call_command('importapiusers', str(fpath), stdout=out)
    output = out.getvalue()
    assert 'created: {}'.format(len(created)) in output
    assert 'updated: {}'.format(len(updated)) in output
    assert 'Errors' in output
    for rownum in errors:
        assert 'Row {} (<no username>)'.format(rownum) in output
