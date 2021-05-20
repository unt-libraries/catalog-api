"""
Contains the `importapiusers` manage.py command. 
"""
from __future__ import absolute_import
import csv

from django.core.management.base import BaseCommand, CommandError

from api.models import APIUser


class Command(BaseCommand):
    """
    Run an `importapiusers` command from manage.py.

    This command takes one argument: the path to a CSV file containing
    the user data to import.
    """
    args = '<userdata.csv>'
    help = 'Create or update APIUser objects based on the supplied CSV file'
    user_model = APIUser

    def add_arguments(self, parser):
        parser.add_argument('file', type=str)

    def handle(self, *args, **options):
        batch = self.csv_to_batch(options['file'])
        cr, up, err = self.user_model.objects.batch_import_users(batch)
        self.stdout.write(self.compile_report(cr, up, err))

    def csv_to_batch(self, filepath):
        try:
            csvfile = open(filepath, 'r')
        except IOError as e:
            msg = ('There was a problem opening the supplied CSV file: {} '
                   ''.format(e))
            raise CommandError(msg)

        with csvfile:
            try:
                reader = csv.reader(csvfile)
            except csv.Error as e:
                msg = 'Supplied CSV file not readable: {}'.format(e)
                raise CommandError(msg)
            try:
                batch = self.user_model.objects.table_to_batch(reader)
            except Exception as e:
                msg = ('Encountered an unknown problem when trying to create '
                       'APIUser batch from supplied CSV data: {}'.format(e))
                raise CommandError(msg)
        return batch

    def compile_report(self, created, updated, errors):
        len_cr, len_up, len_err = len(created), len(updated), len(errors)
        report = ('Done. Users created: {}. Users updated: {}.'
                  ''.format(len_cr, len_up))
        if len_err:
            err_lines = []
            for rownum, user, msg in errors:
                un = user.get('username', '') or '<no username>'
                err_lines.append('Row {} ({}) -- {}'.format(rownum, un, msg))
            report = '{}\nErrors: \n{}'.format(report, '\n'.join(err_lines))
        return report
