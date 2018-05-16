"""
Contains the `tracebranches` manage.py command. 
"""
import json

from django.core.management.base import BaseCommand, CommandError

from sierra.management import relationtrees
from sierra.management.commands import makefixtures


INDENT = 2


class Command(BaseCommand):
    """
    Run a `tracebranches` command from manage.py.

    Use this as a utility to help you generate the `user_branches`
    parameter in your `makefixtures` config file. Running this outputs
    a JSON array specifying all the many-to-many and direct foreign-key
    relationship branches stemming from a particular model. You can
    then prune those branches by hand in your config file if you just
    want a subset of those branches.

    This command takes one argument that identifies the root model you
    wish to run through tracebranches (`app.model`).

    Example:

    python manage.py tracebranches myapp.Users
    """

    def add_arguments(self, parser):
        parser.add_argument('model', type=str)

    def handle(self, *args, **options):
        try:
            model = makefixtures.get_model_from_string(options['model'])
        except Exception as e:
            msg = 'The supplied model argument is invalid. {}'.format(str(e))
            raise CommandError(msg)

        branches = relationtrees.trace_branches(model)
        out = [[rel.fieldname for rel in branch] for branch in branches]
        self.stdout.write(json.dumps(out, indent=INDENT))



