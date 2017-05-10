"""
Contains the `makefixtures` manage.py command. 
"""
import ujson

from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import FieldError
from django.apps import apps


class ConfigIsInvalid(CommandError):
    """
    Raise exception if the makefixtures configuration file is invalid.
    """
    pass


class BadPath(Exception):
    """
    Raise exception if any of the user's `paths` entries are invalid.
    """
    pass


class Command(BaseCommand):
    """
    Run a `makefixtures` command from manage.py.

    This command takes as an argument a path to a JSON configuration
    file that specifies exactly what fixtures to make.
    """
    args = '<config.json>'
    help = 'Generate fixture data according to a supplied json config file'

    def handle(self, *args, **options):
        try:
            config = self.read_config(args[0])
        except Exception as e:
            raise CommandError(e)

        try:
            prepped = self.prep_config(config)
        except ConfigIsInvalid as e:
            raise CommandError(e)

        # objects = { model_name: { obj.pk: <JSON obj> } }
        try:
            objects = self.fetch_objects(prepped)
        except Exception as e:
            raise CommandError(e)

    def read_config(self, filename):
        """
        Read the JSON configuration file and return as Python data.
        """
        with open(filename, 'r') as fh:
            data = ujson.loads(fh.read())
            if not isinstance(data, list):
                data = [data]
            return data

    def prep_config(self, config):
        """
        Partially process and validate config entries.

        Returns a dict after prepping the user-provided configuration:
        { 
            'queries': [
                {'paths': [], 'qs': QuerySet}
                {'paths': [], 'qs': QuerySet}
            ],
            'model_chain': [Model1, Model2]
        }

        Each `queries` dict corresponds 1-1 with an entry in the
        original user-provided config file. `paths` is the full set of
        paths that should be followed to extract related objects,
        including both user-provided paths and paths generated if
        `follow_relations` is True. `qs` is the QuerySet object for
        extracting all objects related to that config entry.
        `model_chain` is a list of models that gives a valid chain of
        dependencies, where models should be output in that order to
        ensure dependencies are satisfied when later loading the data.
        """
        errors, queries, model_chain = [], [], []

        for i, entry in enumerate(config):
            model, qs = None, None
            model_string = entry.get('model', None)
            follow_relations = entry.get('follow_relations', False)
            user_paths = entry.get('paths', [])
            auto_paths = []
            user_filter = entry.get('filter', None)

            try:
                model = self._get_model(model_string, i)
            except ConfigIsInvalid as e:
                errors.append(str(e))

            if model is not None:
                try:
                    qs = self._get_filtered_queryset(model, user_filter, i)
                except ConfigIsInvalid as e:
                    errors.append(str(e))

            if qs is not None:
                path_errors = self._get_userpath_errors(model, user_paths, i)
                if path_errors:
                    errors += path_errors
                else:
                    if follow_relations:
                        auto_paths = self.trace_relations(model)
                    paths = auto_paths + user_paths
                    qs = self._preprocess_relations(qs, model, paths)
                    model_chain = self._get_dependencies(model_chain, model,
                                                         paths)

            if len(errors) == 0:
                queries.append({'qs': qs, 'paths': paths})

        if errors:
            msg = ('Configuration file is invalid. Found {} errors: {}'
                   ''.format(len(errors), '\n'.join(errors)))
            raise ConfigIsInvalid(msg)

        return {'queries': queries, 'model_chain': model_chain}

    def _get_model(self, model_string, index):
        try:
            (app, model) = model_string.split('.')
            return apps.get_model(app, model)
        except AttributeError:
            msg = '`model` is missing from config entry {}.'.format(index)
            raise ConfigIsInvalid(msg)
        except ValueError:
            msg = ('`model` for config entry {} is not formatted as '
                   '"app.model".'.format(index))
            raise ConfigIsInvalid(msg)
        except LookupError:
            msg = ('`model` for config entry {} ({}) could not be found.'
                   ''.format(index, model_string))
            raise ConfigIsInvalid(msg)

    def _get_filtered_queryset(self, model, user_filter, index):
        if user_filter is None:
            return model.objects.all()
        try:
            return model.objects.filter(**user_filter)
        except FieldError:
            msg = ('`filter` for config entry {} ({}) could not be resolved '
                   'into a valid field.'.format(index, user_filter))
            raise ConfigIsInvalid(msg)

    def trace_relations(self, model, onlyfk=False, path=None, fieldcache=None):
        """
        Recursively trace direct relationships from the given model.

        Returns an array of relation paths (array of arrays), where
        each path represents a full branch of related models branching
        off the reference model.

        By default, both foreign-key and many-to-many relationships are
        traced. Indirect relationships (where there is a foreign key on
        another model pointing to the given model) are not traced. You
        may pass True for `onlyfk` to limit the tracing to foreign-key
        relationships only (one-to-one and many-to-one).
        """
        tracing = []
        m2m_fields = model._meta.many_to_many
        fk_fields = [f for f in model._meta.fields if f.rel]
        rel_fields = fk_fields + (m2m_fields if not onlyfk else [])
        for field in rel_fields:
            fieldcache = fieldcache or []
            field_id = '{}.{}'.format(model._meta.model_name, field.name)
            if field_id not in fieldcache:
                nmodel = field.rel.to
                pathcache = fieldcache + [field_id]
                npath = (path or []) + [field.name]
                tracing += self.trace_relations(nmodel, onlyfk, npath,
                                                pathcache)
        if path and len(rel_fields) == 0:
            tracing.append(path)
        return tracing

    def _get_userpath_errors(self, model, user_paths, index):
        errors = []
        for i, path in enumerate(user_paths):
            m = model
            for fieldname in path:
                try:
                    accessor = getattr(m, fieldname)
                except AttributeError:
                    msg = ('`paths` for config entry {} is not valid: path '
                           '{}, {} not found on {}'
                           ''.format(index, i, fieldname, m._meta.model_name))
                    errors.append(msg)
                    break
                m = self._get_related_model_from_accessor(accessor)
        return errors

    def _preprocess_relations(self, qs, model, paths):
        for path in paths:
            selects, prefetch = [], []
            all_fks_so_far = True
            m = model
            for fieldname in path:
                accessor = getattr(m, fieldname)
                if self._relation_is_foreign_key(accessor) and all_fks_so_far:
                    selects.append(fieldname)
                else:
                    all_fks_so_far = False
                    prefetch.append(fieldname)
                m = self._get_related_model_from_accessor(accessor)
            if selects:
                qs = qs.select_related('__'.join(selects))
            if prefetch:
                qs = qs.prefetch_related('__'.join(prefetch))
        return qs

    def _get_dependencies(self, model_chain, model, paths):
        model_chain += [model] if model not in model_chain else []
        for path in paths:
            m = model
            for fieldname in path:
                model_chain += [m] if m not in model_chain else []
                accessor = getattr(m, fieldname)
                nextm = self._get_related_model_from_accessor(accessor)
                if self._relation_is_foreign_key(accessor):
                    model_chain = self._rearrange_models(m, nextm, model_chain)
                m = nextm
        return model_chain

    def _get_related_model_from_accessor(self, acc):
        if self._relation_is_foreign_key(acc) or self._relation_is_m2m(acc):
            return acc.field.rel.to
        elif self._relation_is_m2one(acc):
            return acc.related.model

    def _relation_is_foreign_key(self, r):
        return hasattr(r, 'field') and not hasattr(r, 'related_manager_cls')

    def _relation_is_m2m(self, r):
        return hasattr(r, 'field') and hasattr(r, 'related_manager_cls')

    def _relation_is_m2one(self, r):
        return not hasattr(r, 'field') and hasattr(r, 'related_manager_cls')

    def _rearrange_models(self, model, next_model, model_chain):
        m1_index = model_chain.index(model)
        try:
            m2_index = model_chain.index(next_model)
            if m2_index > m1_index:
                model_chain.remove(next_model)
        except ValueError:
            pass

        if next_model not in model_chain:
            model_chain.insert(m1_index, next_model)

        return model_chain

    def fetch_objects(self, prepped_config):
        """
        Return a set of 
        """
        pass

