"""
This is a custom solr_backend for haystack. It fixes a few minor issues
with the out-of-the-box version.

1. Overrides the SolrSearchBackend._process_results() method. The
out-of-the-box haystack version uses pysolr._to_python() to parse any
results for indexes that aren't managed by haystack. This method for
some reason just pulls the first item out of lists/tuples and returns
it--so search results truncate any lists of items that come from Solr.
Here I've implemented a custom_to_python function that fixes this
problem while still calling pysolr._to_python() on flattened lis
values.

2. Overrides the SolrSearchBackend.clear() method so that the Solr
index optimization isn't triggered if commit is false.
"""
from __future__ import absolute_import
import subprocess
import os
import shlex
import re

from django.apps import apps
from django.conf import settings

from haystack.backends import solr_backend, BaseEngine
from haystack.models import SearchResult
from haystack.constants import ID, DJANGO_CT, DJANGO_ID
from haystack.utils import get_model_ct
from haystack import connections
from pysolr import SolrError
from six.moves import zip


def is_sequence(arg):
    return (not hasattr(arg, 'strip') and 
            hasattr(arg, '__getitem__') or
            hasattr(arg, '__iter__'))


def custom_to_python(val, _to_python):
    """
    Simplest way I could think of to add what we want to
    pysolr._to_python. Recursively unpacks all values in any sequence
    and returns the final data structure.
    """
    if is_sequence(val):
        ret_val = []
        for i in val:
            ret_val.append(custom_to_python(i, _to_python))
        return ret_val
    else:
        return _to_python(val)


class CustomSolrSearchBackend(solr_backend.SolrSearchBackend):
    def _process_results(self, raw_results, highlight=False, result_class=None, distance_point=None):
        results = []
        hits = raw_results.hits
        facets = {}
        stats = {}
        spelling_suggestion = None

        if result_class is None:
            result_class = SearchResult

        if hasattr(raw_results,'stats'):
            stats = raw_results.stats.get('stats_fields',{})

        if hasattr(raw_results, 'facets'):
            facets = {
                'fields': raw_results.facets.get('facet_fields', {}),
                'dates': raw_results.facets.get('facet_dates', {}),
                'queries': raw_results.facets.get('facet_queries', {}),
            }

            for key in ['fields']:
                for facet_field in facets[key]:
                    # Convert to a two-tuple, as Solr's json format returns a list of
                    # pairs.
                    facets[key][facet_field] = list(zip(facets[key][facet_field][::2], facets[key][facet_field][1::2]))

        if self.include_spelling is True:
            if hasattr(raw_results, 'spellcheck'):
                if len(raw_results.spellcheck.get('suggestions', [])):
                    # For some reason, it's an array of pairs. Pull off the
                    # collated result from the end.
                    spelling_suggestion = raw_results.spellcheck.get('suggestions')[-1]

        unified_index = connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        for raw_result in raw_results.docs:
            app_label, model_name = raw_result[DJANGO_CT].split('.')
            additional_fields = {}
            model = apps.get_model(app_label, model_name)

            if model and model in indexed_models:
                for key, value in raw_result.items():
                    index = unified_index.get_index(model)
                    string_key = str(key)

                    if string_key in index.fields and hasattr(index.fields[string_key], 'convert'):
                        additional_fields[string_key] = index.fields[string_key].convert(value)
                    else:
                        additional_fields[string_key] = custom_to_python(value, self.conn._to_python)

                del(additional_fields[DJANGO_CT])
                del(additional_fields[DJANGO_ID])
                del(additional_fields['score'])

                if raw_result[ID] in getattr(raw_results, 'highlighting', {}):
                    additional_fields['highlighted'] = raw_results.highlighting[raw_result[ID]]

                if distance_point:
                    additional_fields['_point_of_origin'] = distance_point

                    if raw_result.get('__dist__'):
                        from haystack.utils.geo import Distance
                        additional_fields['_distance'] = Distance(km=float(raw_result['__dist__']))
                    else:
                        additional_fields['_distance'] = None

                result = result_class(app_label, model_name, raw_result[DJANGO_ID], raw_result['score'], **additional_fields)
                results.append(result)
            else:
                hits -= 1

        return {
            'results': results,
            'hits': hits,
            'stats': stats,
            'facets': facets,
            'spelling_suggestion': spelling_suggestion,
        }

    def clear(self, models=[], commit=True):
        try:
            if not models:
                # *:* matches all docs in Solr
                self.conn.delete(q='*:*', commit=commit)
            else:
                models_to_delete = []

                for model in models:
                    models_to_delete.append("%s:%s" % (DJANGO_CT, get_model_ct(model)))

                self.conn.delete(q=" OR ".join(models_to_delete), commit=commit)

            if commit:
                # Run an optimize post-clear. http://wiki.apache.org/solr/FAQ#head-9aafb5d8dff5308e8ea4fcf4b71f19f029c4bb99
                self.conn.optimize()
        except (IOError, SolrError) as e:
            if not self.silently_fail:
                raise

            if len(models):
                self.log.error("Failed to clear Solr index of models '%s': %s", ','.join(models_to_delete), e)
            else:
                self.log.error("Failed to clear Solr index: %s", e)


class CustomSolrEngine(BaseEngine):
    backend = CustomSolrSearchBackend
    query = solr_backend.SolrSearchQuery


class SolrmarcIndexBackend(CustomSolrSearchBackend):
    """
    This is a custom Solr backend class for Haystack(ish) indexes that
    implements doing index updates via Solrmarc. All of the code here
    is derived from the code that was part of the `BibsDownloadMarc`
    `BibsToSolr` exporters (in `export.basic_exporters`). As we're
    working on additional indexes fed by Solrmarc (for Blacklight),
    it started to make more sense to move that into a lower-level
    class for more uniformity at the index and exporter levels.

    How to use this class? In Django settings, use the SolrmarcEngine
    class in your HAYSTACK_CONNECTIONS definition. Ensure that you've
    created the applicable Solr core and that you have an
    index.properties file in the solr/solrmarc project directory for
    that index. (By default you should name it <core>_index.properties,
    where <core> is the name of the Solr core.) Your haystack index
    class should be a `base.search_indexes.CustomQuerySetIndex` or
    `SolrmarcIndex` class. There are a few class attributes you can add
    to the index class to help further define how the SolrMarc process
    works--without them, sensible defaults are used.

    `s2marc_class` -- The S2MarcBatch (see `export.sierra2marc`) or
    equivalent/derived class that does the batch conversion of Sierra
    data (via the Django ORM models) to MARC records and saves them to
    the filesystem so that Solrmarc can index them. Default is
    S2MarcBatch.

    `index_properties` -- The filename for the index.properties file
    that converts the MARC files to Solr fields. As mentioned above,
    the default is '<core>_index.propertes' -- where <core> is the name
    of the Solr core for that index.

    `config_file` -- The filename for the Solrmarc config.properties
    file that defines a bunch of settings used by Solrmarc. Default is
    the SOLRMARC_CONFIG_FILE Django setting.

    `temp_filepath` -- The filesystem location where the temporary MARC
    file that gets loaded into Solrmarc is stored. Default is the
    MEDIA_ROOT Django setting.
    """

    class IndexError(Exception):
        pass

    def log_error(self, index, obj_str, err):
        err = err if isinstance(err, Exception) else self.IndexError(err)
        index.last_batch_errors.append((obj_str, err))

    def _records_to_marcfile(self, index, records):
        batch = index.s2marc_class(records)
        out_recs = batch.to_marc()
        if out_recs:
            try:
                filename = batch.to_file(out_recs, append=False)
            except IOError as e:
                raise IOError('Error writing to output file: {}'.format(e))
            for e in batch.errors:
                self.log_error(index, e.id, e.msg)
            return filename

    def _formulate_solrmarc_cmd(self, index, rec_filepath, commit):
        def_ip = '{}_index.properties'.format(self.get_core_name())
        index_properties = getattr(index, 'index_properties', None) or def_ip
        def_config = settings.SOLRMARC_CONFIG_FILE
        config_file = getattr(index, 'config_file', None) or def_config
        commit_str = 'true' if commit else 'false'
        jarfile = ('{}/../../solr/solrmarc/StanfordSearchWorksSolrMarc.jar'
                   ''.format(settings.PROJECT_DIR))
        return ('java -Xmx1g -Dsolr.hosturl="{}" '
                '-Dsolrmarc.indexing.properties="{}" '
                '-Dsolr.commit_at_end="{}" '
                '-jar "{}" {} {}'
                ''.format(self.conn.url, index_properties, commit_str, jarfile,
                          config_file, rec_filepath))

    def get_core_name(self):
        return self.conn.url.split('/')[-1]

    def update(self, index, records, commit=False):
        filedir = getattr(index, 'temp_filedir', None) or settings.MEDIA_ROOT
        if not filedir.endswith('/'):
            filedir = '{}/'.format(filedir)
        rec_filename = self._records_to_marcfile(index, records)
        if rec_filename is not None:
            rec_filepath = '{}{}'.format(filedir, rec_filename)
            cmd = self._formulate_solrmarc_cmd(index, rec_filepath, commit)
            call_opts = {'stderr': subprocess.STDOUT, 'shell': False,
                         'universal_newlines': True}
            try:
                result = subprocess.check_output(shlex.split(cmd), **call_opts)
                output = result.decode('unicode-escape')
            except subprocess.CalledProcessError as e:
                msg = ('Solrmarc process did not run successfully: {}'
                       ''.format(e.output))
                self.log_error(index, 'ERROR', msg)
            else:
                for line in output.split("\n")[:-1]:
                    line = re.sub(r'^\s+', '', line)
                    if re.match(r'^(WARN|ERROR)', line):
                        self.log_error(index, 'WARNING', line)
            os.remove(rec_filepath)


class SolrmarcEngine(BaseEngine):
    backend = SolrmarcIndexBackend
    query = solr_backend.SolrSearchQuery
