'''
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
'''

from django.apps import apps

from haystack.backends import solr_backend, BaseEngine
from haystack.models import SearchResult
from haystack.constants import ID, DJANGO_CT, DJANGO_ID
from haystack.utils import get_model_ct
from haystack import connections


def is_sequence(arg):
    return (not hasattr(arg, 'strip') and 
            hasattr(arg, '__getitem__') or
            hasattr(arg, '__iter__'))


def custom_to_python(val, _to_python):
    '''
    Simplest way I could think of to add what we want to
    pysolr._to_python. Recursively unpacks all values in any sequence
    and returns the final data structure.
    '''
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