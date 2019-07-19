"""
Exporters module for catalog-api `blacklight` app, alpha-solrmarc.
"""

from __future__ import unicode_literals
import re

from base import models as sm
from base.search_indexes import BibIndex
from export.exporter import ToSolrExporter
from export.tasks import export_dispatch, RecordSetBundler
from export.models import ExportInstance
from blacklight.exporters import SameRecSetMultiExporter, FromSolrMixin
from blacklight.sierra2marc import S2MarcBatchBlacklightSolrMarc
from blacklight import bl_suggest_alpha_solrmarc as suggest
from utils import solr



class BibsToAlphaSolrmarc(ToSolrExporter):
    """
    This exporter populates the 'alpha-solrmarc' index we're using for
    the Blacklight alpha phase.
    """
    class AlphaSmIndex(BibIndex):
        """
        A custom base.search_indexes.BibIndex (Haystack) index class
        for the 'alpha-solrmarc' index. The main customization this
        adds is running `save_and_suppress_records` when records are
        updated or deleted.
        """
        reserved_fields = {
            'haystack_id': 'id',
            'django_ct': None,
            'django_id': None
        }
        s2marc_class = S2MarcBatchBlacklightSolrMarc

        def get_qualified_id(self, record):
            try:
                return record.get_iii_recnum(False)
            except AttributeError:
                return record.record_metadata.get_iii_recnum(False)

        def save_and_suppress_records(self, using, queryset):
            """
            Save/cache a suppressed version of any records in the given
            `queryset`, with only whatever fields are needed to build
            bl-suggest records. This method is meant to be run over
            a queryset containing records to be updated, added, or
            deleted *before* records are modified. This is to help do
            incremental updates to the bl-suggest index.
            """
            using = using or self.using
            limit, records = 1000, [r for r in queryset]
            for start in range(0, len(records), limit):
                end = start + limit
                ids = [self.get_qualified_id(r) for r in records[start:end]]
                saved = []
                src_fields = suggest.BlSuggestBuilder().all_source_fields
                src_fields |= set(['_version_', 'id'])
                q = solr.Queryset(using=using).filter(id__in=ids)
                for match in q.set_raw_params({'fl': tuple(src_fields)}):
                    match['id'] = '{}_{}'.format(match['id'],
                                                 match['_version_'])
                    match['suppressed'] = True
                    match['_version_'] = None
                    saved.append(match)
                self.get_backend().conn.add(saved, commit=False)

        def update(self, using=None, commit=True, queryset=None):
            self.save_and_suppress_records(using, queryset)
            super(BibsToAlphaSolrmarc.AlphaSmIndex, self).update(using, commit,
                                                                 queryset)

        def delete(self, using=None, commit=True, queryset=None):
            self.save_and_suppress_records(using, queryset)

    app_name = 'blacklight'
    IndexCfgEntry = ToSolrExporter.Index
    index_config = (IndexCfgEntry('Bibs', AlphaSmIndex, 'alpha-solrmarc'),)
    model = sm.BibRecord
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'b'
        }
    ]
    max_rec_chunk = 2000
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__location',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecorditemrecordlink_set__item_record__record_metadata'
            '__record_type',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
            '__iii_language',
        'locations',
    ]
    select_related = ['record_metadata', 'record_metadata__record_type']

    def final_callback(self, vals=None, status='success'):
        """
        If the export job was successful, the final_callback triggers a
        new job to build the bl-suggest index.
        """
        super(BibsToAlphaSolrmarc, self).final_callback(vals, status)
        if status in ('success', 'done_with_errors', 'errors'):
            self.log('Info', 'Triggering bl-suggest build.')
            if self.export_filter == 'full_export':
                suggest_ftype = 'full_export'
            else:
                suggest_ftype = 'last_export'
            export_dispatch(-1, suggest_ftype, 'BuildAlphaSolrmarcSuggest', {})
        else:
            self.log('Info', 'Skipping bl-suggest build.')



class BibsToAlphaSmAndAttachedToSolr(SameRecSetMultiExporter):
    Child = SameRecSetMultiExporter.Child
    children_config = (Child('BibsToAlphaSolrmarc'),
                       Child('BibsAndAttachedToSolr'))
    model = sm.BibRecord
    max_rec_chunk = 500
    app_name = 'blacklight'


class BuildAlphaSolrmarcSuggest(FromSolrMixin, ToSolrExporter):
    """
    Exporter that builds the bl-suggest index based on the alpha-
    solrmarc content index.
    """
    class FacetValueBundler(RecordSetBundler):
        """
        This bundler batches up groups of headings by paging through
        facet listings for a particular queryset.
        """
        def __init__(self, heading_fields):
            """
            On init, specify a list, tuple, or set of `heading_fields`
            needed to build the various types of suggest records in the
            suggest index.
            """
            self.heading_fields = heading_fields

        def get_num_facets(self, fr):
            ct = sum(len(v) for v in fr.facets['facet_fields'].values()) / 2
            return ct

        def set_base_qs_params(self, qs):
            params = {'facet': 'true', 'rows': 0, 'facet.sort': 'index',
                      'facet.mincount': 1}
            return qs.set_raw_params(params)

        def pack(self, queryset, size):
            """
            Each packed bundle is a dict containing the applicable
            facet field for an export task, the start index and size
            for the page of facet-field results to scrape, and the
            number of records and record offset for logging purposes.
            """
            qs = self.set_base_qs_params(queryset)
            for facet_field in self.heading_fields:
                start, rec_offset, numrecs = 0, 0, 1
                qs = qs.set_raw_params({'facet.field': facet_field})
                while numrecs > 0:
                    qs = qs.set_raw_params({'facet.limit': size,
                                            'facet.offset': start})
                    numrecs = self.get_num_facets(qs.full_response)
                    if numrecs > 0:
                        yield { 'facet_field': facet_field, 'start': start,
                                'size': size, 'rec_offset': rec_offset,
                                'numrecs': numrecs }
                        start += size
                        rec_offset += numrecs

        def unpack(self, bundle, all_recs):
            qs = self.set_base_qs_params(all_recs)
            page_params = {'facet.field': bundle['facet_field'],
                           'facet.offset': bundle['start'],
                           'facet.limit': bundle['size']}
            return qs.set_raw_params(page_params)

        def get_bundle_count(self, bundle):
            return bundle['numrecs']

        def get_bundle_offset(self, bundle, part_num, size):
            return bundle['rec_offset']

        def get_bundle_label(self, bundle):
            return '`{}` values'.format(bundle['facet_field'])

    class SuggestIndex(object):
        """
        Custom class that implements `update`, `delete`, and `commit`
        methods (to mimic basic Haystack index functionality).
        """
        id_field = 'id'

        def __init__(self, using='default'):
            self.using = using
            self.last_batch_errors = []

        def conn(self):
            return solr.connect(using=self.using)

        def _update_batch(self, srecs, batch_size, is_delete=False):
            for start in range(0, len(srecs), batch_size):
                batch = srecs[start:start+batch_size]
                if is_delete:
                    self.conn().delete(id=[r['id'] for r in batch])
                else:
                    self.conn().add(batch, commit=False)

        def update(self, queryset=None, commit=True):
            """
            This method is pretty convoluted. It takes the given Solr
            `queryset` and runs through the facet_field results from
            Solr to build bl-suggest records and load them into Solr,
            in batches of 1000.

            Because the suggest builder needs to use records from the
            content index to build the suggest records, it uses the
            same queryset to pull matching records for each facet val.
            It compiles these into batches of (max) 1000 facet values,
            taking care not to load too many content records into
            memory at one time (based on facet value counts).

            As suggest records are built, the `record_count` value is
            checked; if a suggest record has a count of 0, then it
            means all content records associated with that facet value
            have been suppressed and the record is queued for deletion.
            Otherwise it's queued for update. 
            """
            builder = suggest.BlSuggestBuilder()
            content_qs = queryset.set_raw_params({'facet': 'false',
                                                  'rows': 10000})
            content_qs._search_params['fq'] = []
            add_srecs, del_srecs, limit = [], [], 1000
            facet_fields = queryset.full_response.facets['facet_fields']
            for facet_field, values in facet_fields.items():
                fvals, fcounts = values[::2], values[1::2]
                ext = builder.extractors_by_heading_source[facet_field]
                content_qfield = '{}__in'.format(facet_field)
                qvals, qcounts = [], []
                for i, fval in enumerate(fvals):
                    qvals.append(fval)
                    qcounts.append(int(fcounts[i]))
                    if (sum(qcounts) >= limit) or (i == len(fvals) - 1):
                        params = {'fl': ext.source_fields}
                        filt = {content_qfield: qvals}
                        bibs = content_qs.set_raw_params(params).filter(**filt)
                        args = [bibs, [facet_field], qvals]
                        for srec in builder.extract_suggest_recs(*args):
                            if srec['record_count'] > 0:
                                add_srecs.append(srec)
                            else:
                                del_srecs.append(srec)
                        qvals, qcounts = [], []
            self._update_batch(add_srecs, 1000)
            self._update_batch(del_srecs, 1000, is_delete=True)
            if commit:
                self.commit()

        def delete(self, queryset=None, commit=True):
            pass

        def commit(self):
            self.conn().commit()

    app_name = 'blacklight'
    source_solr_conn = 'alpha-solrmarc'
    IndexCfgEntry = ToSolrExporter.Index
    index_config = (IndexCfgEntry('suggest', SuggestIndex, 'bl-suggest'),)
    solr_id_field = SuggestIndex.id_field
    max_rec_chunk = 10000

    @property
    def bundler(self):
        heading_fields = suggest.BlSuggestBuilder().all_heading_source_fields
        return self.FacetValueBundler(heading_fields)

    def filter_by(self, solr_qs, export_filter, options=None):
        if export_filter == 'last_export':
            try:
                latest = ExportInstance.objects.filter(
                    export_type=self.export_type,
                    status__in=('success', 'done_with_errors', 'errors')
                ).order_by('-timestamp')[0]
            except IndexError:
                msg = ('The `last_export` export filter was selected for this '
                       'job, but a suitable export task has never been run '
                       'before. Defaulting to `full_export`.')
                self.log('Warning', msg)
            else:
                return solr_qs.filter(timestamp__gte=latest.timestamp)
        return solr_qs

    def final_callback(self, vals=None, status='success'):
        """
        When this exporter is finished running, it needs to delete the
        temporary cached/suppressed records that were created when the
        alpha-solrmarc content update process ran. It skips that
        deletion if there were any errors during this process.
        """
        if status == 'success':
            self.log('Info', 'Removing suppressed records from {} index.'
                     ''.format(self.source_solr_conn))
            conn = solr.connect(using=self.source_solr_conn)
            conn.delete(q='suppressed:true', commit=True)
        else:
            self.log('Info', 'Errors occurred during this process, so '
                             'suppressed records in the {} index have not '
                             'been removed. Please resolve the errors and '
                             'run this process again.'
                             ''.format(self.source_solr_conn))
        self.commit_indexes()
