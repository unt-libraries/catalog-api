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
from blacklight import sierra2marc_alpha_solrmarc_02 as s2m
from blacklight import bl_suggest_alpha_solrmarc as suggest
from utils import solr



class BibsToAlphaSolrmarc02(ToSolrExporter):
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
        s2marc_class = s2m.S2MarcBatchBlacklightSolrMarc

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
            # self.save_and_suppress_records(using, queryset)
            super(BibsToAlphaSolrmarc02.AlphaSmIndex, self).update(using,
                                                                   commit,
                                                                   queryset)

        def delete(self, using=None, commit=True, queryset=None):
            # self.save_and_suppress_records(using, queryset)
            super(BibsToAlphaSolrmarc02.AlphaSmIndex, self).delete(using,
                                                                   commit,
                                                                   queryset)

    app_name = 'blacklight'
    IndexCfgEntry = ToSolrExporter.Index
    index_config = (IndexCfgEntry('Bibs', AlphaSmIndex, 'alpha-solrmarc-02'),)
    model = sm.BibRecord
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'b'
        }
    ]
    max_rec_chunk = 2000
    prefetch_related = [
        'record_metadata__controlfield_set',
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__location',
        'bibrecorditemrecordlink_set__item_record__location__locationname_set',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecorditemrecordlink_set__item_record__record_metadata'
            '__record_type',
        'bibrecorditemrecordlink_set__item_record__record_metadata'
            '__varfield_set',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
            '__iii_language',
        'locations',
        'locations__locationname_set',
    ]
    select_related = ['record_metadata', 'record_metadata__record_type']

    def final_callback(self, vals=None, status='success'):
        """
        If the export job was successful, the final_callback triggers a
        new job to build the bl-suggest index.
        """
        super(BibsToAlphaSolrmarc02, self).final_callback(vals, status)
        if False:
        # if status in ('success', 'done_with_errors', 'errors'):
            self.log('Info', 'Triggering bl-suggest build.')
            if self.export_filter == 'full_export':
                suggest_ftype = 'full_export'
            else:
                suggest_ftype = 'last_export'
            export_dispatch(-1, suggest_ftype, 'BuildAlphaSolrmarc02Suggest',
                            {})
        else:
            self.log('Info', 'Skipping bl-suggest build.')



class BibsToAlphaSm02AndAttachedToSolr(SameRecSetMultiExporter):
    Child = SameRecSetMultiExporter.Child
    children_config = (Child('BibsToAlphaSolrmarc02'),
                       Child('BibsAndAttachedToSolr'))
    model = sm.BibRecord
    max_rec_chunk = 500
    app_name = 'blacklight'


class BuildAlphaSolrmarc02Suggest(FromSolrMixin, ToSolrExporter):
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

        def get_end_facet_index(self, fr, limit):
            running_total, index = 0, None
            fvals = fr.facets['facet_fields'].values()[0]
            for index, facet_count in enumerate(fvals[1::2]):
                running_total += facet_count
                if running_total >= limit:
                    return index
            return index

        def pack(self, queryset, limit):
            """
            Each packed bundle is a dict containing the applicable
            facet field for an export task, the start index and size
            for the page of facet-field results to scrape, and the
            number of records and record offset for logging purposes.
            """
            for facet_field in self.heading_fields:
                start, i = 0, 0
                qs = queryset.set_raw_params({'facet.field': facet_field,
                                              'rows': 0})
                while i is not None:
                    qs = qs.set_raw_params({'facet.limit': limit,
                                            'facet.offset': start})
                    i = self.get_end_facet_index(qs.full_response, limit)
                    if i is not None:
                        yield { 'facet_field': facet_field, 'start': start,
                                'size': i + 1 }
                        start += i + 1

        def unpack(self, bundle, all_recs):
            page_params = {'facet.field': bundle['facet_field'],
                           'facet.offset': bundle['start'],
                           'facet.limit': bundle['size'],
                           'rows': 0}
            return all_recs.set_raw_params(page_params)

        def get_bundle_count(self, bundle):
            return bundle['size']

        def get_bundle_offset(self, bundle, part_num, size):
            return bundle['start']

        def get_bundle_label(self, bundle):
            return '`{}` values'.format(bundle['facet_field'])

    class SuggestIndex(object):
        """
        Custom class that implements `update`, `delete`, and `commit`
        methods (to mimic basic Haystack index functionality).
        """
        id_field = 'id'
        builder_class = suggest.BlSuggestBuilder

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
            builder = self.builder_class()
            conn = self.conn()
            page_contentqs_by, fcount_limit = 5000, 1000

            content_qs = queryset.set_raw_params({'facet': 'false',
                                                  'rows': page_contentqs_by})
            content_qs._search_params['fq'] = []
            facet_fields = queryset.full_response.facets['facet_fields']
            for facet_field, values in facet_fields.items():
                fvals, fcounts = values[::2], values[1::2]
                ext = builder.extractors_by_heading_source[facet_field]
                params = {'fl': ext.source_fields}
                content_qfield = '{}__in'.format(facet_field)
                add_srecs, del_srecs, qvals, qcounts = [], [], [], []
                for i, fval in enumerate(fvals):
                    qvals.append(fval)
                    qcounts.append(int(fcounts[i]))
                    if (sum(qcounts) >= fcount_limit) or (i == len(fvals) - 1):
                        filt = {content_qfield: qvals}
                        bibs = content_qs.set_raw_params(params).filter(**filt)
                        args = [bibs, [facet_field], qvals]
                        for srec in builder.extract_suggest_recs(*args):
                            if srec['record_count'] > 0:
                                add_srecs.append(srec)
                            else:
                                del_srecs.append(srec)
                        if del_srecs:
                            conn.delete(id=[r['id'] for r in del_srecs],
                                        commit=False)
                        if add_srecs:
                            conn.add(add_srecs, commit=False)
                        add_srecs, del_srecs, qvals, qcounts = [], [], [], []
            if commit:
                self.commit()

        def delete(self, queryset=None, commit=True):
            pass

        def commit(self):
            self.conn().commit()

    app_name = 'blacklight'
    source_solr_conn = 'alpha-solrmarc-02'
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
                return solr_qs.filter(timestamp_of_last_solr_update__gte=latest.timestamp)
        return solr_qs

    def get_records(self, prefetch=True):
        hfields = suggest.BlSuggestBuilder().all_heading_source_fields
        params = {'facet': 'true', 'facet.sort': 'index', 'facet.limit': -1,
                  'facet.mincount': 1, 'facet.field': hfields}
        qset = super(BuildAlphaSolrmarc02Suggest, self).get_records(prefetch)
        return qset.set_raw_params(params)

    def initialize(self):
        if self.export_filter == 'full_export':
            for index in self.indexes.values():
                index.conn().delete(q='*:*', commit=False)

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
