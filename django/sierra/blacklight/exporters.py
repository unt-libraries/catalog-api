"""
Exporters module for catalog-api `blacklight` app.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from base import models as sm
from base.search_indexes import BibIndex
from blacklight import sierra2marc as s2m
from django.conf import settings
from export.exporter import Exporter, CompoundMixin, ToSolrExporter
from export.tasks import SolrKeyRangeBundler
from haystack import exceptions
from utils import solr


class BibsToDiscover(ToSolrExporter):
    """
    This exporter populates the 'discover' indexes.
    """
    class DiscoverIndexer(BibIndex):
        reserved_fields = {
            'haystack_id': 'id',
            'django_ct': None,
            'django_id': None
        }
        conversion_pipeline = s2m.ToDiscoverPipeline()
        s2marc_class = s2m.DiscoverS2MarcBatch

        def get_qualified_id(self, record):
            try:
                return record.get_iii_recnum(False)
            except AttributeError:
                return record.record_metadata.get_iii_recnum(False)

        def log_error(self, obj_str, err):
            self.last_batch_errors.append((obj_str, err))

        def full_prepare(self, obj):
            batch_converter = self.s2marc_class(obj)
            marc_records = batch_converter.to_marc()
            errors = []

            if batch_converter.errors:
                errors.extend(batch_converter.errors)
            elif not marc_records or len(marc_records) != 1:
                id_ = self.get_qualified_id(obj)
                msg = 'Record {}: Unknown problem converting MARC.'.format(id_)
                errors.append(msg)
            else:
                marc = marc_records[0]
                try:
                    self.prepared_data = self.conversion_pipeline.do(obj, marc)
                except Exception as e:
                    id_ = self.get_qualified_id(obj)
                    errors.append('Record {}: {}'.format(id_, e))
            if errors:
                for error in errors:
                    self.log_error('WARNING', error)
                raise exceptions.SkipDocument()
            return self.prepared_data

    app_name = 'blacklight'
    IndexCfgEntry = ToSolrExporter.Index
    index_config = (IndexCfgEntry('Bibs', DiscoverIndexer,
                                  settings.BL_CONN_NAME),)
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
        'record_metadata__leaderfield_set',
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

    def get_records(self, prefetch=True):
        self.options['other_updated_rtype_paths'] = [
            'bibrecorditemrecordlink__item_record'
        ]
        return super(ToSolrExporter, self).get_records(prefetch)


class BibsToDiscoverAndAttachedToSolr(SameRecSetMultiExporter):
    Child = SameRecSetMultiExporter.Child
    children_config = (Child('BibsToDiscover'),
                       Child('BibsAndAttachedToSolr'))
    model = sm.BibRecord
    max_rec_chunk = 500
    app_name = 'blacklight'

    def get_records(self, prefetch=True):
        self.options['other_updated_rtype_paths'] = [
            'bibrecorditemrecordlink__item_record'
        ]
        return super(SameRecSetMultiExporter, self).get_records(prefetch)
