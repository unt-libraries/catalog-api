"""
Exporters module for catalog-api `blacklight` app, alpha-solrmarc.
"""

from __future__ import unicode_literals

from base import models as sm
from base.search_indexes import BibIndex
from export.exporter import ToSolrExporter
from blacklight.exporters import SameRecSetMultiExporter
from blacklight.sierra2marc import S2MarcBatchBlacklightSolrMarc


class BibsToAlphaSolrmarc(ToSolrExporter):

    class AlphaSmIndex(BibIndex):
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


class BibsToAlphaSmAndAttachedToSolr(SameRecSetMultiExporter):
    Child = SameRecSetMultiExporter.Child
    children_config = (Child('BibsToAlphaSolrmarc'),
                       Child('BibsAndAttachedToSolr'))
    model = sm.BibRecord
    max_rec_chunk = 500
    app_name = 'blacklight'
