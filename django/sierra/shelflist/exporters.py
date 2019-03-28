"""
Exporters for the Shelflist app are defined here. See export.exporter
for the base Exporter classes and export.basic_exporters for some
implementations. Here we pretty much just need to define the
final_callback method for item exporters so that any locations
represented in the items have their shelflist row manifest document in
Solr updated automatically whenever items are loaded.
"""

from __future__ import unicode_literals
import logging
import json

import pysolr

from django.conf import settings

from export import exporter, basic_exporters as exporters
from utils.redisobjs import RedisObject
from shelflist.search_indexes import ShelflistItemIndex


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


REDIS_SHELFLIST_PREFIX = 'shelflistitem_manifest'


class ItemsToSolr(exporters.ItemsToSolr):

    Index = exporters.ItemsToSolr.Index
    index_config = (
        Index('Items', ShelflistItemIndex,
              exporters.SOLR_CONNS['ItemsToSolr']),
    )
    max_rec_chunk = 500
    app_name = 'shelflist'

    def final_callback(self, vals=None, status='success'):
        super(ItemsToSolr, self).final_callback(vals, status)
        self.index_shelflist_rows()

    def index_shelflist_rows(self):
        """
        Update shelflistitem manifest in Redis based on what locations
        have been updated by this export job.
        """
        self.log('Info', 'Creating shelflist item manifests.')
        solr = self.indexes['Items'].get_backend().conn
        records = self.get_records()
        locs = records.order_by('location__code').distinct('location__code')
        for location in locs.values_list('location__code', flat=True):
            if location:
                params = {
                    'q': '*:*',
                    'fq': ['type:Item', 'location_code:{}'.format(location)],
                    'fl': 'id',
                    'sort': 'call_number_type asc, call_number_sort asc, '
                            'volume_sort asc, copy_number asc',
                }
                hits = solr.search(rows=0, **params).hits
                results = solr.search(rows=hits, **params)
                data = [i['id'] for i in results]
                r = RedisObject(REDIS_SHELFLIST_PREFIX, location)
                r.set(data)
