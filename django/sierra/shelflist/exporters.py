"""
Exporters for the Shelflist app are defined here. See export.exporter
for the base Exporter class and export.basic_exporters for some
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

from export import exporter
from export import basic_exporters as exporters
from utils.redisobjs import RedisObject
from . import search_indexes as indexes


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')

def index_shelflist_rows(exp):
    """
    This is what does the work of determining the locations that need
    to be updated (based on what locations appear in the exporter
    object's record set) and stuff.
    """
    exp.log('Info', 'Creating shelflist item manifests.')
    solr = pysolr.Solr(settings.HAYSTACK_CONNECTIONS[exp.hs_conn]['URL'])
    records = exp.get_records()
    locations = records.order_by('location__code').distinct('location__code'
                        ).values_list('location__code', flat=True)
    for location in locations:
        if location:
            params = {
                'q': '*:*',
                'fq': ['type:Item', 'location_code:{}'.format(location)],
                'fl': 'id',
                'sort': 'call_number_type asc, call_number_sort asc, volume_sort '
                        'asc, copy_number asc',
            }
            hits = solr.search(rows=0, **params).hits
            results = solr.search(rows=hits, **params)
            data = [i['id'] for i in results]
            r = RedisObject('shelflistitem_manifest', location)
            r.set(data)


class ItemsToSolr(exporters.ItemsToSolr):
    max_rec_chunk = 500
    index_class = indexes.ShelflistItemIndex
    def final_callback(self, vals=None, status='success'):
        super(ItemsToSolr, self).final_callback(vals, status)
        index_shelflist_rows(self)
