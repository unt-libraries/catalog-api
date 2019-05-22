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

from export import exporter, basic_exporters as exporters
from shelflist.search_indexes import ShelflistItemIndex
from utils import solr, redisobjs


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class ItemsToSolr(exporters.ItemsToSolr):

    Index = exporters.ItemsToSolr.Index
    index_config = (
        Index('Items', ShelflistItemIndex,
              exporters.SOLR_CONNS['ItemsToSolr']),
    )
    max_rec_chunk = 500
    app_name = 'shelflist'
    redis_shelflist_prefix = 'shelflistitem_manifest'

    def export_records(self, records):
        super(ItemsToSolr, self).export_records(records)
        return { 'seen_lcodes': self.indexes['Items'].location_set }

    def delete_records(self, records):
        seen_lcodes = self.indexes['Items'].get_location_set_from_recs(records)
        super(ItemsToSolr, self).delete_records(records)
        return { 'seen_lcodes': seen_lcodes }

    def compile_vals(self, results):
        vals = { 'seen_lcodes': set() }
        for r in results:
            vals['seen_lcodes'] |= r['seen_lcodes']
        return vals

    def final_callback(self, vals=None, status='success'):
        super(ItemsToSolr, self).final_callback(vals, status)
        if vals['seen_lcodes']:
            self.log('Info', 'Creating shelflist item manifests for: {}'
                             ''.format(', '.join(seen_lcodes)))
            for lcode in vals['seen_lcodes']:
                manifest = self.indexes['Items'].get_location_manifest(lcode)
                r = redisobjs.RedisObject(self.redis_shelflist_prefix, lcode)
                r.set(manifest)
