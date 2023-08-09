"""
Exporters for the Shelflist app are defined here. See export.exporter
for the base Exporter classes and export.basic_exporters for some
implementations. This overrides the export.basic_exporters.ItemsToSolr
exporter to support features needed for the shelflistitems API
resource: a new index object (ShelflistItemIndex) and creation/storage
of shelflistitem manifests.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from export import basic_exporters as exporters
from shelflist.search_indexes import ShelflistItemIndex
from utils import redisobjs

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
        return {'seen_lcodes': self.indexes['Items'].location_set}

    def delete_records(self, records):
        seen_lcodes = self.indexes['Items'].get_location_set_from_recs(records)
        super(ItemsToSolr, self).delete_records(records)
        return {'seen_lcodes': seen_lcodes}

    def compile_vals(self, results):
        if results is not None:
            vals = {'seen_lcodes': set()}
            for result in results:
                result = result or {}
                vals['seen_lcodes'] |= result.get('seen_lcodes', set())
            return vals

    def final_callback(self, vals=None, status='success'):
        vals = vals or {}
        super(ItemsToSolr, self).final_callback(vals, status)
        lcodes = list(vals.get('seen_lcodes', []))
        total = len(lcodes)
        msg = ('Building shelflist item manifest for {} location(s): {}'
               ''.format(total, ', '.join(lcodes)))
        self.log('Info', msg)
        for i, lcode in enumerate(lcodes):
            manifest = self.indexes['Items'].get_location_manifest(lcode)
            r = redisobjs.RedisObject(self.redis_shelflist_prefix, lcode)
            r.set(manifest)
