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

    def export_records(self, records, vals=None):
        vals = super(ItemsToSolr, self).export_records(records, vals)
        vals_manager = self.spawn_vals_manager(vals)
        vals_manager.extend('seen_lcodes', self.indexes['Items'].location_set,
                            unique=True)
        return vals_manager.vals

    def delete_records(self, records, vals=None):
        # THIS NEEDS WORK. When records are deleted, we have to get the
        # location codes where shelflist item manifests need to be
        # updated from the records in Solr before we delete this batch.
        vals = super(ItemsToSolr, self).delete_records(records, vals)
        vals_manager = self.spawn_vals_manager(vals)
        return vals_manager.vals

    def final_callback(self, vals=None, status='success'):
        vals = super(ItemsToSolr, self).final_callback(vals, status)
        vals_manager = self.spawn_vals_manager(vals)
        seen_lcodes = vals_manager.get('seen_lcodes')
        if seen_lcodes:
            self.log('Info', 'Creating shelflist item manifests for location{}'
                             ' {}'.format('s' if len(seen_lcodes) > 1 else '', 
                                          ', '.join(seen_lcodes)))
            self.indexes['Items'].update_shelflist_item_manifests(seen_lcodes)
