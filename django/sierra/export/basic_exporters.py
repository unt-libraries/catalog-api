"""
Default base Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
"""
from __future__ import unicode_literals
from __future__ import absolute_import
import logging

from django.conf import settings
from six import iteritems

from base import models as sierra_models
from base import search_indexes as indexes
from export.exporter import (Exporter, ToSolrExporter, MetadataToSolrExporter,
                             CompoundMixin, AttachedRecordExporter)
from utils import helpers, redisobjs, solr


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


SOLR_CONNS = settings.EXPORTER_HAYSTACK_CONNECTIONS


class LocationsToSolr(MetadataToSolrExporter):
    """
    Defines process to load Locations into Solr.
    """
    Index = MetadataToSolrExporter.Index
    index_config = (
        Index('Locations', indexes.LocationIndex,
              SOLR_CONNS['LocationsToSolr']),
    )
    model = sierra_models.Location


class ItypesToSolr(MetadataToSolrExporter):
    """
    Defines process to load Itypes into Solr.
    """
    Index = MetadataToSolrExporter.Index
    index_config = (
        Index('Itypes', indexes.ItypeIndex, SOLR_CONNS['ItypesToSolr']),
    )
    model = sierra_models.ItypeProperty


class ItemStatusesToSolr(MetadataToSolrExporter):
    """
    Defines process to load item statuses into Solr.
    """
    Index = MetadataToSolrExporter.Index
    index_config = (
        Index('ItemStatuses', indexes.ItemStatusIndex,
              SOLR_CONNS['ItemStatusesToSolr']),
    )
    model = sierra_models.ItemStatusProperty


class ItemsToSolr(ToSolrExporter):
    """
    Defines processes that load item records into Solr.
    """
    Index = ToSolrExporter.Index
    index_config = (
        Index('Items', indexes.ItemIndex, SOLR_CONNS['ItemsToSolr']),
    )
    model = sierra_models.ItemRecord
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'i'
        }
    ]
    prefetch_related = [
        'record_metadata__varfield_set',
        'checkout',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__bib_record',
        'bibrecorditemrecordlink_set__bib_record__record_metadata',
        'bibrecorditemrecordlink_set__bib_record__record_metadata'
            '__record_type',
        'bibrecorditemrecordlink_set__bib_record__record_metadata'
            '__varfield_set',
        'bibrecorditemrecordlink_set__bib_record__bibrecordproperty_set'
    ]
    select_related = ['record_metadata', 'record_metadata__record_type',
                      'location', 'itype', 'item_status']


class EResourcesToSolr(ToSolrExporter):
    """
    Defines processes that load resource records into Solr.
    """
    Index = ToSolrExporter.Index
    index_config = (
        Index('EResources', indexes.ElectronicResourceIndex,
              SOLR_CONNS['EResourcesToSolr']),
    )
    model = sierra_models.ResourceRecord
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'e'
        }
    ]
    prefetch_related = [
        'access_provider__record_metadata__varfield_set',
        'holding_records',
        'holding_records__bibrecord_set',
        'holding_records__bibrecord_set__record_metadata',
        'holding_records__bibrecord_set__record_metadata__varfield_set',
        'holding_records__record_metadata',
        'holding_records__record_metadata__record_type',
        'record_metadata__varfield_set',
        'resourcerecordholdingrecordrelatedlink_set',
        'resourcerecordholdingrecordrelatedlink_set__holding_record__'\
            'bibrecord_set'
    ]
    select_related = ['record_metadata', 'record_metadata__record_type',
                      'access_provider', 'access_provider__record_metadata']

    max_rec_chunk = 20

    def export_records(self, records):
        self.indexes['EResources'].do_update(records)
        return { 'h_lists': self.indexes['EResources'].h_lists }

    def delete_records(self, records):
        self.indexes['EResources'].do_delete(records)
        return { 'deletions': [r.get_iii_recnum(True) for r in records] }

    def commit_to_redis(self, vals):
        self.log('Info', 'Committing EResource updates to Redis...')
        rhl_obj = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_holdings = rhl_obj.get() or {}
        
        # Update holdings for updated eresources
        for ernum, h_list in vals.get('h_lists', {}).items():
            redisobjs.RedisObject('eresource_holdings_list', ernum).set(h_list)
            for hrnum in h_list:
                reverse_holdings[hrnum] = ernum

        # Delete holdings for deleted eresources
        deletions = vals.get('deletions', [])
        for ernum in deletions:
            ehl_obj = redisobjs.RedisObject('eresource_holdings_list', ernum)
            ehl_obj.conn.delete(ehl_obj.key)

        if deletions:
            for hrnum, ernum in list(reverse_holdings.items()):
                if ernum in deletions:
                    del(reverse_holdings[hrnum])

        rhl_obj.set(reverse_holdings)

    def final_callback(self, vals=None, status='success'):
        vals = vals or {}
        self.commit_to_redis(vals)
        self.commit_indexes()


class HoldingUpdate(CompoundMixin, Exporter):
    """
    Checks for updates to holdings and updates linked EResources as
    needed.

    This is a little more complicated than others. Normally, I'd just
    have the eresource record attached to each holding update itself
    by reindexing itself. However, we have some eresources that have
    10,000+ holdings--if we update these, then they reload all of those
    holdings. This either takes forever or uses up all our memory. So
    instead, we try to grab the eresource record from Solr, update the
    holding information for each individual holding that's been changed
    added or deleted, and then save the record back to Solr. This keeps
    our Sierra DB access to a minimium. We're also using Redis to store
    an index of holding record IDs to the Solr holdings field index to
    help us manage this (since we need a way to identify holdings
    records by more than just their title).
    """
    Child = CompoundMixin.Child
    children_config = (Child('EResourcesToSolr'),)
    model = sierra_models.HoldingRecord
    # deletion_filter = [
    #     {
    #         'deletion_date_gmt__isnull': False,
    #         'record_type__code': 'c'
    #     }
    # ]
    prefetch_related = [
        'bibrecord_set',
        'bibrecord_set__record_metadata__varfield_set',
        'resourcerecord_set',
        'resourcerecord_set__record_metadata__varfield_set',
        'resourcerecord_set__holding_records'
    ]
    
    def __init__(self, *args, **kwargs):
        super(HoldingUpdate, self).__init__(*args, **kwargs)
        self.max_rec_chunk = self.children['EResourcesToSolr'].max_rec_chunk

    def export_records(self, records):
        eresources, er_mapping = set(), {}
        # First we loop through the holding records and determine which
        # eresources need to be updated. er_mapping maps eresource rec
        # nums to lists of holdings rec nums to update.
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_holdings_list = rev_handler.get() or {}
        for h in records:
            h_rec_num = h.record_metadata.get_iii_recnum(True)
            old_er_rec_num = reverse_holdings_list.get(h_rec_num, None)
            try:
                er_record = h.resourcerecord_set.all()[0]
            except IndexError:
                er_record, er_rec_num = None, None
            else:
                er_rec_num = er_record.record_metadata.get_iii_recnum(True)

            if old_er_rec_num and old_er_rec_num != er_rec_num:
                # if the current attached er rec_num in Sierra is
                # different than what's in Redis, then we need to
                # delete this holding from the old er record.
                old_h_data = er_mapping.get(old_er_rec_num, [])
                old_h_data.append({
                    'delete': True,
                    'rec_num': h_rec_num,
                    'title': None
                })
                er_mapping[old_er_rec_num] = {
                    'er_record': None,
                    'holdings': old_h_data
                }

            if er_rec_num:
                holdings = er_mapping.get(er_rec_num, {}).get('holdings', [])
                try:
                    vf = h.bibrecord_set.all()[0].record_metadata\
                            .varfield_set.all()
                except IndexError:
                    title = None
                else:
                    title = helpers.get_varfield_vals(vf, 't', '245',
                                cm_kw_params={'subfields': 'a'},
                                content_method='display_field_content')
                data = {
                    'delete': False,
                    'title': title,
                    'rec_num': h_rec_num
                }
                holdings.append(data)
                er_mapping[er_rec_num] = {
                    'er_record': er_record,
                    'holdings': holdings
                }

        h_vals = {}
        #self.log('Info', er_mapping)
        for er_rec_num, entry in iteritems(er_mapping):
            er_record, holdings = entry['er_record'], entry['holdings']
            # if we've already indexed the eresource this holding is
            # attached to, then we want to pull the record from Solr
            # and make whatever changes to it rather than reindex the
            # whole record and all attached holdings from scratch.
            # Since export jobs get broken up and run in parallel, we
            # want to hold off on actually committing to Solr and
            # updating Redis until the callback runs.
            s = solr.Queryset().filter(record_number=er_rec_num)
            if s.count() > 0:
                rec_queue = h_vals.get(er_rec_num, {})
                rec_append_list = rec_queue.get('append', [])
                rec_delete_list = rec_queue.get('delete', [])

                record = s[0]
                red = redisobjs.RedisObject('eresource_holdings_list',
                                            er_rec_num)
                red_h_list = red.get()
                for data in holdings:
                    try:
                        red_h_index = red_h_list.index(data.get('rec_num'))
                    except AttributeError:
                        self.log('Info', '{}'.format(data.get('rec_num')))
                    except ValueError:
                        record.holdings.append(data.get('title'))
                        rec_append_list.append(data.get('rec_num'))
                    else:
                        if data.get('delete'):
                            # we wait until the final callback to
                            # delete anything from Solr, because that
                            # will mess up our holdings index number
                            rec_delete_list.append(data.get('rec_num'))
                        else:
                            record.holdings[red_h_index] = data.get('title')

                record.save(commit=False)
                rec_queue['append'] = rec_append_list
                rec_queue['delete'] = rec_delete_list
                h_vals[er_rec_num] = rec_queue
            else:
                # if we haven't indexed the record already, we'll add
                # it using the Haystack indexer.
                eresources.add(er_record)

        if eresources:
            eresources = list(eresources)
            ret_er_vals = self.eresources_to_solr.export_records(eresources)

        return { 'holdings': h_vals, 'eresources': ret_er_vals }

    def commit_to_redis(self, vals):
        self.log('Info', 'Committing Holdings updates to Redis...')
        h_vals = vals.get('holdings', {})
        er_vals = vals.get('eresources', {})
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_h_list = rev_handler.get()
        for er_rec_num, lists in (h_vals or {}).items():
            s = solr.Queryset().filter(record_number=er_rec_num)
            try:
                record = s[0]
            except IndexError:
                record = None

            er_handler = redisobjs.RedisObject('eresource_holdings_list',
                                               er_rec_num)
            h_list = er_handler.get()
            for h_rec_num in lists.get('delete', []):
                h_index = h_list.index(h_rec_num)
                del(h_list[h_index])
                del(reverse_h_list[h_rec_num])
                del(record.holdings[h_index])
            for h_rec_num in lists.get('append', []):
                h_list.append(h_rec_num)
                reverse_h_list[h_rec_num] = er_rec_num
            record.save()
            er_handler.set(h_list)
        rev_handler.set(reverse_h_list)

    def final_callback(self, vals=None, status='success'):
        vals = vals or {}
        self.children['EResourcesToSolr'].final_callback(vals, status)
        self.commit_to_redis(vals)


class BibsDownloadMarc(Exporter):
    """
    This exporter is now deprecated--please do not use.

    Previously this defined processes that convert Sierra bib records
    to MARC, but now that is handled through a custom Solr backend for
    Haystack.

    `BibsDownloadMarc` will be removed in the version 1.5.
    """
    max_rec_chunk = 2000
    parallel = False
    model = sierra_models.BibRecord
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecorditemrecordlink_set__item_record__record_metadata'
            '__record_type',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
            '__iii_language',
    ]
    select_related = ['record_metadata', 'record_metadata__record_type']

    def _warn(self):
        msg = ('The `BibsDownloadMarc` exporter is deprecated and will be '
               'removed in version 1.5.')
        self.log('Warning', msg)

    def __init__(self, *args, **kwargs):
        super(BibsDownloadMarc, self).__init__(*args, **kwargs)
        self._warn()

    def export_records(self, records):
        batch = S2MarcBatch(records)
        out_recs = batch.to_marc()
        try:
            filename = batch.to_file(out_recs, append=False)
        except IOError as e:
            raise IOError('Error writing to output file: {}'.format(e))
        else:
            for e in batch.errors:
                self.log('Warning', 'Record {}: {}'.format(e.id, e.msg))
        return { 'marcfile': filename }


class BibsToSolr(ToSolrExporter):
    """
    Defines processes that export Sierra/MARC bibs out to Solr.
    """
    Index = ToSolrExporter.Index
    index_config = (
        Index('Bibs', indexes.BibIndex, SOLR_CONNS['BibsToSolr:BIBS']),
        # Index('MARC', indexes.MarcIndex, SOLR_CONNS['BibsToSolr:MARC'])
    )
    model = sierra_models.BibRecord
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
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecorditemrecordlink_set__item_record__record_metadata'
            '__record_type',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
            '__iii_language',
    ]
    select_related = ['record_metadata', 'record_metadata__record_type']
    is_active = False

    def export_records(self, records):
        pass

    def delete_records(self, records):
        pass


class ItemsBibsToSolr(AttachedRecordExporter):
    """
    Exports item records based on the provided export_filter using the
    existing ItemsToSolr job and then grabs the items' parent bibs and
    exports them using BibsToSolr.
    
    If using this in production, make sure to use it in conjunction
    with a bib loader, like BibsToSolr or BibsItemsToSolr. That way
    bib records that need to be deleted will actually get deleted.
    """
    Child = AttachedRecordExporter.Child

    class BibChild(Child):

        rel_prefix = 'bibrecorditemrecordlink_set__bib_record'

        def derive_records_from_parent(self, parent_record):
            bib_links = parent_record.bibrecorditemrecordlink_set.all()
            return [link.bib_record for link in bib_links]

    children_config = (Child('ItemsToSolr'), BibChild('BibsToSolr'))
    model = sierra_models.ItemRecord


class BibsAndAttachedToSolr(AttachedRecordExporter):
    """
    Exports bib records based on the provided export_filter using the
    existing BibsToSolr job and then grabs any attached items and
    exports them using the specified export processes.
    """
    Child = AttachedRecordExporter.Child

    class ItemChild(Child):

        rel_prefix = 'bibrecorditemrecordlink_set__item_record'

        def derive_records_from_parent(self, parent_record):
            item_links = parent_record.bibrecorditemrecordlink_set.all()
            return [link.item_record for link in item_links]

    children_config = (Child('BibsToSolr'), ItemChild('ItemsToSolr'))
    model = sierra_models.BibRecord
    max_rec_chunk = 500
