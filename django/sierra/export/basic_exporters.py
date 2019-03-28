"""
Default base Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
"""
from __future__ import unicode_literals
import logging
import re
import subprocess
import os

from django.conf import settings

from base import models as sierra_models
from base import search_indexes as indexes
from export import models as export_models
from export.exporter import (Exporter, ToSolrExporter, MetadataToSolrExporter,
                             CompoundMixin, AttachedRecordExporter)
from export.sierra2marc import S2MarcError, S2MarcBatch
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
        'bibrecorditemrecordlink_set__bib_record__record_metadata',
        'bibrecorditemrecordlink_set__bib_record__record_metadata'
            '__varfield_set',
        'bibrecorditemrecordlink_set__bib_record__bibrecordproperty_set'
    ]
    select_related = ['record_metadata', 'location', 'itype']


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
        'record_metadata__varfield_set',
        'resourcerecordholdingrecordrelatedlink_set',
        'resourcerecordholdingrecordrelatedlink_set__holding_record__'\
            'bibrecord_set'
    ]
    select_related = ['record_metadata']

    max_rec_chunk = 20

    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        try:
            self.indexes['EResources'].do_update(records)
        except Exception as e:
            self.log_error(e)
        else:
            vals_manager.update('h_lists', self.indexes['EResources'].h_lists)
        return vals_manager.vals

    def commit_to_redis(self, vm):
        self.log('Info', 'Committing EResource updates to Redis...')
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_holdings_list = rev_handler.get() or {}
        for er_rec_num, h_list in (vm.get('h_lists') or {}).iteritems():
            er_handler = redisobjs.RedisObject('eresource_holdings_list',
                                               er_rec_num)
            er_handler.set(h_list)

            for h_rec_num in h_list:
                reverse_holdings_list[h_rec_num] = er_rec_num
            
            #self.log('Info', 'Number of holdings in reverse_holdings_list: {}'
            #                  .format(len(reverse_holdings_list.keys())))

        rev_handler.set(reverse_holdings_list)

    def final_callback(self, vals=None, status='success'):
        vals_manager = self.spawn_vals_manager(vals)
        self.commit_to_redis(vals_manager)
        self.commit_indexes()
        return vals_manager.vals


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

    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
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

        h_vals = vals_manager.get('h_vals') or {}
        #self.log('Info', er_mapping)
        for er_rec_num, entry in er_mapping.iteritems():
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
                rec_queue = holdings.get(er_rec_num, {})
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

        vals_manager.set('h_vals', h_vals)

        if eresources:
            eresources = list(eresources)
            er_to_solr = self.children['EResourcesToSolr']
            er_vals = er_to_solr.export_records(eresources, vals_manager.vals)
            vals_manager.merge(er_vals)

        return vals_manager.vals

    def commit_to_redis(self, vm):
        self.log('Info', 'Committing Holdings updates to Redis...')
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_h_list = rev_handler.get()
        for er_rec_num, lists in (vm.get('h_vals') or {}).iteritems():
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
        vals_manager = self.spawn_vals_manager(vals)
        self.children['EResourcesToSolr'].final_callback(vals_manager.vals,
                                                         status)
        self.commit_to_redis(vals_manager)
        return vals_manager.vals


class BibsDownloadMarc(Exporter):
    """
    Defines processes that convert Sierra bib records to MARC.
    """
    max_rec_chunk = 1000
    parallel = False
    model = sierra_models.BibRecord
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
    ]
    select_related = ['record_metadata']
        
    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        batch = S2MarcBatch(records)
        out_recs = batch.to_marc()
        marcfile = vals_manager.get('marcfile')
        append = False if marcfile is None else True
        try:
            marcfile = batch.to_file(out_recs, marcfile, append=append)
        except IOError as e:
            self.log('Error', 'Error writing to output file: {}'.format(e))
        else:
            for e in batch.errors:
                self.log('Warning', 'Record {}: {}'.format(e.id, e.msg))
            success_count = vals_manager.get('success_count') or 0
            success_count += batch.success_count
            vals_manager.set('success_count', success_count)
        vals_manager.set('marcfile', marcfile)
        return vals_manager.vals

    def final_callback(self, vals=None, status='success'):
        vals_manager = self.spawn_vals_manager(vals)
        success_count = vals_manager.get('success_count')
        marcfile = vals_manager.get('marcfile')
        if success_count is not None:
            self.log('Info', '{} records successfully processed.'
                     ''.format(success_count))
        if marcfile is not None:
            self.log('Info', '<a href="{}{}">Download File</a> '
                    '(Link expires after 24 hrs.)'
                    ''.format(settings.MEDIA_URL, marcfile))
        return vals_manager.vals


class BibsToSolr(CompoundMixin, ToSolrExporter):
    """
    Defines processes that export Sierra/MARC bibs out to Solr.
    """
    Index = ToSolrExporter.Index
    Child = CompoundMixin.Child
    index_config = (
        Index('Bibs', indexes.BibIndex, SOLR_CONNS['BibsToSolr:BIBS']),
        Index('MARC', indexes.MarcIndex, SOLR_CONNS['BibsToSolr:MARC'])
    )
    children_config = (Child('BibsDownloadMarc'),)
    model = sierra_models.BibRecord
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'b'
        }
    ]
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
    ]
    select_related = ['record_metadata']
    max_rec_chunk = 1000
    
    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        cmd = 'bash'
        index_script = settings.SOLRMARC_COMMAND
        config_file = settings.SOLRMARC_CONFIG_FILE
        filedir = settings.MEDIA_ROOT
        if filedir[-1] != '/':
            filedir = '{}/'.format(filedir)
        bib_converter = self.children['BibsDownloadMarc']
        child_vals = bib_converter.export_records(records)
        child_vm = bib_converter.spawn_vals_manager(child_vals)
        filepath = '{}{}'.format(filedir, child_vm.get('marcfile'))
        try:
            output = subprocess.check_output([cmd, index_script, config_file,
                                             filepath], 
                                             stderr=subprocess.STDOUT,
                                             shell=False,
                                             universal_newlines=True)
            output = output.decode('unicode-escape')
        except subprocess.CalledProcessError as e:
            error_lines = e.output.split("\n")
            for line in error_lines:
                self.log('Error', line)
            self.log('Error', 'Solrmarc process did not run successfully.')
        else:
            error_lines = output.split("\n")
            del(error_lines[-1])
            if error_lines:
                for line in error_lines:
                    line = re.sub(r'^\s+', '', line)
                    if re.match(r'^WARN', line):
                        self.log('Warning', line)
                    elif re.match(r'^ERROR', line):
                        self.log('Error', line)

            # if all went well, we now try to index the MARC record
            try:
                self.indexes['MARC'].do_update(records)
            except Exception as e:
                self.log_error(e)

        # delete the file when we're done so we don't take up space
        os.remove(filepath)
        return vals_manager.vals


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
        def derive_records(self, parent_record):
            bib_links = parent_record.bibrecorditemrecordlink_set.all()
            return [link.bib_record for link in bib_links]

    children_config = (Child('ItemsToSolr'), BibChild('BibsToSolr'))
    model = sierra_models.ItemRecord

    @property
    def prefetch_related(self):
        return self.main_child.prefetch_related + [
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record__record_metadata'
        ]


class BibsAndAttachedToSolr(AttachedRecordExporter):
    """
    Exports bib records based on the provided export_filter using the
    existing BibsToSolr job and then grabs any attached items and
    holdings and exports them using the specified export processes.
    """
    Child = AttachedRecordExporter.Child

    class ItemChild(Child):
        def derive_records(self, parent_record):
            item_links = parent_record.bibrecorditemrecordlink_set.all()
            return [link.item_record for link in item_links]

    class HoldingChild(Child):
        def derive_records(self, parent_record):
            return [h for h in parent_record.holding_records.all()]

    children_config = (Child('BibsToSolr'), ItemChild('ItemsToSolr'),
                       HoldingChild('HoldingUpdate'))
    model = sierra_models.BibRecord
    max_rec_chunk = 100

    @property
    def prefetch_related(self):
        return self.main_child.prefetch_related + [
            'holding_records',
            'holding_records__bibrecord_set',
            'holding_records__bibrecord_set__record_metadata__varfield_set',
            'holding_records__resourcerecord_set',
            'holding_records__resourcerecord_set__record_metadata'
                '__varfield_set',
            'holding_records__resourcerecord_set__holding_records'
        ]
