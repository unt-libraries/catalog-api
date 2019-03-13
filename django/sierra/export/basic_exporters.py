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
from . import exporter
from .sierra2marc import S2MarcError, S2MarcBatch
from utils import helpers, redisobjs, solr, dict_merge

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


SOLR_CONNS = settings.EXPORTER_HAYSTACK_CONNECTIONS


def collapse_vals(vals):
    new_vals = {}
    for v in vals:
        new_vals = dict_merge(new_vals, v)
    return new_vals


class MetadataToSolrExporter(exporter.ToSolrExporter):
    """
    Subclassable exporter subclass. Subclass this to create simple
    exporters for "metadata" that is in your III system--Locations,
    Itypes, Ptypes, Material Types, etc.
    """

    index_settings = (
        ('first_index_name', {
            'class': None,  # haystack.SearchIndex
            'core': None    # Solr core string, e.g. 'bibdata'
        }),
        ('second_index_name', {
            'class': None,  # haystack.SearchIndex
            'core': None    # Solr core string, e.g. 'bibdata'
        }),
    )

    def get_records(self):
        return self.model.objects.all()

    def get_deletions(self):
        return None

    def update_index(self, index_name, records):
        self.indexes[index_name].reindex(commit=False, queryset=records)


class LocationsToSolr(MetadataToSolrExporter):
    """
    Defines process to load Locations into Solr.
    """
    model = sierra_models.Location
    index_settings = (
        ('Locations', {
            'class': indexes.LocationIndex,
            'core': SOLR_CONNS['LocationsToSolr']
        }),
    )


class ItypesToSolr(MetadataToSolrExporter):
    """
    Defines process to load Itypes into Solr.
    """
    model = sierra_models.ItypeProperty
    index_settings = (
        ('Itypes', {
            'class': indexes.ItypeIndex,
            'core': SOLR_CONNS['ItypesToSolr']
        }),
    )


class ItemStatusesToSolr(MetadataToSolrExporter):
    """
    Defines process to load item statuses into Solr.
    """
    model = sierra_models.ItemStatusProperty
    index_settings = (
        ('ItemStatuses', {
            'class': indexes.ItemStatusIndex,
            'core': SOLR_CONNS['ItemStatusesToSolr']
        }),
    )


class ItemsToSolr(exporter.ToSolrExporter):
    """
    Defines processes that load item records into Solr.
    """
    model = sierra_models.ItemRecord
    index_settings = (
        ('Items', {
            'class': indexes.ItemIndex,
            'core': SOLR_CONNS['ItemsToSolr']
        }),
    )
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


class EResourcesToSolr(exporter.ToSolrExporter):
    """
    Defines processes that load resource records into Solr.
    """
    model = sierra_models.ResourceRecord
    index_settings = (
        ('EResources', {
            'class': indexes.ElectronicResourceIndex,
            'core': SOLR_CONNS['EResourcesToSolr']
        }),
    )
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

    def export_records(self, records, vals={}):
        try:
            self.indexes['EResources'].update(commit=False, queryset=records)
        except Exception as e:
            self.log_error(e)
        else:
            h_lists = vals.get('h_lists', {})
            h_lists.update(self.indexes['EResources'].h_lists)
            vals['h_lists'] = h_lists
        return vals

    def commit_to_redis(self, vals):
        self.log('Info', 'Committing EResource updates to Redis...')
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_holdings_list = rev_handler.get() or {}
        for er_rec_num, h_list in vals.get('h_lists', {}).iteritems():
            er_handler = redisobjs.RedisObject('eresource_holdings_list',
                                               er_rec_num)
            er_handler.set(h_list)

            for h_rec_num in h_list:
                reverse_holdings_list[h_rec_num] = er_rec_num
            
            #self.log('Info', 'Number of holdings in reverse_holdings_list: {}'
            #                  .format(len(reverse_holdings_list.keys())))

        rev_handler.set(reverse_holdings_list)

    def final_callback(self, vals={}, status='success'):
        if isinstance(vals, (list, tuple)):
            vals = collapse_vals(vals)
        self.commit_to_redis(vals)
        self.commit_indexes()


class HoldingUpdate(exporter.CompoundMixin, exporter.Exporter):
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
    model = sierra_models.HoldingRecord
    exporter_names = ('EResourcesToSolr',)
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
        er_to_solr = self.exporter_classes['EResourcesToSolr']
        self.max_rec_chunk = er_to_solr.max_rec_chunk

    def export_records(self, records, vals={}):
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

        h_vals = vals.get('holdings', {})
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

        vals['holdings'] = h_vals

        if eresources:
            eresources = list(eresources)
            er_vals = vals.get('eresources', {})
            er_to_solr = self.exporters['EResourcesToSolr']
            er_vals.update(er_to_solr.export_records(eresources, er_vals))
            vals['eresources'] = er_vals

        return vals

    def commit_to_redis(self, vals):
        self.log('Info', 'Committing Holdings updates to Redis...')
        h_vals = vals.get('holdings', {})
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_h_list = rev_handler.get()
        for er_rec_num, lists in h_vals.iteritems():
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

    def final_callback(self, vals={}, status='success'):
        if isinstance(vals, (list, tuple)):
            vals = collapse_vals(vals)

        er_vals = vals.get('eresources', {})
        self.exporters['EResourcesToSolr'].final_callback(er_vals, status)
        self.commit_to_redis(vals)


class BibsDownloadMarc(exporter.Exporter):
    """
    Defines processes that convert Sierra bib records to MARC.
    """
    max_rec_chunk = 1000
    parallel = False
    model_name = 'BibRecord'
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname_set'
    ]
    select_related = ['record_metadata']
        
    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        batch = S2MarcBatch(records)
        out_recs = batch.to_marc()
        try:
            if 'marcfile' in vals:
                marcfile = batch.to_file(out_recs, vals['marcfile'])
            else:
                vals['marcfile'] = batch.to_file(out_recs, append=False)
        except IOError as e:
            self.log('Error', 'Error writing to output file: {}'.format(e), 
                     log_label)
        else:
            for e in batch.errors:
                self.log('Warning', 'Record {}: {}'.format(e.id, e.msg),
                         log_label)
            if 'success_count' in vals:
                vals['success_count'] += batch.success_count
            else:
                vals['success_count'] = batch.success_count
        return vals

    def final_callback(self, vals={}, status='success'):
        log_label = self.__class__.__name__
        if 'success_count' in vals:
            self.log('Info', '{} records successfully '
                    'processed.'.format(vals['success_count']), log_label)
        if 'marcfile' in vals:
            self.log('Info', '<a href="{}{}">Download File</a> '
                    '(Link expires after 24 hrs.)'
                    ''.format(settings.MEDIA_URL, vals['marcfile']), log_label)


class BibsToSolr(exporter.CompoundMixin, exporter.ToSolrExporter):
    """
    Defines processes that export Sierra/MARC bibs out to Solr. Note
    that we instantiate a BibsDownloadMarc exporter first because we
    need to output a MARC file that will be indexed using Solrmarc.
    """
    model = sierra_models.BibRecord
    index_settings = (
        ('Bibs', {
            'class': indexes.BibIndex,
            'core': SOLR_CONNS['BibsToSolr:BIBS']
        }),
        ('MARC', {
            'class': indexes.MarcIndex,
            'core': SOLR_CONNS['BibsToSolr:MARC']
        })
    )
    exporter_names = ('BibsDownloadMarc',)
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
    
    def export_records(self, records, vals={}):
        cmd = 'bash'
        index_script = settings.SOLRMARC_COMMAND
        config_file = settings.SOLRMARC_CONFIG_FILE
        filedir = settings.MEDIA_ROOT
        if filedir[-1] != '/':
            filedir = '{}/'.format(filedir)
        bib_converter = self.exporters['BibsDownloadMarc']
        ret_vals = bib_converter.export_records(records, vals={})
        filename = ret_vals['marcfile']
        filepath = '{}{}'.format(filedir, filename)
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
            self.log('Error', 'Solrmarc process did not run successfully.',
                     self.log_label)
        else:
            error_lines = output.split("\n")
            del(error_lines[-1])
            if error_lines:
                for line in error_lines:
                    line = re.sub(r'^\s+', '', line)
                    if re.match(r'^WARN', line):
                        self.log('Warning', line, log_label)
                    elif re.match(r'^ERROR', line):
                        self.log('Error', line, log_label)

            # if all went well, we now try to index the MARC record
            try:
                self.update_index('MARC', records)
            except Exception as e:
                self.log_error(e)

        # delete the file when we're done so we don't take up space
        os.remove(filepath)
        return vals


class AttachedRecordExporter(exporter.CompoundMixin, exporter.Exporter):
    """
    Base class for creating exporters that export a main set of records
    plus one or more sets of attached records.
    """
    exporter_names = tuple()

    @property
    def main_name(self):
        return self.exporter_classes.keys()[0]

    @property
    def attached_names(self):
        return self.exporter_classes.keys()[1:]

    @property
    def prefetch_related(self):
        return self.exporter_classes[self.main_name].prefetch_related

    @property
    def select_related(self):
        return self.exporter_classes[self.main_name].select_related

    @property
    def deletion_filter(self):
        return self.exporter_classes[self.main_name].deletion_filter

    def get_attached_records(self, record):
        pass

    def build_record_sets(self, records):
        record_sets = {self.main_name: records}
        for record in records:
            attached = self.get_attached_records(record)
            for key in self.attached_names:
                record_sets[key] = record_sets.get(key, [])
                record_sets[key].extend(attached[key])
        return record_sets

    def export_records(self, records, vals={}):
        rsets = self.build_record_sets(records)
        for key, exporter in self.exporters.items():
            rset = list(set(rsets[key]))
            vals[key] = vals.get(key, {})
            vals[key].update(exporter.export_records(rset, vals[key]))
        return vals

    def delete_records(self, records, vals={}):
        return self.exporters[self.main_name].delete_records(records, vals)

    def final_callback(self, vals={}, status='success'):
        if type(vals) is list:
            vals = collapse_vals(vals)

        for key, exporter in self.exporters.items():
            exporter.final_callback(vals.get(key, {}), status)


class ItemsBibsToSolr(AttachedRecordExporter):
    """
    Exports item records based on the provided export_filter using the
    existing ItemsToSolr job and then grabs the items' parent bibs and
    exports them using BibsToSolr.
    
    If using this in production, make sure to use it in conjunction
    with a bib loader, like BibsToSolr or BibsItemsToSolr. That way
    bib records that need to be deleted will actually get deleted.
    """
    model = sierra_models.ItemRecord
    exporter_names = ('ItemsToSolr', 'BibsToSolr')

    @property
    def prefetch_related(self):
        return self.exporter_classes[self.main_name].prefetch_related + [
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record__record_metadata'
        ]

    def get_attached_records(self, r):
        return {
            'BibsToSolr': [
                bl.bib_record for bl in r.bibrecorditemrecordlink_set.all()
            ]
        }


class BibsAndAttachedToSolr(AttachedRecordExporter):
    """
    Exports bib records based on the provided export_filter using the
    existing BibsToSolr job and then grabs any attached items and
    holdings and exports them using the specified export processes.
    """
    model = sierra_models.BibRecord
    exporter_names = ('BibsToSolr', 'ItemsToSolr', 'HoldingUpdate')
    max_rec_chunk = 100

    @property
    def prefetch_related(self):
        return self.exporter_classes[self.main_name].prefetch_related + [
            'holding_records',
            'holding_records__bibrecord_set',
            'holding_records__bibrecord_set__record_metadata__varfield_set',
            'holding_records__resourcerecord_set',
            'holding_records__resourcerecord_set__record_metadata'
                '__varfield_set',
            'holding_records__resourcerecord_set__holding_records'
        ]

    def get_attached_records(self, r):
        return {
            'ItemsToSolr': [
                bl.item_record for bl in r.bibrecorditemrecordlink_set.all()
            ],
            'HoldingUpdate': [
                h for h in r.holding_records.all()
            ]
        }
