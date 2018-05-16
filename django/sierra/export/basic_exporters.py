'''
Default base Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
'''
from __future__ import unicode_literals
import logging
import sys, traceback
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


def collapse_vals(vals):
    new_vals = {}
    for v in vals:
        new_vals = dict_merge(new_vals, v)
    return new_vals


class MetadataToSolrExporter(exporter.Exporter):
    '''
    Subclassable exporter subclass. Subclass this to create simple
    exporters for "metadata" that is in your III system--Locations,
    Itypes, Ptypes, Material Types, etc. You should just need to
    specify a model_name and (haystack) index.
    '''
    model_name = ''
    hs_conn = None
    index_class = None
    
    def __init__(self, *args, **kwargs):
        c_name = self.__class__.__name__
        self.hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS[c_name]
        super(MetadataToSolrExporter, self).__init__(*args, **kwargs)

    def get_records(self):
        return getattr(sierra_models, self.model_name).objects.all()

    def get_deletions(self):
        return None

    def export_records(self, records, vals={}):
        # Note that our export_records process doesn't even use the
        # records that are passed to it--it just sends an "update"
        # call to the indexer and the indexer grabs the records
        # straight from the model. We still define the get_records
        # method because this gives our export dispatcher task a 
        # record count which it logs on the export instance.
        log_label = self.__class__.__name__
        try:
            self.index_class().reindex(using=self.hs_conn, commit=False)
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            logger.info(traceback.extract_tb(tb))
            self.log('Error', e, log_label)
        return vals

    def final_callback(self, vals={}, status='success'):
        self.log('Info', 'Committing updates to Solr...')
        index = self.index_class()
        index.commit(using=self.hs_conn)


class LocationsToSolr(MetadataToSolrExporter):
    '''
    Defines process to load Locations into Solr.
    '''
    model_name = 'Location'
    index_class = indexes.LocationIndex


class ItypesToSolr(MetadataToSolrExporter):
    '''
    Defines process to load Itypes into Solr.
    '''
    model_name = 'ItypeProperty'
    index_class = indexes.ItypeIndex


class ItemStatusesToSolr(MetadataToSolrExporter):
    '''
    Defines process to load item statuses into Solr.
    '''
    model_name = 'ItemStatusProperty'
    index_class = indexes.ItemStatusIndex


class ItemsToSolr(exporter.Exporter):
    '''
    Defines processes that load item records into Solr.
    '''
    hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS['ItemsToSolr']
    model_name = 'ItemRecord'
    index_class = indexes.ItemIndex
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'i'
        }
    ]
    prefetch_related = [
        'record_metadata__varfield_set',
        'checkout_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__bib_record__record_metadata',
        'bibrecorditemrecordlink_set__bib_record__record_metadata'
            '__varfield_set',
        'bibrecorditemrecordlink_set__bib_record__bibrecordproperty_set'
    ]
    select_related = ['record_metadata', 'location', 'itype']

    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        index = self.index_class(queryset=records)
        try:
            index.update(using=self.hs_conn, commit=True)
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            logger.info(traceback.extract_tb(tb))
            self.log('Error', e, log_label)
        return vals

    def delete_records(self, records, vals={}):
        log_label = self.__class__.__name__
        index = self.index_class()
        for i in records:
            try:
                index.remove_object('base.itemrecord.{}'.format(str(i.id)),
                                    using=self.hs_conn, commit=True)
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.info(traceback.extract_tb(tb))
                self.log('Error', 'Record {}: {}'
                         ''.format(str(i), e), log_label)
        return vals

    def final_callback(self, vals={}, status='success'):
        self.log('Info', 'Committing updates to Solr...')
        index = self.index_class()
        index.commit(using=self.hs_conn)


class EResourcesToSolr(exporter.Exporter):
    '''
    Defines processes that load item records into Solr.
    '''
    hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS['EResourcesToSolr']
    model_name = 'ResourceRecord'
    index_class = indexes.ElectronicResourceIndex
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
        log_label = self.__class__.__name__
        index = self.index_class(queryset=records)
        try:
            index.update(using=self.hs_conn, commit=False)
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            logger.info(traceback.extract_tb(tb))
            self.log('Error', e, log_label)
        else:
            h_lists = vals.get('h_lists', {})
            h_lists.update(index.h_lists)
            vals['h_lists'] = h_lists
        return vals

    def delete_records(self, records, vals={}):
        log_label = self.__class__.__name__
        index = self.index_class()
        for i in records:
            try:
                index.remove_object('base.resourcerecord.{}'.format(str(i.id)),
                                    using=self.hs_conn, commit=False)
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.info(traceback.extract_tb(tb))
                self.log('Error', 'Record {}: {}'
                         ''.format(str(i), e), log_label)
        return vals

    def final_callback(self, vals={}, status='success'):
        if type(vals) is list:
            vals = collapse_vals(vals)

        self.log('Info', 'Committing updates to Solr and Redis...')

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

        index = self.index_class()
        index.commit(using=self.hs_conn)


class HoldingUpdate(exporter.Exporter):
    '''
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
    records by more than just their title)
    '''
    hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS['HoldingUpdate']
    model_name = 'HoldingRecord'
    index_class = indexes.ElectronicResourceIndex
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'c'
        }
    ]
    prefetch_related = [
        'bibrecord_set',
        'bibrecord_set__record_metadata__varfield_set',
        'resourcerecord_set',
        'resourcerecord_set__record_metadata__varfield_set',
        'resourcerecord_set__holding_records'
    ]

    def __init__(self, *args, **kwargs):
        super(HoldingUpdate, self).__init__(*args, **kwargs)
        er_et = export_models.ExportType.objects.get(pk='EResourcesToSolr')
        self.eresources_to_solr = er_et.get_exporter_class()
        self.max_rec_chunk = self.eresources_to_solr.max_rec_chunk

    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        eresources = set()
        er_mapping = {}
        # First we loop through the holding records and determine which
        # eresources need to be updated. er_mapping maps eresource rec
        # nums to lists of holdings rec nums to update.
        rev_handler = redisobjs.RedisObject('reverse_holdings_list', '0')
        reverse_holdings_list = rev_handler.get() or {}
        for h in records:
            h_rec_num = h.record_metadata.get_iii_recnum(True)
            old_er_rec_num = reverse_holdings_list.get(h_rec_num, None)
            try:
                er_rec_num = h.resourcerecord_set.all()[0]\
                    .record_metadata.get_iii_recnum(True)
            except IndexError:
                er_rec_num = None

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
                er_mapping[old_er_rec_num] = old_h_data

            if er_rec_num:
                holding_data = er_mapping.get(er_rec_num, [])
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
                holding_data.append(data)
                er_mapping[er_rec_num] = holding_data

        h_vals = vals.get('holdings', {})
        #self.log('Info', er_mapping)
        for er_rec_num, holdings in er_mapping.iteritems():
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
                eresources.add(e)

        vals['holdings'] = h_vals

        if eresources:
            eresources = list(eresources)
            er_vals = vals.get('eresources', {})
            er_vals.update(self.eresources_to_solr(self.instance.pk,
                self.export_filter, self.export_type, self.options)
                .export_records(eresources, er_vals))
            vals['eresources'] = er_vals

        return vals

    def delete_records(self, records, vals={}):
        return vals

    def final_callback(self, vals={}, status='success'):
        if type(vals) is list:
            vals = collapse_vals(vals)

        h_vals = vals.get('holdings', {})
        er_vals = vals.get('eresources', {})

        self.eresources_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(er_vals, status)

        # commit changes to Redis and commit deletions to Solr
        self.log('Info', 'Committing updates to Redis...')
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
        index = self.index_class()
        index.commit(using=self.hs_conn)
    

class BibsDownloadMarc(exporter.Exporter):
    '''
    Defines processes that convert Sierra bib records to MARC.
    '''
    max_rec_chunk = 1000
    parallel = False
    model_name = 'BibRecord'
    prefetch_related = [
        'record_metadata__varfield_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__item_record',
        'bibrecorditemrecordlink_set__item_record__record_metadata',
        'bibrecordproperty_set',
        'bibrecordproperty_set__material__materialpropertyname'
    ]
    select_related = ['record_metadata']
    
    def get_deletions(self):
        return None
        
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


class BibsToSolr(exporter.Exporter):
    '''
    Defines processes that export Sierra/MARC bibs out to Solr. Note
    that we instantiate a BibsDownloadMarc exporter first because we
    need to output a MARC file that will be indexed using Solrmarc.
    '''
    max_rec_chunk = 1000
    bibs_hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS['BibsToSolr:BIBS']
    marc_hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS['BibsToSolr:MARC']
    model_name = 'BibRecord'
    bibs_index_class = indexes.BibIndex
    marc_index_class = indexes.MarcIndex
    bib2marc_class = BibsDownloadMarc
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
        'bibrecordproperty_set__material__materialpropertyname'
    ]
    select_related = ['record_metadata']
    
    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        cmd = 'bash'
        index_script = settings.SOLRMARC_COMMAND
        config_file = settings.SOLRMARC_CONFIG_FILE
        filedir = settings.MEDIA_ROOT
        if filedir[-1] != '/':
            filedir = '{}/'.format(filedir)
        bib_converter = self.bib2marc_class(
            self.instance.pk, self.export_filter, self.export_type,
            self.options
        )
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
                     log_label)
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

            # if all went well, we now try to output JSON-MARC to the
            # MARC index
            index = self.marc_index_class(queryset=records)
            try:
                index.update(using=self.marc_hs_conn, commit=False)
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.info(traceback.extract_tb(tb))
                self.log('Error', e, log_label)

        # delete the file when we're done so we don't take up space
        os.remove(filepath)
        return vals

    def delete_records(self, records, vals={}):
        log_label = self.__class__.__name__
        bibs_index = self.bibs_index_class()
        marc_index = self.marc_index_class()
        for i in records:
            try:
                bibs_index.remove_object('base.bibrecord.{}'.format(str(i.id)),
                                    using=self.bibs_hs_conn, commit=False)
                marc_index.remove_object('base.bibrecord.{}'.format(str(i.id)),
                                    using=self.marc_hs_conn, commit=False)
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.info(traceback.extract_tb(tb))
                self.log('Error', 'Record {}: {}'
                         ''.format(str(i), e), log_label)
        return vals

    def final_callback(self, vals={}, status='success'):
        log_label = self.__class__.__name__
        self.log('Info', 'Committing updates to Solr...', log_label)
        bibs_index = self.bibs_index_class()
        marc_index = self.marc_index_class()
        bibs_index.commit(using=self.bibs_hs_conn)
        marc_index.commit(using=self.marc_hs_conn)


class ItemsBibsToSolr(exporter.Exporter):
    '''
    Exports item records based on the provided export_filter using the
    existing ItemsToSolr job and then grabs the items' parent bibs and
    exports them using BibsToSolr.
    
    If using this in production, make sure to use it in conjunction
    with a bib loader, like BibsToSolr or BibsItemsToSolr. That way
    bib records that need to be deleted will actually get deleted.
    '''
    model_name = 'ItemRecord'
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'i'
        }
    ]
    
    def __init__(self, *args, **kwargs):
        super(ItemsBibsToSolr, self).__init__(*args, **kwargs)
        item_et = export_models.ExportType.objects.get(pk='ItemsToSolr')
        bib_et = export_models.ExportType.objects.get(pk='BibsToSolr')
        items_to_solr = item_et.get_exporter_class()
        bibs_to_solr = bib_et.get_exporter_class()
        self.prefetch_related = items_to_solr.prefetch_related
        self.prefetch_related.extend([
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record',
            'bibrecorditemrecordlink_set__bib_record'
                '__bibrecorditemrecordlink_set__item_record__record_metadata',
        ])
        self.select_related = items_to_solr.select_related
        self.items_to_solr = items_to_solr
        self.bibs_to_solr = bibs_to_solr
    
    def export_records(self, records, vals={}):
        bibs = []
        for r in records:
            bibs.append(r.bibrecorditemrecordlink_set.all()[0].bib_record)
        self.items_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).export_records(records)
        self.bibs_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).export_records(bibs)
        return vals

    def delete_records(self, records, vals={}):
        self.items_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).delete_records(records)
        return vals

    def final_callback(self, vals={}, status='success'):
        self.items_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(vals, status)
        self.bibs_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(vals, status)


class BibsAndAttachedToSolr(exporter.Exporter):
    '''
    Exports bib records based on the provided export_filter using the
    existing BibsToSolr job and then grabs any attached items and
    holdings and exports them using the specified export processes.
    '''
    model_name = 'BibRecord'
    deletion_filter = [
        {
            'deletion_date_gmt__isnull': False,
            'record_type__code': 'b'
        }
    ]
    select_related = BibsToSolr.select_related
    max_rec_chunk = 100

    def __init__(self, *args, **kwargs):
        super(BibsAndAttachedToSolr, self).__init__(*args, **kwargs)
        item_et = export_models.ExportType.objects.get(pk='ItemsToSolr')
        bib_et = export_models.ExportType.objects.get(pk='BibsToSolr')
        holding_et = export_models.ExportType.objects.get(pk='HoldingUpdate')
        items_to_solr = item_et.get_exporter_class()
        bibs_to_solr = bib_et.get_exporter_class()
        holdings_to_solr = holding_et.get_exporter_class()
        self.prefetch_related = bibs_to_solr.prefetch_related
        self.prefetch_related.extend([
            'holding_records',
            'holding_records__bibrecord_set',
            'holding_records__bibrecord_set__record_metadata__varfield_set',
            'holding_records__resourcerecord_set',
            'holding_records__resourcerecord_set__record_metadata'\
                '__varfield_set',
            'holding_records__resourcerecord_set__holding_records'
        ])
        self.select_related = bibs_to_solr.select_related
        self.items_to_solr = items_to_solr
        self.bibs_to_solr = bibs_to_solr
        self.holdings_to_solr = holdings_to_solr

    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        items, holdings = [], []

        for r in records:
            items.extend([bl.item_record 
                         for bl in r.bibrecorditemrecordlink_set.all()])
            holdings.extend([h for h in r.holding_records.all()])

        i_vals = vals.get('items', {})
        h_vals = vals.get('holdings', {})
        b_vals = vals.get('bibs', {})

        i_vals.update(self.items_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).export_records(items, i_vals))
        h_vals.update(self.holdings_to_solr(self.instance.pk,
            self.export_filter, self.export_type, self.options)
            .export_records(holdings, h_vals))
        b_vals.update(self.bibs_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).export_records(records, b_vals))

        vals['items'] = i_vals
        vals['holdings'] = h_vals
        vals['bibs'] = b_vals

        return vals

    def delete_records(self, records, vals={}):
        self.bibs_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).delete_records(records)
        return vals

    def final_callback(self, vals={}, status='success'):
        if type(vals) is list:
            vals = collapse_vals(vals)

        i_vals = vals.get('items', {})
        h_vals = vals.get('holdings', {})
        b_vals = vals.get('bibs', {})

        self.items_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(i_vals, status)
        self.holdings_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(h_vals, status)
        self.bibs_to_solr(self.instance.pk, self.export_filter,
            self.export_type, self.options).final_callback(b_vals, status)
