'''
Default batch Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
'''
from __future__ import unicode_literals
import logging
import re

from django.conf import settings

from . import exporter
from . import models

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class AllMetadataToSolr(exporter.Exporter):
    '''
    Loads ALL metadata-type data into Solr, as defined by the
    EXPORTER_METADATA_TYPE_REGISTRY setting in your Django settings.
    '''
    hs_conn = None
    index_class = None
    
    def __init__(self, *args, **kwargs):
        c_name = self.__class__.__name__
        self.hs_conn = settings.EXPORTER_HAYSTACK_CONNECTIONS[c_name]
        self.index_class = models.ExportType.objects.get(
            pk=settings.EXPORTER_METADATA_TYPE_REGISTRY[0])\
                .get_exporter_class().index_class
        super(AllMetadataToSolr, self).__init__(*args, **kwargs)

    def get_records(self):
        records = []
        for exporter_name in settings.EXPORTER_METADATA_TYPE_REGISTRY:
            export_type = models.ExportType.objects.get(pk=exporter_name)
            exporter = export_type.get_exporter_class()(self.instance.pk,
                self.export_filter, self.export_type, self.options)
            records.extend(exporter.get_records())
        return records
    
    def export_records(self, records, vals={}):
        for exporter_name in settings.EXPORTER_METADATA_TYPE_REGISTRY:
            export_type = models.ExportType.objects.get(pk=exporter_name)
            exporter = export_type.get_exporter_class()(self.instance.pk,
                self.export_filter, self.export_type, self.options)
            exporter.export_records(records)
        return vals

    def final_callback(self, vals={}, status='success'):
        self.log('Info', 'Committing updates to Solr...')
        index = self.index_class()
        index.commit(using=self.hs_conn)


class AllToSolr(exporter.Exporter):
    '''
    Uses RecordMetadata to load ALL major III record types into Solr.
    Set up the EXPORTER_ALL_TYPE_REGISTRY setting in your Django
    settings to register what export jobs correspond with which record
    types. Only registered record types will be loaded.
    '''
    record_filter = []
    select_related = [
        'record_type'
    ]
    model_name = 'RecordMetadata'

    def __init__(self, *args, **kwargs):
        '''
        We want prefetch_related, record_filter, and deletion_filter 
        for this to be conglomerates of each of those attributes
        in whatever processes are registered, so here is where we set
        that up. 
        
        Note that since the focal model for AllToSolr is 
        RecordMetadata, we need to ensure that we add the needed
        xrecord_set__ prefixes and remove any references to this model,
        e.g. record_metadata.
        '''
        super(AllToSolr, self).__init__(*args, **kwargs)
        def model_set(p_class, fieldname='', add_set=True):
            if fieldname == 'record_metadata':
                ret_val = None
            else:
                find_rm = re.search(r'^record_metadata__(.*)', fieldname)
                if find_rm:
                    ret_val = find_rm.groups()[0]
                else:
                    ret_val = '{}{}'.format(p_class.model_name.lower(),
                            '_set' if add_set else '')
                    if fieldname:
                        ret_val = '{}__{}'.format(ret_val, fieldname)
            return ret_val

        for rt in settings.EXPORTER_ALL_TYPE_REGISTRY:
            for p_name in settings.EXPORTER_ALL_TYPE_REGISTRY[rt]:
                ex_type = models.ExportType.objects.get(pk=p_name)
                p_class = ex_type.get_exporter_class()
                self.prefetch_related.append(model_set(p_class))
                for pr in p_class.prefetch_related:
                    field = model_set(p_class, pr)
                    if field and field not in self.prefetch_related:
                        self.prefetch_related.append(field)
                for sr in p_class.select_related:
                    # here prefetch_related is not a typo. Since these
                    # are attached to model_sets, each is now an M2One
                    # relationship instead of a One2M.
                    field = model_set(p_class, sr)
                    if field and field not in self.prefetch_related:
                        self.prefetch_related.append(field)
                for rf in p_class.record_filter:
                    self.record_filter.append(
                        {model_set(p_class, key, False): rf[key] for key in rf
                            if model_set(p_class, key, False)}
                    )
                    self.record_filter[-1]['record_type__code'] = rt
                    self.record_filter[-1]['deletion_date_gmt__isnull'] = True
                if not p_class.record_filter:
                    self.record_filter.append({'record_type__code': rt,
                                              'deletion_date_gmt__isnull': True
                                              })
                for df in p_class.deletion_filter:
                    # deletion_filters are already from the POV of
                    # RecordMetadata, so we don't need to do anything
                    # fancy with them before appending them.
                    self.deletion_filter.append(df)

    def dispatch_it(self, records, process_type='export_records'):
        div_records = {}
        for r in records:
            rt = r.record_type.code
            if rt in settings.EXPORTER_ALL_TYPE_REGISTRY:
                if rt not in div_records:
                    div_records[rt] = []
                full_record = r.get_full_record()
                if full_record is not None:
                    # This has a full record and is not a deletion
                    div_records[rt].append(full_record)
                elif r.deletion_date_gmt is not None:
                    # This is a deletion
                    div_records[rt].append(r)
                else:
                    # Something's wrong if there's no full record and
                    # this isn't a deletion
                    self.log('Warning', 'Record {} had no full record '
                            'attached and was not a deletion.'
                            ''.format(r.get_iii_recnum()))
        for rt in div_records:
            for p_name in settings.EXPORTER_ALL_TYPE_REGISTRY[rt]:
                try:
                    ex_type = models.ExportType.objects.get(pk=p_name)
                    p_class = ex_type.get_exporter_class()
                except models.ExportType.DoesNotExist:
                    self.log('Error', 'Could not run {} {} on {}-type'
                             'records: process is not defined.'
                             ''.format(process_type, p_name, rt))
                else:
                    process = p_class(self.instance.pk,
                        self.export_filter, self.export_type, self.options)
                    getattr(process, process_type)(div_records[rt])
    
    def export_records(self, records, vals={}):
        self.dispatch_it(records, process_type='export_records')
        return vals

    def delete_records(self, records, vals={}):
        self.dispatch_it(records, process_type='delete_records')
        return vals