"""
Exporters module for catalog-api `blacklight` app, alpha-solrmarc.
"""

from __future__ import unicode_literals
import logging
from collections import OrderedDict

from export import models as em
from export.exporter import Exporter
from export.basic_exporters import collapse_vals
from .exporters import BaseSolrMarcBibsToSolr, BaseBibsDownloadMarc


class BibsToAlphaSolrmarc(BaseSolrMarcBibsToSolr):
    
    bib2marc_class = BaseBibsDownloadMarc
    cores = {'bibs': 'alpha-solrmarc'}

    def get_record_id(self, record):
        return record.get_iii_recnum(False)


class BibsToAlphaSmAndAttachedToSolr(Exporter):

    model_name = 'BibRecord'

    def __init__(self, *args, **kwargs):
        super(BibsToAlphaSmAndAttachedToSolr, self).__init__(*args, **kwargs)
        attached_et = em.ExportType.objects.get(pk='BibsAndAttachedToSolr')
        to_alpha_sm_et = em.ExportType.objects.get(pk='BibsToAlphaSolrmarc')
        bibs_and_attached_to_solr = attached_et.get_exporter_class()
        bibs_to_alpha_solrmarc = to_alpha_sm_et.get_exporter_class()
        children = (bibs_and_attached_to_solr, bibs_to_alpha_solrmarc)
        prefetch = self.combine_table_lists('prefetch_related', children)
        select = self.combine_table_lists('select_related', children)
        self.deletion_filter = bibs_and_attached_to_solr.deletion_filter
        self.max_rec_chunk = bibs_and_attached_to_solr.max_rec_chunk
        self.prefetch_related = prefetch
        self.select_related = select
        self.bibs_and_attached_to_solr = bibs_and_attached_to_solr
        self.bibs_to_alpha_solrmarc = bibs_to_alpha_solrmarc

    def combine_table_lists(self, tlist_attr, exporters):
        table_lists = [getattr(exp, tlist_attr) for exp in exporters]
        combined_dupes = sorted([t for tlist in table_lists for t in tlist])
        return OrderedDict.fromkeys(combined_dupes).keys()

    def _do_task(self, task, vals, *args, **kwargs):
        for et in ('bibs_and_attached_to_solr', 'bibs_to_alpha_solrmarc'):
            instance = getattr(self, et)(self.instance.pk, self.export_filter,
                                         self.export_type, self.options)
            kwargs['vals'] = vals.get(et, {})
            new_vals = getattr(instance, task)(*args, **kwargs) or {}
            kwargs['vals'].update(new_vals)
            vals[et] = kwargs['vals']
        return vals

    def export_records(self, records, vals={}):
        return self._do_task('export_records', vals, records)

    def delete_records(self, records, vals={}):
        return self._do_task('delete_records', vals, records)

    def final_callback(self, vals={}, status='success'):
        if type(vals) is list:
            vals = collapse_vals(vals)

        self._do_task('final_callback', vals, status=status)
