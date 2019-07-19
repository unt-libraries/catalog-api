"""
Exporters module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
from export.exporter import Exporter, CompoundMixin, ToSolrExporter
from utils import solr
from export.tasks import SolrKeyRangeBundler


class SameRecSetMultiExporter(CompoundMixin, Exporter):
    """
    A type of compound exporter that runs multiple export jobs against
    the same recordset. Use the `model` class attr to define what model
    the recordset comes from. The default Exporter.get_records is used
    (as usual) to get records, but `prefetch_` and `select_related`
    attrs are combined from the children exporters. Deletions are a
    little tricky; generally it's assumed you'd have the same deletion
    filters for each child exporter, in which case one set of deletions
    is fetched (via the default Exporter.get_deletions method). If not,
    then a batch deletion recordset is put together (like for a
    BatchExporter) so that the correct records are deleted.
    """
    Child = CompoundMixin.Child
    children_config = tuple()

    @property
    def select_related(self):
        try:
            self._select_related = self._select_related
        except AttributeError:
            rel = 'select_related'
            self._select_related = self.combine_rels_from_children(rel)
        return self._select_related

    @property
    def prefetch_related(self):
        try:
            self._prefetch_related = self._prefetch_related
        except AttributeError:
            rel = 'prefetch_related'
            self._prefetch_related = self.combine_rels_from_children(rel)
        return self._prefetch_related

    @property
    def deletion_filter(self):
        return self.children.values()[0].deletion_filter

    def get_deletions(self):
        """
        If this deletion filter and the deletion filter for all
        children are the same, then get one set of deletions. Otherwise
        get a separate set for each child (like a BatchExporter).
        """
        children = self.children.values()
        if all((c.deletion_filter == self.deletion_filter for c in children)):
            return super(SameRecSetMultiExporter, self).get_deletions()
        return self.get_records_from_children(deletions=True)

    def export_records(self, records):
        return self.do_op_on_children('export_records', records)

    def delete_records(self, records):
        return self.do_op_on_children('delete_records', records)

    def compile_vals(self, results):
        return self.compile_vals_from_children(results)

    def final_callback(self, vals=None, status='success'):
        self.do_final_callback_on_children(vals, status)


class FromSolrMixin(object):
    """
    This is a mixin to use with Exporter classes that implements a
    `get_filtered_queryset` method for grabbing records from a Solr
    core or index instead of via the Django ORM.
    """
    parallel = False
    model = None
    source_solr_conn = None
    deletion_filter = None
    solr_id_field = 'id'
    source_fields = tuple()

    @property
    def bundler(self):
        return SolrKeyRangeBundler(self.solr_id_field)

    @staticmethod
    def filter_by(solr_qs, export_filter, options=None):
        """
        For now we aren't going to need to actually filter, so we just
        return the queryset.
        """
        return solr_qs

    def get_filtered_queryset(self, export_filter, filter_options,
                              other_filters=None):
        """
        Utility method for fetching a filtered queryset based on the
        provided args and kwargs. Returns a utils.solr.Queryset, which
        can be used interchangeably with ORM QuerySets for many common
        filtering operations.
        """
        qs = solr.Queryset(using=self.source_solr_conn)
        qs = self.filter_by(qs, export_filter, options=filter_options)
        if other_filters is not None:
            qs = qs.filter(**other_filters)
        if self.source_fields:
            fl = set((self.solr_id_field, '_version_') + self.source_fields)
            qs = qs.set_raw_params({'fl': list(fl)})
        return qs

    def get_records(self, prefetch=True):
        options = self.options.copy()
        options['is_deletion'] = False
        return self.get_filtered_queryset(self.export_filter, options)

    def get_deletions(self):
        """
        For now we don't need to do deletions, so just return None.
        """
        return None
