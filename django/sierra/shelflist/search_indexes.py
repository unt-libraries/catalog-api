"""
This contains additional specifications for the haystack Solr indexes
for the shelflist app.
"""
from __future__ import absolute_import

from base import search_indexes
from haystack import indexes
from utils import solr


class ShelflistItemIndex(search_indexes.ItemIndex):
    """
    Custom haystack SearchIndex object that is based on the base
    search_indexes.ItemIndex, but does a few additional things.

    It accommodates user-entered fields that don't exist in Sierra.
    Whenever Sierra item records are reindexed, we need to make sure
    any values for user-entered fields that are in the index already
    don't get overwritten with blank values. (Class attr
    `user_data_fields` lists which fields those are.)

    New features communicate which locations need their shelflist item
    manifests updated due to index changes. A `location_set` property
    tracks all location codes seen during an update. The
    `get_location_set_from_recs` method queries Solr to return the
    unique set of location codes (in Solr) matching records in a Django
    queryset (RecordMetadata or ItemRecord instances), for deletions.

    The `get_location_manifest` method pulls a list of item IDs from
    Solr, sorted in shelflist order, for a particular location, to help
    build shelflist item manifests.
    """
    shelf_status = indexes.FacetCharField(null=True)
    inventory_notes = indexes.MultiValueField(null=True)
    flags = indexes.MultiValueField(null=True)
    inventory_date = indexes.DateTimeField(null=True)
    user_data_fields = ('shelf_status', 'inventory_notes', 'flags',
                        'inventory_date')
    solr_shelflist_sort_criteria = ['call_number_type', 'call_number_sort',
                                    'volume_sort', 'copy_number']

    def __init__(self, *args, **kwargs):
        super(ShelflistItemIndex, self).__init__(*args, **kwargs)
        self.location_set = set()

    def update(self, using=None, commit=True, queryset=None):
        self.location_set = set()
        super(ShelflistItemIndex, self).update(using, commit, queryset)

    def prepare_location_code(self, obj):
        code = super(ShelflistItemIndex, self).prepare_location_code(obj)
        if code:
            self.location_set.add(code)
        return code

    def has_any_user_data(self, obj):
        """
        Returns True if the provided obj has any fields containing
        user-supplied data. These fields are defined in the
        user_data_fields class attribute.
        """
        return any(hasattr(obj, field) for field in self.user_data_fields)

    def prepare(self, obj):
        """
        Prepares data on the object for indexing. Here, if the object
        doesn't have any user data fields defined, then we know it's
        coming from Sierra and that means we need to query the Solr
        index to add any existing values for these fields to the object
        before it's re-indexed.
        """
        self.prepared_data = super(ShelflistItemIndex, self).prepare(obj)
        if not self.has_any_user_data(obj):
            conn = self.get_backend().conn
            obj_qid = self.get_qualified_id(obj)
            qid_field = self.reserved_fields['haystack_id']
            item = solr.Queryset(conn=conn).get_one(**{qid_field: obj_qid})
            if item:
                for field in self.user_data_fields:
                    self.prepared_data[field] = getattr(item, field, None)
        return self.prepared_data

    def get_location_set_from_recs(self, records, using=None):
        """
        Query the underlying Solr index to pull the set of location
        codes represented by the given `records` (ItemRecord queryset,
        or RecordMetadata queryset of items).
        """
        rec_qids = [self.get_qualified_id(r) for r in records]
        qid_field = self.reserved_fields['haystack_id']
        conn = self.get_backend(using=using).conn
        lcode_qs = solr.Queryset(conn=conn).filter(**{
            f'{qid_field}__in': rec_qids
        })
        facet_params = {'rows': 0, 'facet': 'true',
                        'facet.field': 'location_code', 'facet.mincount': 1}
        facets = lcode_qs.set_raw_params(facet_params).full_response.facets
        try:
            return set(facets['facet_fields']['location_code'][0::2])
        except KeyError:
            return set()

    def get_location_manifest(self, location_code, using=None):
        """
        Query the underlying Solr index to pull a list of all item ids
        for a given location code, pre-sorted based on the
        `solr_shelflist_sort_criteria` class attribute. Returns the
        list of ids, in order.
        """
        conn = self.get_backend(using=using).conn
        man_qs = solr.Queryset(conn=conn).filter(type=self.type_name,
                                                 location_code=location_code)
        man_qs = man_qs.order_by(*self.solr_shelflist_sort_criteria).only('id')
        results = man_qs.set_raw_params({'rows': len(man_qs)}).full_response
        return [i['id'] for i in results]
