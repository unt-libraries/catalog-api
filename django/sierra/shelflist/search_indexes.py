"""
This contains additional specifications for the haystack Solr indexes
for the shelflist app.
"""
from haystack import indexes

from base import search_indexes
from utils import solr


class ShelflistItemIndex(search_indexes.ItemIndex):
    """
    Custom haystack SearchIndex object that is based on the base
    search_indexes.ItemIndex, but does three additional things.

    One, it adds storage for user-entered fields that don't exist in
    Sierra. Whenever Sierra item records are reindexed, we need to make
    sure the values for the user-entered fields that are in the index
    don't get overwritten with blank values. (Class attr
    `user_data_fields` list which fields those are.)

    Two, it adds a `location_set` property to an object, which lets us
    track the set of location codes seen when modifying records via an
    index update.

    Three, it adds methods for updating shelflist item manifests in
    Redis for particular locations.
    """
    shelf_status = indexes.FacetCharField(null=True)
    inventory_notes = indexes.MultiValueField(null=True)
    flags = indexes.MultiValueField(null=True)
    inventory_date = indexes.DateTimeField(null=True)
    user_data_fields = ('shelf_status', 'inventory_notes', 'flags',
                        'inventory_date')
    solr_shelflist_sort_criteria = ['call_number_type asc',
                                    'call_number_sort asc',
                                    'volume_sort asc',
                                    'copy_number asc']

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
            item = solr.Queryset(conn=conn).get_one(id=obj.id)
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
        record_pks = [r['pk'] for r in records.values('pk')]
        conn = self.get_backend(using=using).conn
        lcode_qs = solr.Queryset(conn=conn).filter(id__in=record_pks)
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
        fq = ['type:{}'.format(self.type_name),
              'location_code:{}'.format(location_code)]
        params = { 'q': '*:*', 'fq': fq, 'fl': 'id',
                   'sort': self.solr_shelflist_sort_criteria }
        hits = conn.search(rows=0, **params).hits
        return [i['id'] for i in conn.search(rows=hits, **params)]
