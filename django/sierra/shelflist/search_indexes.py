"""
This contains additional specifications for the haystack Solr indexes
for the shelflist app. 
"""
from haystack import indexes

from base import search_indexes
from utils import solr


class ShelflistItemIndex(search_indexes.ItemIndex):
    """
    Adds storage for user-entered fields, which don't exist in Sierra.
    Whenever Sierra item records are reindexed, we need to make sure
    the values for the user-entered fields that are in the index don't
    get overwritten with blank values.
    """
    shelf_status = indexes.FacetCharField(null=True)
    inventory_notes = indexes.MultiValueField(null=True)
    flags = indexes.MultiValueField(null=True)
    inventory_date = indexes.DateTimeField(null=True)
    user_data_fields = ('shelf_status', 'inventory_notes', 'flags',
                        'inventory_date')

    def __init__(self, *args, **kwargs):
        super(ShelflistItemIndex, self).__init__(*args, **kwargs)
        self.solr_conn = solr.connect()

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
            item = solr.Queryset(conn=self.solr_conn).get_one(id=obj.id)
            if item:
                for field in self.user_data_fields:
                    self.prepared_data[field] = getattr(item, field, None)
        return self.prepared_data
