'''
This contains additional specifications for the haystack Solr indexes
for the shelflist app. 
'''
from haystack import indexes

from base import search_indexes
from utils import solr


class ShelflistItemIndex(search_indexes.ItemIndex):
    '''
    Adds storage for shelflistItem-specific fields, which don't come
    from Sierra. Whenever these are reindexed, we need to make sure
    the values in the index aren't removed. Also, if an object is
    passed in that has these fields set, we want to use those values.
    '''
    shelf_status = indexes.FacetCharField(null=True)
    inventory_notes = indexes.MultiValueField(null=True)
    flags = indexes.MultiValueField(null=True)
    inventory_date = indexes.DateTimeField(null=True)

    def __init__(self, *args, **kwargs):
        super(ShelflistItemIndex, self).__init__(*args, **kwargs)
        self.solr_conn = solr.connect()

    def prepare_shelf_status(self, obj):
        ret_val = getattr(obj, 'shelf_status', None)
        if ret_val is None:
            try:
                item = solr.Queryset(conn=self.solr_conn).filter(id=obj.id)[0]
            except Exception:
                pass
            else:
                ret_val = getattr(item, 'shelf_status', None)
        return ret_val

    def prepare_inventory_notes(self, obj):
        ret_val = getattr(obj, 'inventory_notes', None)
        if ret_val is None:
            try:
                item = solr.Queryset(conn=self.solr_conn).filter(id=obj.id)[0]
            except Exception:
                pass
            else:
                ret_val = getattr(item, 'inventory_notes', None)
        return ret_val

    def prepare_flags(self, obj):
        ret_val = getattr(obj, 'flags', None)
        if ret_val is None:
            try:
                item = solr.Queryset(conn=self.solr_conn).filter(id=obj.id)[0]
            except Exception:
                pass
            else:
                ret_val = getattr(item, 'flags', None)
        return ret_val

    def prepare_inventory_date(self, obj):
        ret_val = getattr(obj, 'inventory_date', None)
        if ret_val is None:
            try:
                item = solr.Queryset(conn=self.solr_conn).filter(id=obj.id)[0]
            except Exception:
                pass
            else:
                ret_val = getattr(item, 'inventory_date', None)
        return ret_val
