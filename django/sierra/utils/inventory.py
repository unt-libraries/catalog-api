"""
This is small collection of utility functions for managing shelflists
for inventory projects.

Use `set_item_fields` to set fields on specific docs or to set certain
fields on all docs to default values (at specific locations).

Use `clear_inventory_locations` to do a reset on inventory-specific
fields for items at specific locations. This is for clearing inventory
history when someone wants to start a new inventory from scratch.
"""

from utils import solr


def rows_to_data(rows):
    return {row['id']: row for row in rows}


def set_item_fields(locations, data={}, default={}, batch_size=1000,
                    using='haystack', verbose=True):
    for location in locations:
        qs = solr.Queryset(using=using)
        qs = qs.filter(location_code=location).order_by('id')
        num_items = qs.count()
        if verbose:
            print('Location ___{}___ ({} items)'.format(location, num_items))
        
        for start in range(0, num_items, batch_size):
            end = start + batch_size
            batch = []
            for doc in qs[start:end]:
                values_to_set = data.get(doc['id']) or default
                if values_to_set:
                    doc.update(values_to_set)
                    batch.append(doc)
                    
            if verbose:
                real_end = num_items if num_items <= end else end
                print('Adding items {} to {} ...'.format(start + 1, real_end))
            qs._conn.add(batch, commit=False)

        # A final hard commit after each location
        qs._conn.commit()


def clear_inventory_locations(locations, batch_size=1000, using='haystack',
                              verbose=True):
    default = {
        'shelf_status': None,
        'inventory_notes': None,
        'inventory_date': None,
        'flags': None
    }
    set_item_fields(locations, default=default, batch_size=batch_size,
                    using=using, verbose=verbose)
