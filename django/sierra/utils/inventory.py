"""
This is small collection of utility functions for managing shelflists
for inventory projects.

Use `set_item_fields` to set fields on specific docs or to set certain
fields on all docs to default values (at specific locations).

Use `clear_inventory_locations` to do a reset on *all* inventory-
specific fields for items at specific locations. This is for clearing
inventory history when someone wants to start a new inventory from
scratch.

If you want to reset shelf statuses or flags but keep notes for
historical purposes, you can use `set_item_fields` with
`auto_notes=True`. This will auto-generate the appropriate system notes
to log the actions taken to clear the status and each flag, to ensure
the app continues to log future actions correctly.
"""

from datetime import datetime
import pytz

from utils import solr


FLAG_CODES = {
    'problemData': 'PR-ITEM-DATA',
    'problemWrongSL': 'PR-ITEM-WRONGSL',
    'problemAdjItemsNISL': 'PR-ADJITEMS-NISL',
    'problemShelflistSort': 'PR-SL-SORT',
    'problemOther': 'PR-ITEM-OTHER',
    'workflowEnd': 'WF-END',
    'workflowPulled': 'WF-PULLED',
    'workflowOther': 'WF-OTHER',
}
SYSTEMLOG_STATUS = '@SYSTEMLOG-STATUS'
SYSTEMLOG_FLAG = '@SYSTEMLOG-FLAG'


# Here are some private functions used for auto-generating inventory
# notes.

def _make_system_inventory_note(user, msg):
    now = solr.Queryset()._val_to_solr_str(datetime.now(pytz.UTC))
    return '|'.join([now, user, msg])


def _make_ss_notes(doc, new_val):
    if doc.get('shelf_status') == new_val:
        return []

    if new_val is None:
        msg = 'BATCH-PROCESS cleared status'
    else:
        msg = 'BATCH-PROCESS set status to {}'.format(new_val)
    return [_make_system_inventory_note(SYSTEMLOG_STATUS, msg)]


def _make_fl_notes(doc, new_val):
    old_flag_set = set(doc.get('flags') or [])
    new_flag_set = set(new_val or [])
    notes = []

    if old_flag_set == new_flag_set:
        return []

    clear_flags = old_flag_set - new_flag_set
    add_flags = new_flag_set - old_flag_set
    for flag in clear_flags | add_flags:
        flag_code = FLAG_CODES.get(flag, flag)
        verb = 'cleared' if flag in clear_flags else 'set'
        msg = 'BATCH-PROCESS {} flag {}'.format(verb, flag_code)
        notes.append(_make_system_inventory_note(SYSTEMLOG_FLAG, msg))
    return notes


def rows_to_data(rows):
    """
    Convert a list of rows to the `data` format for set_item_fields.
    """
    return {row['id']: row for row in rows}


def set_item_fields(locations, data={}, default={}, batch_size=1000,
                    using='haystack|update', verbose=True, auto_notes=True):
    """
    Set fields on a batch of shelflist items in Solr and reload them.

    Args:
        - locations: A list of location code (strings) to operate on.
        - data: (Optional) A dict mapping Solr IDs to item data dicts.
          If provided, each matching item is updated with whatever is
          in the data dict. The item in Solr is overwritten completely.
        - default: (Optional) A dict containing default values to use
          for items NOT in `data`. Fields not provided are carried over
          from the item in Solr. Use {'field': None} to clear a field.
        - batch_size: (Default 1000) An int for how many records to
          include with each call to update Solr.
        - using: (Default 'haystack') The Solr core on which to run the
          update.
        - verbose: (Default True) If True, prints messages to stdout
          for each location and each batch to indicate progress. If
          False, no output is printed.
        - auto_notes: (Default True) If True, AND if the values to be
          set do not include 'inventory_notes' (i.e., you're not
          providing your own notes), AND if the values to be set DO
          include 'shelf_status' or 'flags' -- add appropriate system
          notes to simulate setting/clearing the status and/or flags,
          as needed.
    """
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
                values_to_set = data.get(doc['id']) or default.copy()
                if values_to_set:
                    if auto_notes:
                        set_ss = 'shelf_status' in values_to_set
                        set_fl = 'flags' in values_to_set
                        set_in = 'inventory_notes' in values_to_set
                        if (set_ss or set_fl) and not set_in:
                            notes = doc.get('inventory_notes') or []
                            if set_ss:
                                new_ss = values_to_set['shelf_status']
                                notes.extend(_make_ss_notes(doc, new_ss))
                            if set_fl:
                                new_fl = values_to_set['flags']
                                notes.extend(_make_fl_notes(doc, new_fl))
                            values_to_set['inventory_notes'] = notes or None
                    doc.update(values_to_set)
                    batch.append(doc)
                    
            if verbose:
                real_end = num_items if num_items <= end else end
                print('Adding items {} to {} ...'.format(start + 1, real_end))
            qs._conn.add(batch, commit=False)

        # A final hard commit after each location
        solr.commit(qs._conn, using)


def clear_inventory_locations(locations, batch_size=1000, using='haystack',
                              verbose=True):
    """
    Clear all inventory-specific fields for a batch of shelflist items.

    This is just a convenience wrapper for `set_item_fields`. You still
    must provide a list of locations to update.

    See `set_item_fields` for a description of each applicable arg.

    Args:
        - locations
        - batch_size (Default 1000)
        - using (Default 'haystack')
        - verbose (Default True)
    """
    default = {
        'shelf_status': None,
        'inventory_notes': None,
        'inventory_date': None,
        'flags': None
    }
    set_item_fields(locations, default=default, batch_size=batch_size,
                    using=using, verbose=verbose)


def copy_inventory_data_from_other_index(
    locations, from_url='http://localhost:8983/solr/haystack',
    to_using='haystack|update', batch_size=1000, verbose=True,
    from_key='haystack_id', to_key='haystack_id'    
):
    """
    Copy inventory-specific data from one index to another.

    We use this when we need to migrate to new Solr servers and we want
    to copy all inventory-specific data from the old one to the new
    one. The overall workflow is:    
        - On the new server, reindex all item data from Sierra into the
          haystack index. When finished, inventory-specific data will
          not yet be present.
        - On the process server (on the new stack), open an SSH tunnel
          to the old production Solr server that maps port 8983 to
          localhost:8983.
        - On the process server (on the new stack), run this function,
          either from a shell or a script. Provide the list of location
          codes you wish to copy over.

    Args:
        - locations, a list of locations to act on.
        - from_url, the Solr server external to the current stack
          you wish to copy data from. Default is
          'http://localhost:8983/solr/haystack'.
        - to_using, the haystack connection string you want to use to
          load data into. Default is 'haystack|update'.
        - batch_size, default is 1000
        - verbose, default is True
        - from_key, the field in the "from" Solr data you wish to use
          as a match key. Default is 'haystack_id'.
        - to_key, the field in the "to" Solr data you wish to use as a
          match key. Default is 'haystack_id'. Note that the values in
          your 'from_key' field must match the values in your 'to_key'
          field.
    """
    inv_fields = ['shelf_status', 'inventory_notes', 'inventory_date', 'flags']
    for location in locations:
        old_qs = solr.Queryset(url=from_url, page_by=batch_size)
        old_qs = old_qs.filter(location_code=location).order_by(from_key)
        new_qs = solr.Queryset(using=to_using, page_by=batch_size)
        new_qs = new_qs.filter(location_code=location).order_by(to_key)
        nqs_iter = iter(new_qs)
        num_items = old_qs.count()
        batch = []
        num_updated = 0
        num_skipped = 0
        done_flag = False

        if verbose:
            print()
            print(f'Location ___{location}___ ({num_items} items)')

        new_item = next(nqs_iter, None)
        for old_item in old_qs:
            total_seen = num_updated + num_skipped
            if verbose and total_seen % batch_size == 0:
                pc = round((total_seen / num_items) * 100, 2)
                print(f'Updated {num_updated}, skipped {num_skipped} ({pc}%)')

            if len(batch) == batch_size:
                new_qs._conn.add(batch, commit=False)
                batch = []
                if verbose:
                    print(f'===Adding {batch_size} docs to Solr===')

            updated_flag = False
            while True:
                if new_item is None:
                    done_flag = True
                    break
                if old_item[from_key] < new_item[to_key]:
                    break
                if old_item[from_key] == new_item[to_key]:
                    for field in inv_fields:
                        if old_item.get(field) != new_item.get(field):
                            new_item[field] = old_item.get(field)
                            updated_flag = True
                    if updated_flag:
                        del(new_item['_version_'])
                        batch.append(new_item)
                    new_item = next(nqs_iter, None)
                    break
                new_item = next(nqs_iter, None)

            if updated_flag:
                num_updated += 1
            else:
                num_skipped += 1

            if done_flag:
                num_left = num_items - (num_updated + num_skipped)
                num_skipped += num_left
                break

        if batch:
            new_qs._conn.add(batch, commit=False)
            if verbose:
                print(f'===Adding {len(batch)} docs to Solr===')

        if verbose:
            print(f'Done. Updated {num_updated}, skipped {num_skipped}.')
            print()

        # A final hard commit after each location
        solr.commit(new_qs._conn, to_using)
