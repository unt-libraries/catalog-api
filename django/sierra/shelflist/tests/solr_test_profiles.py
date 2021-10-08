"""
Contains structures needed to make Solr test data for `shelflist` app.
"""

from __future__ import absolute_import
import random

from six import text_type
from six.moves import range

from utils.test_helpers import solr_factories as sf
from utils.test_helpers import solr_test_profiles as tp


SOLR_TYPES = tp.SOLR_TYPES
GLOBAL_UNIQUE_FIELDS = tp.GLOBAL_UNIQUE_FIELDS
GENS = tp.GENS


SHELFLISTITEM_FIELDS = tp.ITEM_FIELDS + (
    'inventory_date', 'shelf_status', 'flags', 'inventory_notes'
)


# ShelflistItem-specific field gens

def shelf_status(record):
    if 'inventory_date' in record:
        choices = ['unknown'] + ['onShelf'] * 6 + ['notOnShelf'] * 3
        return random.choice(choices)


def flags(record):
    if 'inventory_date' in record:
        flags = []
        emitter = sf.DataEmitter()
        for _ in range(0, random.randint(0, 3)):
            flags.append(text_type(emitter.emit('string', mn=5, mx=10)))
        return flags or None


def inventory_notes(record):
    emitter = sf.DataEmitter()

    def format_note(date, msg):
        dstr = '{}-{}-{}T{}:{}:{}Z'.format(date.year, date.month, date.day,
                                           date.hour, date.minute, date.second)
        return text_type('{}|{}').format(dstr, msg)

    def username():
        return text_type(emitter.emit('string', mn=4, mx=8,
                                      alphabet=tp.LETTERS_LOWER))

    def status_msg(status):
        if status == 'unknown':
            action = 'cleared status'
        else:
            action = 'set status to {}'.format(status)
        return text_type('@SYSTEMLOG-STATUS|{} {}').format(username(), action)

    def flag_msg(flag, action):
        return text_type('@SYSTEMLOG-FLAG|{} {} flag {}').format(username(),
                                                                 action, flag)

    def manual_msg():
        msg = emitter.emit('text', mn_words=1, mx_words=5)
        return text_type('{}|{}').format(username(), msg)

    if 'inventory_date' in record:
        notes = []
        d = record['inventory_date']
        notes.append(format_note(d, status_msg(record['shelf_status'])))

        for flag in record.get('flags', []):
            d = emitter.emit('date', mn=(d.year, d.month, d.day, d.hour,
                                         d.minute))
            notes.append(format_note(d, flag_msg(flag, 'set')))

        for _ in range(0, random.randint(0, 2)):
            d = emitter.emit('date', mn=(d.year, d.month, d.day, d.hour,
                                         d.minute))
            notes.append(format_note(d, manual_msg()))
        return notes


SHELFLISTITEM_GENS = tp.ITEM_GENS + (
    ('inventory_date', tp.chance(GENS.type('date'), 50)),
    ('shelf_status', shelf_status),
    ('flags', flags),
    ('inventory_notes', inventory_notes),
)
