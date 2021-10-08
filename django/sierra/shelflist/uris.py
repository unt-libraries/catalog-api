from __future__ import absolute_import

from api.uris import Uris


class ShelflistAPIUris(Uris):
    root = r'api/'
    named_uripatterns = {
        'shelflistitems-list': [r'v', {'v': r'1'}, r'/locations/',
                                {'code': ''}, r'/shelflistitems/'],
        'shelflistitems-detail': [r'v', {'v': r'1'}, r'/locations/',
                                  {'code': ''}, r'/shelflistitems/', {'id': ''}],
    }
