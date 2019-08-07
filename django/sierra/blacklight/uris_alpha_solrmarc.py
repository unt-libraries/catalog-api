"""
URI definitions for `asm-search-suggestions` and
`asm-browse-suggestions` REST resources, for bl-suggest.
"""

from api.uris import Uris

class AsmUris(Uris):
    root = r'api/'
    named_uripatterns = {
        'asm-search-suggestions-list': [r'v', {'v': r'1'},
                                        r'/asm-search-suggestions/'],
        'asm-browse-suggestions-list': [r'v', {'v': r'1'},
                                        r'/asm-browse-suggestions/']
    }
