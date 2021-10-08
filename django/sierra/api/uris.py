from __future__ import absolute_import

import re

from django.conf import settings


class Uris(object):
    '''
    Provides a simpler (or at least faster) way to build URIs for a
    REST API project that requires links between resources and thus
    lots of potentially slow URL reversals.

    The named_uripatterns member is similar to Django's urlpatterns,
    except it's made for simple lookups of URLs based on names, and it
    uses arrays instead of REs to build parameters into the URLs. Each
    named pattern consists of a name and an array. Array elements can
    be strings or one-element dicts. One-element dicts provide for
    named parameters--the parameter name is the key and a default value
    is the value (or an empty string). When dicts are replaced with
    values passed to class methods, the array is basically joined to
    build a uri or urlpattern.

    To use this in other apps, simply subclass this class and provide new
    named_uripatterns.
    '''
    root = ''
    named_uripatterns = {}

    @classmethod
    def get_uri(self, name, req=None, absolute=False, template=False,
                **kwargs):
        '''
        Get a named URI based on parameters passed via kwargs. Each
        kwarg should match the name of a named_uripatterns parameter.
        If absolute is True, this builds an absolute URI, in which case
        req should contain the current request object.
        '''
        if absolute:
            uri = re.sub(r'(//[^/]*)/.*$', r'\1', req.build_absolute_uri())
            uri = '{}{}{}'.format(uri, settings.SITE_URL_ROOT, self.root)
        else:
            uri = '/{}'.format(self.root)
        pattern = self.named_uripatterns.get(name, None)

        if pattern is not None:
            for p in pattern:
                if isinstance(p, dict):
                    if (template and not list(p.values())[0]):
                        uri = '{}{{{}}}'.format(uri, list(p.keys())[0])
                    else:
                        uri = '{}{}'.format(uri, kwargs.get(list(p.keys())[0],
                                                            list(p.values())[0]))
                else:
                    uri = '{}{}'.format(uri, p)

        return uri

    @classmethod
    def get_urlpattern(self, name, **kwargs):
        '''
        Get an RE pattern for plugging into Django's urlpatterns.
        Provide the name and the relevant parameters in kwargs and get
        back the pattern. If kwargs aren't provided, a generic pattern
        that captures digits and alphabet characters is used for each
        URL parameter.
        '''
        pattern = self.named_uripatterns.get(name, None)
        ret = r'^'

        if pattern is not None:
            for p in pattern:
                if isinstance(p, dict):
                    ret = r'{}{}'.format(ret, kwargs.get(list(p.keys())[0],
                                                         r'([0-9A-Za-z]+)'))
                else:
                    ret = r'{}{}'.format(ret, p)
        else:
            ret = self.root

        return r'{}$'.format(ret)


class APIUris(Uris):
    root = r'api/'
    named_uripatterns = {
        'api-root': [r'v', {'v': r'1'}, r'/'],
        'apiusers-list': [r'v', {'v': r'1'}, r'/apiusers/'],
        'apiusers-detail': [r'v', {'v': r'1'}, r'/apiusers/', {'id': ''}],
        'items-list': [r'v', {'v': r'1'}, r'/items/'],
        'items-detail': [r'v', {'v': r'1'}, r'/items/', {'id': ''}],
        'bibs-list': [r'v', {'v': r'1'}, r'/bibs/'],
        'bibs-detail': [r'v', {'v': r'1'}, r'/bibs/', {'id': ''}],
        'marc-list': [r'v', {'v': r'1'}, r'/marc/'],
        'marc-detail': [r'v', {'v': r'1'}, r'/marc/', {'id': ''}],
        'locations-list': [r'v', {'v': r'1'}, r'/locations/'],
        'locations-detail': [r'v', {'v': r'1'}, r'/locations/', {'code': ''}],
        'itemtypes-list': [r'v', {'v': r'1'}, r'/itemtypes/'],
        'itemtypes-detail': [r'v', {'v': r'1'}, r'/itemtypes/', {'code': ''}],
        'itemstatuses-list': [r'v', {'v': r'1'}, r'/itemstatuses/'],
        'itemstatuses-detail': [r'v', {'v': r'1'}, r'/itemstatuses/',
                                {'code': ''}],
        'callnumbermatches-list': [r'v', {'v': r'1'}, r'/callnumbermatches/'],
        'firstitemperlocation-list': [r'v', {'v': r'1'},
                                      r'/firstitemperlocation/'],
        'eresources-list': [r'v', {'v': r'1'}, r'/eresources/'],
        'eresources-detail': [r'v', {'v': r'1'}, r'/eresources/', {'id': ''}],
    }
