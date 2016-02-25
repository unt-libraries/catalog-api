from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.core.urlresolvers import reverse_lazy
from django.views.generic import RedirectView
admin.autodiscover()

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')

urlpatterns = patterns('',
    url(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'))),
    url(r'^api/', include('shelflist.urls')),
    url(r'^api/', include('api.urls'))
) + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.ADMIN_ACCESS:
    urlpatterns += patterns('', url(r'^admin/', include(admin.site.urls)))
