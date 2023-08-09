from __future__ import absolute_import

import logging

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include, re_path
from django.urls import reverse_lazy
from django.views.generic import RedirectView

admin.autodiscover()


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')

urlpatterns = [
    re_path(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'),
                                        permanent=True)),
    re_path(r'^api/', include('shelflist.urls')),
    re_path(r'^api/', include('api.urls'))
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.ADMIN_ACCESS:
    urlpatterns.append(re_path(r'^admin/', admin.site.urls))

if settings.DEBUG:
    import debug_toolbar
    urlpatterns.append(re_path(r'^__debug__/', include(debug_toolbar.urls)))
