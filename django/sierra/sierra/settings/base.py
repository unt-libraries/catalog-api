# Base Django settings for sierra project.

import os
from unipath import Path

import ujson

from django.core.exceptions import ImproperlyConfigured


def get_env_variable(var_name):
    """ Get the environment variable or return None"""
    try:
        return os.environ[var_name]
    except KeyError:
        return None


def raise_setting_error(setting):
    msg = 'The {} setting in settings.json is not set.'.format(setting)
    raise ImproperlyConfigured(msg)


with open('{}/settings.json'.format(Path(__file__).ancestor(1))) as fh:
    local_settings = ujson.loads(fh.read())

if local_settings.get('SIERRA_DB_USER', None) is None:
    raise_setting_error('SIERRA_DB_USER')

if local_settings.get('SIERRA_DB_PASSWORD', None) is None:
    raise_setting_error('SIERRA_DB_PASSWORD')

if local_settings.get('SIERRA_DB_HOST', None) is None:
    raise_setting_error('SIERRA_DB_HOST')

if local_settings.get('SIERRA_DB_PASSWORD', None) is None:
    raise_setting_error('SIERRA_DB_PASSWORD')

if local_settings.get('SECRET_KEY', None) is None:
    raise_setting_error('SECRET_KEY')

if local_settings.get('LOG_FILE_DIR', None) is None:
    raise_setting_error('LOG_FILE_DIR')

if local_settings.get('MEDIA_ROOT', None) is None:
    raise_setting_error('MEDIA_ROOT')

PROJECT_DIR = '{}'.format(Path(__file__).ancestor(3))

# Path to the directory where user-initiated downloads are stored.
# Temporary MARC files get stored here before being loaded by SolrMarc.
# Be sure to create this directory if it doesn't exist.
MEDIA_ROOT = local_settings.get('MEDIA_ROOT')

# Path to the directory where static files get put when you run the 
# collectstatic admin command. Usually only matters in production.
STATIC_ROOT = local_settings.get('STATIC_ROOT', None)

# Path to the directory where log files go. Be sure this directory
# exists, or else you'll get errors.
LOG_FILE_DIR = local_settings.get('LOG_FILE_DIR')

ALLOWED_HOSTS = local_settings.get('ALLOWED_HOSTS', [])

# Defines ports for Solr and Redis instances.
SOLR_PORT = get_env_variable('SOLR_PORT') or 8983
REDIS_PORT = get_env_variable('REDIS_PORT') or 6379

# Defines who gets emailed when error messages happen. Should be a
# tuple of tuples, where the inner tuple contains ('name', 'email')
ADMINS = tuple((a[0], a[1]) for a in local_settings.get('ADMINS', ()))
MANAGERS = ADMINS

DATABASES = {
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'iii',
        'USER': local_settings.get('SIERRA_DB_USER'),
        'PASSWORD': local_settings.get('SIERRA_DB_PASSWORD'),
        'HOST': local_settings.get('SIERRA_DB_HOST'),
        'PORT': '1032',
        'TEST_MIRROR': 'sierra',
        'OPTIONS': {'autocommit': True, },
    },
}

DATABASES['default'] = local_settings.get('DEFAULT_DATABASE', 
    {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'django_sierra',
    })

DATABASE_ROUTERS = ['sierra.routers.SierraRouter']

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = local_settings.get('TIME_ZONE', 'America/Chicago')

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

SITE_URL_ROOT = local_settings.get('SITE_URL_ROOT', '/')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = local_settings.get('MEDIA_URL', '/media/')

# URL prefix for static files.
# Example: "/static/", "http://static.example.com/"
STATIC_URL = local_settings.get('STATIC_URL', '/static/')

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = local_settings.get('SECRET_KEY')

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    #   'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'sierra.middleware.AppendOrRemoveSlashMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'sierra.urls'

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'sierra.wsgi.application'

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'haystack',
    'django_extensions',
    'kombu.transport.django',
    'djcelery',
    'corsheaders',
    'base',
    'export',
    'api',
    'django.contrib.admin',
    'shelflist',
)

SESSION_SERIALIZER = 'django.contrib.sessions.serializers.JSONSerializer'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'datetime': {
            'format': '%(asctime)s %(levelname)s %(message)s'
        }
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        },
        'file': {
            'level': 'ERROR',
            'class': 'logging.FileHandler',
            'filename': '{}/sierra.log'.format(LOG_FILE_DIR),
            'formatter': 'datetime'
        },
        'export_file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': '{}/exporter.log'.format(LOG_FILE_DIR),
            'mode': 'a',
            'formatter': 'datetime'
        },
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
        'django': {
            'handlers': ['file'],
            'level': 'ERROR',
            'propagate': False,
        },
        'sierra.custom': {
            'handlers': ['console'],
            'level': 'INFO',
        },
        'sierra.file': {
            'handlers': ['file'],
            'level': 'INFO',
        },
        'exporter.file': {
            'handlers': ['export_file'],
            'level': 'INFO',
        }
    }
}

solr_haystack_url = local_settings.get('SOLR_HAYSTACK_URL', 
                        'http://127.0.0.1:{}/solr/haystack'.format(SOLR_PORT))
solr_bibdata_url = local_settings.get('SOLR_BIBDATA_URL', 
                        'http://127.0.0.1:{}/solr/bibdata'.format(SOLR_PORT))
solr_marc_url = local_settings.get('SOLR_MARC_URL', 
                        'http://127.0.0.1:{}/solr/marc'.format(SOLR_PORT))

# HAYSTACK_CONNECTIONS, a required setting for Haystack
HAYSTACK_CONNECTIONS = {
    'default': {
        'ENGINE': 'sierra.solr_backend.CustomSolrEngine',
        'URL': solr_haystack_url,
        'EXCLUDED_INDEXES': ['base.search_indexes.ItemIndex'],
        'TIMEOUT': 60 * 20,
    },
    'haystack': {
        'ENGINE': 'sierra.solr_backend.CustomSolrEngine',
        'URL': solr_haystack_url,
        'EXCLUDED_INDEXES': ['base.search_indexes.ItemIndex'],
        'TIMEOUT': 60 * 20,
    },
    'bibdata': {
        'ENGINE': 'sierra.solr_backend.CustomSolrEngine',
        'URL': solr_bibdata_url,
        'TIMEOUT': 60 * 20,
    },
    'marc': {
        'ENGINE': 'sierra.solr_backend.CustomSolrEngine',
        'URL': solr_marc_url,
        'TIMEOUT': 60 * 20,
    },
}

# HAYSTACK_LIMIT_TO_REGISTERED_MODELS, set to False to allow Haystack
# to search our SolrMarc indexes, which are not model-based
HAYSTACK_LIMIT_TO_REGISTERED_MODELS = False

# HAYSTACK_ID_FIELD, change default haystack-internal id
HAYSTACK_ID_FIELD = 'haystack_id'

# REST_FRAMEWORK settings
REST_FRAMEWORK = {
    'PAGINATE_BY': 20,
    'PAGINATE_BY_PARAM': 'limit',
    'PAGINATE_PARAM': 'offset',
    'ORDER_BY_PARAM': 'order_by',
    'SEARCH_PARAM': 'search',
    'SEARCHTYPE_PARAM': 'searchtype',
    'MAX_PAGINATE_BY': 500,
    'DEFAULT_FILTER_BACKENDS': ('api.filters.HaystackFilter',),
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'api.simpleauth.SimpleSignatureAuthentication',
    ),
    'DEFAULT_PAGINATION_SERIALIZER_CLASS':
        'api.pagination.SierraApiPaginationSerializer',
    'DEFAULT_RENDERER_CLASSES': ('api.renderers.HALJSONRenderer',
                            'rest_framework.renderers.BrowsableAPIRenderer',),
    'DEFAULT_PARSER_CLASSES': ('rest_framework.parsers.JSONParser',),
    'EXCEPTION_HANDLER': 'api.exceptions.sierra_exception_handler'
}

# CELERY settings

redis_celery_url = local_settings.get('REDIS_CELERY_URL',
                                'redis://localhost:{}/0'.format(REDIS_PORT))
BROKER_URL = redis_celery_url
BROKER_TRANSPORT_OPTIONS = {
    'visibility_timeout': 3600,
    'fanout_prefix': True,
    'fanout_patterns': True,
}
CELERY_RESULT_BACKEND = redis_celery_url
CELERYBEAT_SCHEDULER = 'djcelery.schedulers.DatabaseScheduler'
CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']
CELERY_TASK_RESULT_EXPIRES = None
CELERYD_TASK_TIME_LIMIT = None
CELERYD_TASK_SOFT_TIME_LIMIT = None
CELERYD_FORCE_EXECV = True
CELERYD_MAX_TASKS_PER_CHILD = 2
CELERY_CHORD_PROPAGATES = False

# CORS settings
CORS_ORIGIN_REGEX_WHITELIST = tuple(
                        local_settings.get('CORS_ORIGIN_REGEX_WHITELIST', ()))
CORS_ALLOW_HEADERS = (
    'x-requested-with',
    'content-type',
    'accept',
    'origin',
    'authorization',
    'x-csrftoken',
    'x-username',
    'x-timestamp'
)


# Sierra Export App settings

# Username for the user that should be attached to Export Instances for
# scheduled (automated) export jobs.
EXPORTER_AUTOMATED_USERNAME = local_settings.get('EXPORTER_AUTOMATED_USERNAME',
                                                 'django_admin')

# Change the below to change what label is used for Exporter log
# messages coming from Celery tasks.
TASK_LOG_LABEL = 'Scheduler'

# This maps Exporter jobs to the haystack connection names that each
# should use.
EXPORTER_HAYSTACK_CONNECTIONS = {
    'ItemsToSolr': 'haystack',
    'BibsToSolr:BIBS': 'bibdata',
    'BibsToSolr:MARC': 'marc',
    'LocationsToSolr': 'haystack',
    'ItypesToSolr': 'haystack',
    'ItemStatusesToSolr': 'haystack',
    'EResourcesToSolr': 'haystack',
    'HoldingUpdate': 'haystack',
    'AllMetadataToSolr': 'haystack',
}

# Determines whether the Exporter jobs email site admins when a job
# generates errors and/or warnings.
EXPORTER_EMAIL_ON_ERROR = local_settings.get('EXPORTER_EMAIL_ON_ERROR', True)
EXPORTER_EMAIL_ON_WARNING = local_settings.get('EXPORTER_EMAIL_ON_WARNING',
                                               True)

# Maps III record types to Exporter jobs that should run for those
# record types when an "All" type export is run. Note that you can map
# multiple jobs to the same record type. In this case all specified
# jobs will run when the All export is triggered.
EXPORTER_ALL_TYPE_REGISTRY = {
    'b': ['BibsToSolr'],
    'i': ['ItemsToSolr'],
    'e': ['EResourcesToSolr']
}

# List of Exporter jobs that should be triggered when an AllMetadata
# exporter job is run.
EXPORTER_METADATA_TYPE_REGISTRY = [
    'LocationsToSolr', 'ItypesToSolr', 'ItemStatusesToSolr',
]

# The path (relative or absolute) to the command that runs SolrMarc.
SOLRMARC_COMMAND = '../../solr/solrmarc/indexfile.sh'
# The name of the properties file to use when running SolrMarc.
SOLRMARC_CONFIG_FILE = local_settings.get('SOLRMARC_CONFIG_FILE',
                                          'dev_config.properties')

# This maps DRF views to haystack connections. Only needed for views
# that don't use the default connection.
REST_VIEWS_HAYSTACK_CONNECTIONS = {
    'Bibs': 'bibdata',
    'Marc': 'marc',
}

# This specifies which installed apps have user permissions settings
# for the APIUser (api app), allowing you to define app-specific
# permissions in apps and manage them centrally via the api app.
# For apps with custom permissions, implement as a dict named 
# "permissions" in the app's __init__.py.
API_PERMISSIONS = [
    'shelflist',
]

# REDIS_CONNECTION details connection details for the Redis instance
# and database where App data for the catalog API will live. It's
# strongly recommended that this be a different database than your
# celery broker.
REDIS_CONNECTION = {
    'host': local_settings.get('REDIS_APPDATA_HOST', 'localhost'),
    'port': local_settings.get('REDIS_APPDATA_PORT', REDIS_PORT),
    'db': local_settings.get('REDIS_APPDATA_DATABASE', 1)
}

# Do we allow access to the admin interface on /admin URL?
ADMIN_ACCESS = local_settings.get('ADMIN_ACCESS', True)

# Is this settings file for testing?
TESTING = False
