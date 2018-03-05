# Base Django settings for sierra project.

import os
from unipath import Path

import dotenv

from django.core.exceptions import ImproperlyConfigured


def get_env_variable(var_name, default=None):
    """Get the environment variable or return default value"""
    val = os.environ.get(var_name, default)
    return True if val == 'true' else False if val == 'false' else val


def raise_setting_error(setting):
    raise ImproperlyConfigured('The {} setting is not set.'.format(setting))


# Use dotenv to load env variables from a .env file
dotenv.load_dotenv('{}/.env'.format(Path(__file__).ancestor(1)))

# Check required settings
required = ['SIERRA_DB_USER', 'SIERRA_DB_PASSWORD', 'SIERRA_DB_HOST',
            'SECRET_KEY', 'LOG_FILE_DIR', 'MEDIA_ROOT', 'DEFAULT_DB_USER',
            'DEFAULT_DB_PASSWORD']

for setting in required:
    if get_env_variable(setting) is None:
        raise_setting_error(setting)


PROJECT_DIR = '{}'.format(Path(__file__).ancestor(3))

# Path to the directory where user-initiated downloads are stored.
# Temporary MARC files get stored here before being loaded by SolrMarc.
# Be sure to create this directory if it doesn't exist.
MEDIA_ROOT = get_env_variable('MEDIA_ROOT')

# Path to the directory where static files get put when you run the 
# collectstatic admin command. Usually only matters in production.
STATIC_ROOT = get_env_variable('STATIC_ROOT')

# Path to the directory where log files go. Be sure this directory
# exists, or else you'll get errors.
LOG_FILE_DIR = get_env_variable('LOG_FILE_DIR')

ALLOWED_HOSTS = get_env_variable('ALLOWED_HOSTS', '').split(' ')

# Defines who gets emailed when error messages happen. Should be a
# tuple of tuples, where the inner tuple contains ('name', 'email')
admin_list = get_env_variable('ADMINS', '').split(';')
ADMINS = tuple(tuple(admin.split(',')) for admin in admin_list)
MANAGERS = ADMINS

DATABASES = {
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'iii',
        'USER': get_env_variable('SIERRA_DB_USER'),
        'PASSWORD': get_env_variable('SIERRA_DB_PASSWORD'),
        'HOST': get_env_variable('SIERRA_DB_HOST'),
        'PORT': '1032',
        'TEST': {
            'MIRROR': 'sierra',
        },
        'OPTIONS': {'autocommit': True, },
    },
    'default': {
        'ENGINE': get_env_variable('DEFAULT_DB_ENGINE', 
                                   'django.db.backends.mysql'),
        'NAME': get_env_variable('DEFAULT_DB_NAME', 'django_catalog_api'),
        'USER': get_env_variable('DEFAULT_DB_USER'),
        'PASSWORD': get_env_variable('DEFAULT_DB_PASSWORD'),
        'HOST': get_env_variable('DEFAULT_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('DEFAULT_DB_PORT', '3306'),
        'TEST': {
            'MIRROR': 'default',
        },
    }
}

DATABASE_ROUTERS = ['sierra.routers.SierraRouter']

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = get_env_variable('TIME_ZONE', 'America/Chicago')

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

SITE_URL_ROOT = get_env_variable('SITE_URL_ROOT', '/')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = get_env_variable('MEDIA_URL', '/media/')

# URL prefix for static files.
# Example: "/static/", "http://static.example.com/"
STATIC_URL = get_env_variable('STATIC_URL', '/static/')

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = get_env_variable('SECRET_KEY')

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

SOLR_PORT = get_env_variable('SOLR_PORT', '8983')
SOLR_HOST = get_env_variable('SOLR_HOST', '127.0.0.1')
solr_haystack_url = get_env_variable('SOLR_HAYSTACK_URL', 
                    'http://{}:{}/solr/haystack'.format(SOLR_HOST, SOLR_PORT))
solr_bibdata_url = get_env_variable('SOLR_BIBDATA_URL', 
                    'http://{}:{}/solr/bibdata'.format(SOLR_HOST, SOLR_PORT))
solr_marc_url = get_env_variable('SOLR_MARC_URL', 
                    'http://{}:{}/solr/marc'.format(SOLR_HOST, SOLR_PORT))

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
REDIS_CELERY_PORT = get_env_variable('REDIS_CELERY_PORT', '6379')
REDIS_CELERY_HOST = get_env_variable('REDIS_CELERY_HOST', '127.0.0.1')
redis_celery_url = 'redis://{}:{}/0'.format(REDIS_CELERY_HOST,
                                            REDIS_CELERY_PORT)
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
            get_env_variable('CORS_ORIGIN_REGEX_WHITELIST', '').split(' '))
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
EXPORTER_AUTOMATED_USERNAME = get_env_variable('EXPORTER_AUTOMATED_USERNAME',
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
EXPORTER_EMAIL_ON_ERROR = get_env_variable('EXPORTER_EMAIL_ON_ERROR', True)
EXPORTER_EMAIL_ON_WARNING = get_env_variable('EXPORTER_EMAIL_ON_WARNING', True)

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
SOLRMARC_COMMAND = get_env_variable('SOLRMARC_COMMAND',
                                    '../../solr/solrmarc/indexfile.sh')
# The name of the properties file to use when running SolrMarc.
SOLRMARC_CONFIG_FILE = get_env_variable('SOLRMARC_CONFIG_FILE',
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

# REDIS_CONNECTION stores connection details for the Redis instance
# and database where App data for the catalog API will live. It's
# strongly recommended that this be a different port and/or database
# than your Celery broker.
REDIS_CONNECTION = {
    'host': get_env_variable('REDIS_APPDATA_HOST', '127.0.0.1'),
    'port': get_env_variable('REDIS_APPDATA_PORT', '6380'),
    'db': get_env_variable('REDIS_APPDATA_DATABASE', 0)
}

# Do we allow access to the admin interface on /admin URL?
ADMIN_ACCESS = get_env_variable('ADMIN_ACCESS', True)

# Is this settings file for testing?
TESTING = False
