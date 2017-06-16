from .base import *

# Check required TEST settings
required = ['TEST_SIERRA_DB_USER', 'TEST_SIERRA_DB_PASSWORD',
            'TEST_DEFAULT_DB_USER', 'TEST_DEFAULT_DB_PASSWORD']

for setting in required:
    if get_env_variable(setting) is None:
        raise_setting_error(setting)

DEBUG = True
TESTING = True

INSTALLED_APPS += (
    'sierra.tests.testmodels',
    'sierra'
)

DATABASES = {
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': get_env_variable('TEST_SIERRA_DB_NAME', 'sierra_test'),
        'USER': get_env_variable('TEST_SIERRA_DB_USER'),
        'PASSWORD': get_env_variable('TEST_SIERRA_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_SIERRA_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_SIERRA_DB_PORT', '5432')
    },
    'default': {
        'ENGINE': get_env_variable('TEST_DEFAULT_DB_ENGINE',
                                   'django.db.backends.mysql'),
        'NAME': get_env_variable('TEST_DEFAULT_DB_NAME', 'capi_test'),
        'USER': get_env_variable('TEST_DEFAULT_DB_USER'),
        'PASSWORD': get_env_variable('TEST_DEFAULT_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_DEFAULT_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_DEFAULT_DB_PORT', '3307')
    }
}

# CELERY settings
REDIS_CELERY_PORT = get_env_variable('TEST_REDIS_CELERY_PORT', '6279')
REDIS_CELERY_HOST = get_env_variable('TEST_REDIS_CELERY_HOST', 'localhost')
redis_celery_url = 'redis://{}:{}/0'.format(REDIS_CELERY_HOST,
                                            REDIS_CELERY_PORT)
BROKER_URL = redis_celery_url
CELERY_RESULT_BACKEND = redis_celery_url

# Other Redis settings
REDIS_CONNECTION = {
    'host': get_env_variable('TEST_REDIS_APPDATA_HOST', 'localhost'),
    'port': get_env_variable('TEST_REDIS_APPDATA_PORT', '6280'),
    'db': get_env_variable('TEST_REDIS_APPDATA_DATABASE', 0)
}

# Solr settings
SOLR_PORT = get_env_variable('TEST_SOLR_PORT', '8883')
SOLR_HOST = get_env_variable('TEST_SOLR_HOST', 'localhost')
solr_haystack_url = get_env_variable('SOLR_HAYSTACK_URL', 
                    'http://{}:{}/solr/haystack'.format(SOLR_HOST, SOLR_PORT))
solr_bibdata_url = get_env_variable('SOLR_BIBDATA_URL', 
                    'http://{}:{}/solr/bibdata'.format(SOLR_HOST, SOLR_PORT))
solr_marc_url = get_env_variable('SOLR_MARC_URL', 
                    'http://{}:{}/solr/marc'.format(SOLR_HOST, SOLR_PORT))

HAYSTACK_CONNECTIONS['default']['URL'] = solr_haystack_url
HAYSTACK_CONNECTIONS['haystack']['URL'] = solr_haystack_url
HAYSTACK_CONNECTIONS['bibdata']['URL'] = solr_bibdata_url
HAYSTACK_CONNECTIONS['marc']['URL'] = solr_marc_url
