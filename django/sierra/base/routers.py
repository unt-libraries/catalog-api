class BaseRouter(object):
    '''
    Contains a simple database router for base Sierra app. Used to
    direct all queries of models of this app to the Sierra database.
    Make sure that your database for Sierra is named 'sierra' in your
    settings.py configuration.
    '''
    def db_for_read(self, model, **hints):
        '''
        Routes all read attempts for base models to Sierra DB.
        '''
        if model._meta.app_label == 'base':
            return 'sierra'
        return None

    def allow_migrate(self, db, model):
        '''
        We NEVER want to sync the Sierra models with a live db. Sierra
        itself is read only, and you probably don't want the 344+
        tables for Sierra accidentally being added to your default
        Django database.
        '''
        if model._meta.app_label == 'base':
            return False
        return None
