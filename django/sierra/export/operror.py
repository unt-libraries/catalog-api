try:
    from MySQLdb import OperationalError
except ImportError:
    class OperationalError(Exception):
        pass
