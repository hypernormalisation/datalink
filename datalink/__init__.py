# -*- coding: utf-8 -*-

__author__ = 'Stephen Ogilvy'
__copyright__ = 'Stephen Ogilvy'
__licence__ = 'GPLv3'
__version__ = '0.0.1'
__email__ = 'sogilvy@protonmail.com'

import logging
import sqlalchemy
import types
import collections.abc
import datalink.links as dllinks
import datalink.stores as dlstores
import datalink.utils as dlutils


log = logging.getLogger(__name__)


def test_output():
    log.info('Test logging output from datalink.')


# def format_url(
#         database=None, dialect='sqlite',
#         username=None, password=None, host=None, port=None,
#         ):
#     """Function to create a URL from the config to be used in sqlalchemy."""
#
#     if 'sqlite' in dialect:
#         return sqlalchemy.engine.url.URL(dialect, database=database)
#
#     if 'postgres' in dialect:
#         if password:
#             return sqlalchemy.engine.url.URL(
#                 dialect, database=database, host=host, username=username, password=password
#             )
#         else:
#             return sqlalchemy.engine.url.URL(
#                 dialect, database=database, host=host, username=username,
#             )
#

def factory(
        name, table, fields,
        url=None,
        database=None,
        # dialect='sqlite',
        # username=None, password=None, host=None, port=None,
        lookup='uuid', bidirectional=True,
        ):
    """
    Factory function to produce a new class derived from DataStore.
    """
    # Check args and kwargs
    for arg in [name, fields, table]:
        if not arg:
            raise ValueError(f'{arg} is a required positional field.')
    if not isinstance(fields, collections.abc.Mapping):
        raise ValueError('fields must be a valid mapping.')

    # If url not supplied, use the config to construct a database URL.
    if not url:
        url = sqlalchemy.engine.url.URL(dialect, database=database)
        # url = format_url(
        #     database=database, dialect=dialect, username=username,
        #     password=password, host=host, port=port
        # )

    new_class = types.new_class(name, bases=(dlstores.DataStore, ))
    new_class.__name__ = name

    new_class.url = str(url)
    new_class.table = table
    new_class._fields = fields

    new_class._lookup = lookup
    new_class._bidirectional = bidirectional
    return new_class
