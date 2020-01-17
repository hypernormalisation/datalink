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


def factory(
        name, table, fields,
        url=None,
        database=None,
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

    # If url not supplied, assume sqlite and use database as file path
    # to construct an sqlite URL using the default sqlalchemy driver.
    if not url:
        url = sqlalchemy.engine.url.URL('sqlite', database=database)

    new_class = types.new_class(name, bases=(dlstores.DataStore, ))
    new_class.__name__ = name

    new_class.url = str(url)
    new_class.table = table
    new_class._fields = fields

    new_class._lookup = lookup
    new_class._bidirectional = bidirectional
    return new_class
