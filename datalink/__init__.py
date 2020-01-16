# -*- coding: utf-8 -*-

__author__ = 'Stephen Ogilvy'
__copyright__ = 'Stephen Ogilvy'
__licence__ = 'GPLv3'
__version__ = '0.0.1'
__email__ = 'sogilvy@protonmail.com'

import logging
import types
import collections.abc
import datalink.links as dllinks
import datalink.stores as dlstores
import datalink.utils as dlutils


log = logging.getLogger(__name__)


def test_output():
    log.info('Test logging output from datalink.')


def factory(
        name=None, db_path=None,
        table_name=None, fields=None, lookup='uuid',
        dialect='sqlite', bidirectional=True
        ):
    """
    Factory function to produce a new class derived from DataStore.
    """
    for arg in [name, db_path, table_name]:
        if not arg:
            raise ValueError(f'{arg} is a required field.')
    if not fields and not isinstance(fields, collections.abc.Mapping):
        raise ValueError('data_fields must be a valid mapping.')

    new_class = types.new_class(name, bases=(dlstores.DataStore, ))
    new_class.__name__ = name
    new_class.db_path = db_path
    new_class.table_name = table_name
    new_class._datastore_map = fields
    new_class.lookup = lookup
    new_class.dialect = dialect
    new_class.bidirectional = bidirectional
    return new_class
