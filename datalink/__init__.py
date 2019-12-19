# -*- coding: utf-8 -*-

__author__ = 'Stephen Ogilvy'
__copyright__ = 'Stephen Ogilvy'
__licence__ = 'GPLv3'
__version__ = '0.0.1'
__email__ = 'sogilvy@protonmail.com'

import logging
import collections.abc
import datalink.links as dllinks
import datalink.stores as dlstores
import datalink.utils as dlutils


log = logging.getLogger(__name__)


def test_output():
    log.info('logging from datalink')


def link_factory(
        name=None, db_path=None,
        table_name=None, data_fields=None, lookup='uuid',
        dialect='sqlite',
        ):
    """
    Factory function to produce a new class derived from DataStore.
    """
    for arg in [name, db_path, table_name]:
        if not arg:
            raise ValueError(f'{arg} is a required field.')
    if not data_fields and not isinstance(data_fields, collections.abc.Mapping):
        raise ValueError('data_fields must be a valid mapping.')

    class NewClass(dlstores.DataStore):
        pass
    NewClass.__name__ = name
    NewClass.db_path = db_path
    NewClass.table_name = table_name
    NewClass._datastore_map = data_fields
    NewClass.lookup = lookup
    NewClass.dialect = dialect
    return NewClass
