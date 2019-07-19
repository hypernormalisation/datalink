import copy
import ast
import collections.abc
import json
import datalink.links
import logging

log = logging.getLogger(__name__)


class DataStoreDescriptor(object):
    """A descriptor for the relevant key in the data store."""

    def __init__(self, key):
        self.key = key

    def __get__(self, instance, owner):
        return instance._data[self.key]

    def __set__(self, instance, value):
        instance._data[self.key] = value
        if instance._has_data_updated:
            instance._save_state()
            instance._set_data_hash()


class DataStore:
    """Class for a basic mapping data store."""
    db_path = None
    table_name = None
    _data_fields = {}
    _config = None

    def __init__(self, config=None, **kwargs):
        self._hash_previous = None
        self._data = self._data_fields

        # Dynamically generate any required class properties.
        for key in self._data:
            if not hasattr(self.__class__, key):
                setattr(self.__class__, key, DataStoreDescriptor(key))

        # Intercept any field initialisations.
        d = {}
        for k, v in kwargs.items():
            if k in self._data_fields:
                d[k] = v
        for k in d:
            kwargs.pop(k)

        # Perform a first hashing of the data from the defaults.
        self._set_data_hash()

        # Establish link
        if not self._config:
            self.link = datalink.links.UniqueLookup(table_name=self.table_name,
                                                    db_path=self.db_path,
                                                    **kwargs)
        else:
            self.link = datalink.links.NamespaceLookup(**kwargs)

        # Check for any found data and initialise it.
        if self.link.loaded_data:
            self._format_loaded_data()
        # Else initialise any variables from the declaration.
        else:
            if d:
                self.update(**d)

    # Properties for interfacing with the link to save, and to handle
    # translation between SQL friendly data and the python objects in
    # the data store.
    def _save_state(self):
        # log.debug('Call to _save_state.')
        self.link.save(self._sql_friendly_data)

    @property
    def _sql_friendly_data(self):
        """
        Property to return a version of the data store
        with data types supported by SQL.
        """
        d = copy.deepcopy(self._data)
        for key, val in d.items():
            if isinstance(val, collections.abc.Sequence) and not isinstance(val, str):
                try:
                    d[key] = str(val)
                except TypeError:
                    raise
        # Add the uuid
        d['datalink_uuid'] = self.link.uuid
        return d

    def _format_loaded_data(self):
        """
        Take the loaded data and format it back into python objects.
        Should only be called in initialisation.
        """
        results = list(self.link.loaded_data)
        if len(results) != 1:
            log.warning(f'Ambiguous uuid in loading of data,'
                        f' received {len(results)} results.')
        d = results[0]
        d.pop('id')
        d.pop('datalink_uuid')
        for k, v in d.items():
            try:
                d[k] = ast.literal_eval(v)
            except (ValueError, SyntaxError):
                d[k] = v
        self._data = dict(d)

    # Properties for accessing and updating the data store.
    @property
    def data(self):
        return self._data

    @property
    def uuid(self):
        return self.link.uuid

    def update(self, **kwargs):
        """
        Update multiple properties at once.
        Only uses descriptor directly in last call for
        only one save call.
        """
        for k in kwargs:
            if k not in self.data:
                raise KeyError(f'update received a non data store parameter: {k}')
        for i, (k, v) in enumerate(kwargs.items()):
            if i == len(kwargs) - 1:
                setattr(self, k, v)
            else:
                self._data[k] = v

    # Properties and methods for hashing data and detecting changes
    # in the internal data store state.
    @property
    def _hashable_data(self):
        """Make any unhashable values in the data store hashable."""
        d = copy.deepcopy(self._data)
        for key, val in d.items():
            if isinstance(val, collections.abc.Hashable):
                continue
            else:
                if isinstance(val, collections.abc.Iterable):
                    try:
                        d[key] = tuple(val)
                    except TypeError:
                        raise
                else:
                    raise ValueError(f'Unsupported data store value {val}'
                                     f'in field {key}.')
        return d

    def _get_data_hash(self):
        """
        Creates a hash of the internal data store, casting
        unhashable types to hashables where possible.
        """
        d = self._hashable_data
        # Make a hash and assign it.
        h = hash(json.dumps(d, sort_keys=True))
        return h

    def _set_data_hash(self):
        self._hash_previous = self._get_data_hash()

    @property
    def _has_data_updated(self):
        new_hash = self._get_data_hash()
        if new_hash == self._hash_previous:
            return False
        else:
            return True

# from pandas.util import hash_pandas_object
# import pandas as pd
# import numpy as np

# np.random.seed(42)
# arr = np.random.choice(['foo', 'bar', 42], size=(3,4))
# df = pd.DataFrame(arr)

# df
# h = hash_pandas_object(df)
# type(h)
# h

# arr = np.random.choice(['foo', 'bar', 42], size=(3,4))
# df = pd.DataFrame(arr)
# df
# h2 = hash_pandas_object(df)
# type(h2)
# h2
# h3 = h2.copy()
# h.equals(h2)
# h2.equals(h3)
