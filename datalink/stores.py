import copy
import ast
import collections.abc
import datalink.links
import logging
from datalink.utils import GenericEntry, ListEntry
from traits.api import HasTraits

log = logging.getLogger(__name__)


class DataStore(HasTraits):
    """Class for a basic mapping data store."""
    db_path = None
    table_name = None
    _data_fields = {}
    _datastore_map = {}  # populated in manufactured classes
    lookup = None
    dialect = None
    bidirectional = True

    def __init__(self, *args, **kwargs):

        if args and not len(args) == 1:
            raise ValueError('Only takes K0 or 1 positional arguments.')

        # Intercept user defined values
        instance_map = copy.deepcopy(self._datastore_map)
        datastore_arg_dict = {}
        for k, v in kwargs.items():
            if k in instance_map:
                datastore_arg_dict[k] = v
        for k in datastore_arg_dict:
            kwargs.pop(k)
        instance_map.update(datastore_arg_dict)

        # Set the traits
        for attr, value in instance_map.items():
            trait = f'{attr}_trait'
            if isinstance(value, collections.abc.Iterable) and not isinstance(value, str):
                setattr(self, trait, ListEntry(value))
            else:
                setattr(self, trait, GenericEntry(value))
            getattr(self, trait).on_trait_change(self._conditional_save_state, 'val[]')

        # Flags for internal operation.
        self._save_flag = True

        # Establish link
        link_id = None
        if args:
            link_id = args[0]
        if 'dialect' in kwargs:
            self.dialect = kwargs.pop('dialect')
        self.link = self.get_link(link_id)

        # Check for any found data and initialise it.
        if self.link.loaded_data:
            self._format_loaded_data()
            # If the user passed args, update the entry.
            if datastore_arg_dict:
                self.update(**datastore_arg_dict)

        # Save new entries.
        else:
            self._conditional_save_state()

    # Intercept trait calls.
    def __getattr__(self, name):
        # Update from SQL entry.
        if name in self._datastore_map:
            if self.bidirectional:
                self._force_load()
            return getattr(self, f'{name}_trait').val
        else:
            raise AttributeError(name)

    def __setattr__(self, attr, value):
        if attr in self._datastore_map:
            self.__dict__[f'{attr}_trait'].val = value
        else:
            self.__dict__[attr] = value

    # def __str__(self):
    #     return str(self._data)

    # Properties for interfacing with the link to save, and to handle
    # translation between SQL friendly data and the python objects in
    # the data store.
    def get_link(self, link_id):
        """Factory method to construct the link."""
        if self.lookup == 'uuid':  # Only uuid supported at present.
            return datalink.links.UUIDLookup(table_name=self.table_name,
                                             db_path=self.db_path,
                                             dialect=self.dialect,
                                             link_id=link_id)
        else:
            raise ValueError(self.lookup)

    def _force_load(self):
        self.link.loaded_data = self.link.load()
        self._format_loaded_data()

    def _conditional_save_state(self):
        """Push an update to the db only if the _save_flag is true."""
        if self._save_flag:
            self.link.save(self._sql_friendly_data)

    def save(self):
        """Method to force a save."""
        self.link.save(self._sql_friendly_data)

    @property
    def data(self):
        """Property exposing the most up-to-date version of the data."""
        self._force_load()
        return self._data

    @property
    def _data(self):
        """For use internally."""
        d = {k: v for k, v in zip(self._datastore_map,
                                  [getattr(self, f'{attr}_trait').val
                                   for attr in self._datastore_map])}
        d['id'] = self.id
        return d

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
        d['id'] = self.link.id
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
        self._save_flag = False
        for k, v in d.items():
            try:
                v = ast.literal_eval(v)
            except (ValueError, SyntaxError):
                pass
            setattr(self, k, v)
        self._save_flag = True

    # Properties for accessing and updating the data store.
    @property
    def is_loaded(self):
        if self.link.loaded_data:
            return True
        else:
            return False

    @property
    def is_new(self):
        if self.link.loaded_data:
            return False
        else:
            return True

    @property
    def id(self):
        return self.link.id

    def update(self, **kwargs):
        """
        Update multiple properties at once with one push to db.
        """
        self._save_flag = False
        for k in kwargs:
            if k not in self._data:
                raise KeyError(f'update received a non data store parameter: {k}')
        for i, (k, v) in enumerate(kwargs.items()):
            if i == len(kwargs) - 1:
                self._save_flag = True
            setattr(self, k, v)
