import copy
import ast
import collections.abc
import datalink.links
import logging
from traits.api import *

log = logging.getLogger(__name__)


# Classes to make traits work.
class ListEntry(HasTraits):
    val = List()
    def __init__(self, v):
        self.val = v


class IntEntry(HasTraits):
    val = Int(0)
    def __init__(self, v):
        self.val = v


class FloatEntry(HasTraits):
    val = Float(0.0)
    def __init__(self, v):
        self.val = v


class StringEntry(HasTraits):
    val = Str('')
    def __init__(self, v):
        self.val = v


# clumsy as fuck
trait_assignment_dict = {
    int: IntEntry,
    float: FloatEntry,
    list: ListEntry,
    str: StringEntry,
}


class DataStore(HasTraits):
    """Class for a basic mapping data store."""
    db_path = None
    table_name = None
    _data_fields = {}
    _datastore_map = {}  # populated in manufactured classes
    self._datastore_atts = []
    self._datastore_traits = []
    lookup = 'uuid'

    def __init__(self, *args, **kwargs):

        if args and not len(args) == 1:
            raise ValueError('Only takes 0 or 1 positional arguments.')

        # Intercept user defined values
        instance_map = copy.deepcopy(self._datastore_map)
        d = {}
        for k, v in kwargs.items():
            if k in instance_map:
                d[k] = v
        for k in d:
            kwargs.pop(k)
        instance_map.update(d)

        # Set the traits
        for attr, value in instance_map.items():
            trait = f'{attr}_trait'
            # print(attr, trait, value)
            for my_type, container in trait_assignment_dict.items():
                if isinstance(value, my_type):
                    setattr(self, trait, container(value))
                    break
            else:
                print('No matching type!')
            getattr(self, trait).on_trait_change(self._save_state, 'val[]')

        # Flags for internal operation.
        self._save_flag = True

        # Establish link
        link_id = None
        if args:
            link_id = args[0]
        self.link = self.get_link(link_id)

        # print(self.link.id)

        # Check for any found data and initialise it.
        if self.link.loaded_data:
            self._format_loaded_data()
        # Save new entries.
        else:
            # print(self.data)
            self._save_state()

    # Intercept trait calls.
    def __getattr__(self, name):
        if name in self._datastore_atts:
            print('rerouting')
            return getattr(self, f'{name}_trait').val
        else:
            print(name)
            print(self.__dict__)
            raise AttributeError(name)

    def __setattr__(self, attr, value):
        if attr in self._datastore_atts:
            self.__dict__[f'{attr}_trait'].val = value
        else:
            self.__dict__[attr] = value

    def get_link(self, link_id):
        """Factory method to construct the link."""
        if self.lookup == 'uuid':  # Only uuid supported at present.
            return datalink.links.UUIDLookup(table_name=self.table_name,
                                             db_path=self.db_path,
                                             link_id=link_id)
        else:
            raise ValueError(self.lookup)

    # Properties for interfacing with the link to save, and to handle
    # translation between SQL friendly data and the python objects in
    # the data store.
    def _save_state(self):
        if self._save_flag:
            self.link.save(self._sql_friendly_data)

    @property
    def data(self):
        d = {k: v for k, v in zip(self._datastore_atts,
                                 [getattr(self, f'{attr}_trait').val
                                  for attr in self._datastore_atts])}
        d['id'] = self.id
        return d

    @property
    def _sql_friendly_data(self):
        """
        Property to return a version of the data store
        with data types supported by SQL.
        """
        d = copy.deepcopy(self.data)
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
    def is_loaded_from_db(self):
        if self.link.loaded_data:
            return True
        else:
            return False

    @property
    def id(self):
        return self.link.id

    def update(self, **kwargs):
        """
        Update multiple properties at once with one push to db.
        """
        self._save_flag = False
        for k in kwargs:
            if k not in self.data:
                raise KeyError(f'update received a non data store parameter: {k}')
        for i, (k, v) in enumerate(kwargs.items()):
            if i == len(kwargs) - 1:
                self._save_flag = True
            setattr(self, k, v)
