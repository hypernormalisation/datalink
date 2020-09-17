import copy
import ast
import collections.abc
import datalink.links
import logging
import pandas as pd
import uuid
from datalink.utils import GenericEntry, ListEntry
from traits.api import HasTraits


log = logging.getLogger(__name__)


class DataStore(HasTraits):
    """Class for a basic mapping data store."""
    url = None
    table = None
    _fields = {}
    _bidirectional = True

    def __init__(self, *args, **kwargs):

        if args and not len(args) == 1:
            raise ValueError('Only takes K0 or 1 positional arguments.')

        # Intercept user defined values
        instance_map = copy.deepcopy(self._fields)
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
            getattr(self, trait).on_trait_change(
                self._conditional_save_state, 'val[]'
            )

        # Flags for internal operation.
        self._save_flag = True

        # Establish link
        link_id = None
        if args:
            link_id = args[0]
        self.link = self._get_link(link_id)

        # Check for any found data and initialise it.
        if self.link.loaded_data:
            self._format_loaded_data()
            # If already exists and the user passed
            # args, update the entry.
            if datastore_arg_dict:
                self.update(**datastore_arg_dict)

        # Save new entries.
        else:
            self._conditional_save_state()

    def __getattr__(self, name):
        """Intercept traits."""
        if name in self._fields:
            if self._bidirectional:
                self._force_load()
            return getattr(self, f'{name}_trait').val
        else:
            raise AttributeError(name)

    def __setattr__(self, attr, value):
        if attr in self._fields:
            self.__dict__[f'{attr}_trait'].val = value
        else:
            self.__dict__[attr] = value

    def __bool__(self):
        """Return true if data for the id was found in the database."""
        if self.link.loaded_data:
            return True
        return False

    # Properties for interfacing with the link to save, and to handle
    # translation between SQL friendly data and the python objects in
    # the data store.
    def _get_link(self, link_id):
        """Factory method to construct the link."""
        return datalink.links.SQLInterfaceMap(
            table_name=self.table,
            url=self.url,
            link_id=link_id
        )

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
        d = {k: v for k, v in zip(self._fields,
                                  [getattr(self, f'{attr}_trait').val
                                   for attr in self._fields])}
        d['datalink_id'] = self.id
        return d

    @property
    def _sql_friendly_data(self):
        """
        Property to return a version of the data store
        with data types supported by SQL backends.
        """
        d = copy.deepcopy(self._data)
        for key, val in d.items():
            if isinstance(val, collections.abc.Sequence) and not isinstance(val, str):
                try:
                    d[key] = str(val)
                except TypeError:
                    raise
        # Add the uuid
        d[self.link.id_name] = self.link.id
        return d

    def _format_loaded_data(self):
        """
        Take the loaded data and format it back into python objects.
        Should only be called in initialisation.
        """
        d = self.link.loaded_data
        d.pop('id')
        self._save_flag = False
        for k, v in d.items():
            try:
                v = ast.literal_eval(v)
            except (ValueError, SyntaxError):
                pass
            setattr(self, k, v)
        self._save_flag = True

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


class FrameStore:
    """Class to contain a dataframe that is loaded from and saved to
    an SQL database
    """
    url = None
    table = None
    conversion = False

    def __init__(self, *args, df=None):

        if args and not len(args) == 1:
            raise ValueError('Only takes K0 or 1 positional arguments.')

        self._df = None

        # Establish link
        link_id = None
        if args:
            link_id = args[0]
        self.link = self._get_link(link_id)

        if isinstance(self.link.loaded_data, pd.DataFrame):
            self._format_loaded_data()

        if df is not None:
            self.df = df
            self.save()

    def __bool__(self):
        """Return true if data for the id was found in the database."""
        if self.link.loaded_data is not False:
            return True
        return False

    def _get_link(self, link_id):
        """Factory method to construct the link."""
        return datalink.links.SQLInterfaceFrame(
            table_name=self.table,
            url=self.url,
            link_id=link_id
        )

    @property
    def id(self):
        return self.link.id

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, data):

        # Needs different logic for when we get something other than a frame.
        df = data.copy(deep=True)

        self._df = df

    def save(self):
        """Method to save the frame's contents.

        If the conversion class attribute is set, all contents of the frame
        will be saved as strings. This can be useful if you have lists or
        other types in the dataframe's cells that sqlite cannot handle.
        """
        if isinstance(self.df, pd.DataFrame) and not self.df.empty:
            if self.conversion:
                df_as_str = self.df.copy().astype(str)
                self.link.save(df_as_str)
            else:
                self.link.save(self.df)

    def _format_loaded_data(self):
        df = self.link.loaded_data
        for column in df.columns:
            try:
                df[column] = df.apply(
                    lambda row: ast.literal_eval(row[column]), axis=1)
            except (NameError, SyntaxError, ValueError):
                pass
        df.apply(pd.to_numeric, errors='ignore')
        self.df = df
