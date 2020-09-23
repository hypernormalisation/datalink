import copy
import ast
import collections.abc
import datetime
import datalink.links
import logging
import pandas as pd
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
    on_fail = None

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
    def df(self, new_input):
        if isinstance(new_input, pd.DataFrame):
            df = new_input
            # If a single dict, interpret this as a single row frame.
        elif isinstance(new_input, dict):
            df = pd.DataFrame([new_input])
            # If it's a series, convert to a single row frame
        elif isinstance(new_input, pd.Series):
            df = pd.DataFrame(new_input).T
            # Otherwise attempt to construct a frame.
        else:
            try:
                df = pd.DataFrame(new_input)
            except Exception:
                raise
        # Internally set the frame.
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

        # drop the datalink-specific columns
        df.drop(columns=['datalink_frame_id', 'datalink_row_id'], inplace=True)
        self.df = df


class TemporalFrameStore(FrameStore):
    """A type of store that automatically assigns id based on epochs
    and a maximum age.
    """
    max_age_seconds = None

    def __init__(self):

        self._df = None

        # A flag to only save to the adjunct table
        # on the very first pass.
        self._adjunct_save = True

        # Make a datetime at UTC time now.
        now = datetime.datetime.utcnow()

        # Check the validity of this time.
        most_recent_datetime = self.return_most_recent_datetime(now)

        # If there are no previous entries, use the on_fail if given
        # then return.
        if most_recent_datetime is None:
            if self.on_fail:
                log.info('No previous info found and an on_fail is configured.'
                         ' Creating and saving new frame.')
                self.df = self.on_fail.__func__()
                self.save()
            return

        # If it's good, make a link with the appropriate id as the iso format
        # time as a string and load the data.
        if self.is_datetime_good(now, most_recent_datetime):

            # Forbid saving to the adjunct table if it's not new data.
            self._adjunct_save = False

            data_id = str(most_recent_datetime)
            self.link = self._get_link(data_id)

            if isinstance(self.link.loaded_data, pd.DataFrame):
                self._format_loaded_data()
        else:
            log.info('Previous info found but too old, and an on_fail '
                     'is configured. Creating and saving new frame.')
            if self.on_fail:
                self.df = self.on_fail.__func__()
                self.save()

    def return_most_recent_datetime(self, now):
        """Returns False if no recent entries or the most recent is too old.
        Returns True if it's acceptable.
        """
        link_id = str(now)
        link = self._get_link(link_id)
        self.link = link

        # Get a list of dt_lists and find the most recent
        dt_list_raw = link.return_datetime_list()
        if not dt_list_raw:
            log.debug('No entries found in adjunct table.')
            return None
        dt_list = []
        for dt_raw in dt_list_raw:
            try:
                dt = datetime.datetime.fromisoformat(dt_raw)
                dt_list.append(dt)
            except ValueError:
                pass

        if not dt_list:
            return None

        # If we have any datetimes, find the most recent
        most_recent_dt = None
        if dt_list:
            most_recent_dt = min(dt_list, key=lambda d: abs(d - now))
        return most_recent_dt

    def is_datetime_good(self, now, dt):
        """Evaluates the given datetime dt against now.

        If it's within the acceptable age return True, else False.
        """
        age_most_recent_seconds = (now - dt).seconds
        if age_most_recent_seconds > self.max_age_seconds:
            return False
        else:
            return True

    def save(self):
        """Save as for FrameStore but if necessary save the datetime
        to the adjunct table.
        """
        super().save()
        if self._adjunct_save:
            self.link.create_adjunct_entry()
        self._adjunct_save = False
