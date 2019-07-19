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

log = logging.getLogger(__name__)


def link_factory(
        name=None, db_path=None,
        table_name=None, data_fields=None, config=None
        ):
    """
    Factory function to produce a new class derived from DataStore.
    """
    class NewClass(dlstores.DataStore):
        pass

    if not name:
        raise ValueError('name is a required field.')
    NewClass.__name__ = name

    if not db_path:
        raise ValueError('db_path is a required field.')
    NewClass.db_path = db_path

    if not table_name:
        raise ValueError('table_name is a required field.')
    NewClass.table_name = table_name

    if not data_fields and not isinstance(data_fields, collections.abc.Mapping):
        raise ValueError('data_fields must be a valid mapping.')
    NewClass._data_fields = data_fields

    # Handle namespace lookup config - not working at present.
    if config:
        NewClass._config = config
    return NewClass


def test_output():
    log.info('logging from datalink')


# Nonstandard lookup functionality is UNDER CONSTRUCTION
# MyStore2 = datalink.link_factory(name='MyStore2', db_path='/tmp/test2.db', table_name='data',
#                                  config='client_id',
#                                  data_fields={'name': None, 'age': None, 'postcode': None})
# s = MyStore2(name='John Doe', age=36, postcode='M1 111')


# class Metadata(Base):
#     """Properties for metadata objects."""
#
#     def __init__(self, **kwargs):
#         super().__init__()
#
#         # Ensure lookup attributes are present and given.
#         if not hasattr(self, "_identifiers"):
#             log.error('Subclasses must define self._identifiers, the '
#                            'attributes used in metadata lookups!')
#             raise ValueError('Change your subclass definition.')
#
#         self.attrs = {}
#         for attr in self._identifiers:
#             try:
#                 val = kwargs.pop(attr)
#                 self.attrs[attr] = val
#             except KeyError:
#                 log.error(f'Missing argument "{attr}" for '
#                                f'{self.__class__.__name__} construction.')
#                 raise
#
#         result = self.load()
#         if not result:
#             log.debug('No data found for this configuration.')
#         elif result:
#             log.debug('Found and loaded data for this configuration.')
#
#     @property
#     def table_name(self):
#         values = []
#         for attr in sorted(self._identifiers):
#             # If an attr is a list need to make the string
#             # predictable through iteration and ordering.
#             val = self.attrs.get(attr)
#             if (isinstance(val, collections.abc.MutableSequence) or
#                     isinstance(val, tuple)):
#                 for element in val:
#                     if (isinstance(element, collections.abc.MutableSequence)
#                             or isinstance(element, tuple)):
#                         raise ValueError('Nested containers are not supported for '
#                                          'lookup attributes.')
#                 val = tuple(sorted(val))
#             if isinstance(val, collections.abc.Mapping):
#                 raise ValueError('Mappings are not supported for lookup attributes.')
#             values.append(str(val))
#         s = '_'.join(list(values))
#         for char in CHARS_TO_REMOVE:
#             s = s.replace(char, '')
#         return s
#
#     @property
#     def sql_query(self):
#         """Abstract property for sql query to be used in loading."""
#         return f'{self.table_name}'
#
#
# ############################################################################
# # Abstract classes for datastores
# ############################################################################
# class Frame:
#     """Base class for a frame to be persisted and loaded from SQL."""
#
#     @property
#     def data(self):
#         if not hasattr(self, '_data'):
#             self._data = None
#         if self._data is None:
#             return self._data
#         return self._data
#
#     @data.setter
#     def data(self, new_input):
#         """Take the input and ensure a DataFrame is correctly inserted as data."""
#         # If we get a DataFrame, assign it internally.
#         if isinstance(new_input, pd.DataFrame):
#             df = new_input
#         # If we get a single dict, interpret this as a single row frame.
#         elif isinstance(new_input, dict):
#             df = pd.DataFrame([new_input])
#         # If it's a series, make it a single row frame
#         elif isinstance(new_input, pd.Series):
#             df = pd.DataFrame(new_input).T
#         # Else throw it at the frame constructor.
#         else:
#             try:
#                 df = pd.DataFrame(new_input)
#             except Exception:
#                 raise
#         if 'dt' not in df.columns:
#             df['dt'] = int(datetime.utcnow().timestamp()*1000)
#         # Internally set the frame.
#         self._data = df
#
#     @property
#     def has_data(self):
#         """Property to indicate if the data is loaded and valid."""
#         if self.data is None:
#             return False
#         if isinstance(self.data, dict):
#             return all(v is None for v in d.values())
#         if isinstance(self.data, pd.DataFrame):
#             return not self.data.empty
#         log.error('Unexpected data type received! Check this class.')
#         return False
#
#     def convert_sql_data(self, df):
#         """Convert the SQL data into the expected python object types."""
#         # Try to eval columns to restore collections.
#         log.debug('Converting string vars')
#         for column in df.columns:
#             try:
#                 df[column] = df.apply(lambda row: eval(row[column]), axis=1)
#             except (NameError, SyntaxError, ValueError):
#                 pass
#         df.apply(pd.to_numeric, errors='ignore')
#         return df
#
#     def load(self):
#         """Method to load data. If valid data is found, returns True, else False"""
#         if self.does_table_exist:
#             try:
#                 df = pd.read_sql(self.sql_query, self.engine)
#                 # display(df)
#                 df = self.convert_sql_data(df)
#                 if df.empty:
#                     return False
#                 self.data = df
#                 self._has_loaded_data = True
#                 # log.debug(f'Loaded data for config.')
#                 return True
#             except Exception:
#                 raise
#         return False
#
#
# class Mapping:
#     """Base class for a simple mapping, to represent a data store."""
#     @property
#     def data(self):
#         if not hasattr(self, '_data'):
#             self._data = None
#         return self._data
#
#     @data.setter
#     def data(self, new_input):
#         """Take the input and ensure a dict is correctly inserted as data."""
#         if not hasattr(self, '_data'):
#             self._data = None
#         if self._data is not None:
#             log.warning('Internal data store has already been initialised! '
#                              'For safety, the data setter is now disabled, use the generated'
#                              ' and implemented property setters instead.')
#             log.warning(f'Valid setters: {self._data.keys()}')
#             return
#         try:
#             if 'group_uuid4' not in new_input.keys():
#                 new_input['group_uuid4'] = self.group_uuid4
#             if 'dt' not in new_input.keys():
#                 new_input['dt'] = int(datetime.utcnow().timestamp()*1000)
#             self._data = new_input
#         except (AttributeError, KeyError) as e:
#             log.error(f'Failed to add standard keys and set data store: {e}')
#
#     @property
#     def has_data(self):
#         """Property to indicate if the data is loaded and valid."""
#         if self.data is not None:
#             if isinstance(self._data, dict):
#                 return True
#         return False
#
#
# ############################################################################
# # Classes that real applications should subclass from.
# ############################################################################
# class UniqueFrame(Unique, Frame):
#     """
#     Class for a frame of unique data.
#
#     Subclasses can simply give a name and docstring, e.g.:
#     >>> class MyFrame(UniqueFrame):
#     >>>     pass
#
#     The first instance of MyFrame will create a database MyFrame.db in the
#     specified recording dict.
#
#     If the internal datastore is provided with either a valid frame, or
#     python collections which can be used in the pd.DataFrame constructor,
#     it will be assigned a group_uuid4 for lookup, and a datetime
#     corresponding to when the data was assigned.
#
#     Each row in the frame
#
#     MyFrame.save() can then be used to save the DataFrame to the SQL db,
#     in the table table_MyFrame.
#
#     The group_uuid4 can be used to later retrieve the data with e.g.
#
#     >>> m = MyFrame(group_uuid4='dac2f879-f6d5-40a0-ad98-25806dd6c579')
#
#     which will load the data if it exists, and raise a ValueError if not.
#     """
#
#     def __init__(self, *args, **kwargs):
#         # Ensure database
#         Base.__init__(self)
#         # Load the data if uuid4 is given.
#         Unique.__init__(self, **kwargs)
#
#     @property
#     def data(self):
#         return super().data
#
#     @data.setter
#     def data(self, new_input):
#         """Override Frame setter to also add uuid4s if needed."""
#         Frame.data.fset(self, new_input)
#         df = self._data
#         if 'uuid4' not in df.columns:
#             df['uuid4'] = [uuid.uuid4() for _ in range(len(df.index))]
#         if 'group_uuid4' not in df.columns:
#             df['group_uuid4'] = self.group_uuid4
#         # Internally set the frame.
#         self._data = df
#
#     def save(self):
#         """
#         Method to persist the data to the relevant db. All cells converted to strings.
#         """
#         if self.is_group_uuid4_saved:
#             self.delete_group()
#
#         if not self.has_data:
#             log.warning(f'Call to save data without any data set!')
#             return
#
#         data_copy = self.data.copy().astype(str)
#         data_copy.to_sql(self.table_name, self.engine, if_exists="append", index=False)
#         log.debug(f'Persisted data to table {self.table_name}.')
#
#
# class MetadataFrame(Frame, Metadata):
#     """
#     Class for a frame of metadata.
#
#     Subclasses are required to give a property _identifiers, containing a
#     list of attribute names to be used in the lookup.
#
#     >>> class MyMetadata(MetadataFrame):
#     >>>     _identifiers = ['name', 'age']
#
#     Identifiers are not permitted to be nested collections or mappings.
#     Strings, unnested lists, numericals etc. are all permitted.
#
#     All instances of MyMetadata will be required to give "name" and "age"
#
#     The first instance of MyMetadata will create a database MyMetadata.db in the
#     specified recording dict.
#     """
#
#     def __init__(self, *args, **kwargs):
#         # Ensure database
#         Base.__init__(self)
#         # Load the data if uuid4 is given.
#         Metadata.__init__(self, **kwargs)
#
#     def save(self):
#         """
#         Method to persist the data to the relevant db.
#         All cells converted to strings.
#         """
#         if not self.has_data:
#             log.debug(f'Empty dataframe is being saved')
#         data_copy = self._data.copy().astype(str)
#         data_copy.to_sql(self.table_name, self.engine, if_exists="replace", index=False)
#         log.info(f'Persisted data to table {self.table_name}.')
#
#     def load(self):
#         # log.debug('Metadata loading')
#         result = Frame.load(self)
#         if result:
#             log.debug(f'Loaded config for {self.table_name}')
#         return result
#
#
# class UniqueMapping(Mapping, Unique):
#     """Class for a unique mapping of data."""
#     def __init__(self, **kwargs):
#         # A container for the last known condition of the data
#         # when it was most recently loaded or saved.
#         self._data_last_save = None
#
#         # Load the data if uuid4 is given.
#         Unique.__init__(self, **kwargs)
#
#         # If data was loaded, skip to property generation
#         if not self.has_loaded_data:
#
#             # Now dynamically set attributes for the data store.
#             # Get all required args. Throw error if missing.
#             # We use an OrderedDict to remember the insertion order,
#             # so the required, optional, and derived attrs go in
#             # in the correct order.
#             attrs = collections.OrderedDict()
#
#             # First any required args.
#             if hasattr(self, '_required_attrs'):
#                 for attr in self._required_attrs:
#                     try:
#                         val = kwargs.pop(attr)
#                         attrs[attr] = val
#                     except KeyError:
#                         log.error(f'Missing argument "{attr}" for '
#                                        f'{self.__class__.__name__} construction.')
#                         raise
#
#             # Add optional attrs with defaults second.
#             # Once the setters and getters are generated, we will apply user overrides.
#             if hasattr(self, '_optional_attrs'):
#                 for attr, default_value in self._optional_attrs.items():
#                     try:
#                         val = kwargs.pop(attr)
#                         attrs[attr] = val
#                     except KeyError:
#                         attrs[attr] = default_value
#
#             # Add derived attrs last, initialised to None.
#             if hasattr(self, '_derived_attrs'):
#                 for attr in self._derived_attrs:
#                     attrs[attr] = None
#
#             # Now make the data, but init to Nones...
#             self.data = dict(zip(attrs.keys(), [None for _ in attrs.keys()]))
#             # ... generate the getter and setters as needed ...
#             self.expand_properties()
#             # ... and use settings, including user defined, to fill the data store.
#             self.set_attrs_with_setters(attrs)
#
#         # If we already have data, just expand the properties.
#         else:
#             self.expand_properties()
#
#     ############################################################################
#     # Properties
#     ############################################################################
#     @property
#     def data(self):
#         if not hasattr(self, '_data'):
#             self._data = None
#         else:
#             self.update_read_only()
#         return self._data
#
#     @data.setter
#     def data(self, new_input):
#         """The mapping data setter is fine, but we need a class definition
#         here for scope reasons."""
#         Mapping.data.fset(self, new_input)
#
#     ############################################################################
#     # Methods for dynamic property setting
#     ############################################################################
#     def set_attrs_with_setters(self, attrs):
#         log.debug('Using property setters to initialise data store values.')
#
#         # Make a list of read-only properties and assign them
#         # after any autogenerated properties.
#         read_only_attrs = {}
#
#         for attr in attrs:
#             # log.debug(f'Configuring datastore attribute: {attr}')
#             if hasattr(self.__class__, attr):
#                 # If the attribute has a setter, use it.
#                 prop = getattr(self.__class__, attr)
#                 if hasattr(prop, 'fset') and prop.fset is not None:
#                     try:
#                         # log.debug(f'Setting attr: {attr}, {attrs[attr]}')
#                         setattr(self, attr, attrs[attr])
#                     except AttributeError:
#                         raise
#                 # If it does not, we are dealing with a read only property.
#                 # This property's data store value should be initialised to the
#                 # getter, and it should be manually set in the save call in
#                 # the UniqueMapping functionality.
#                 else:
#                     read_only_attrs[attr] = attrs[attr]
#
#         for attr in read_only_attrs:
#             try:
#                 # log.debug(f'Setting property for read only store var {attr}')
#                 self.set_data_val(attr, None)
#             except AttributeError:
#                 raise
#
#     # Functions the user can use in derived class getters and setters,
#     # to always reliably return the data, regardless of this class's structure.
#     # Useful for persistence redevelopments.
#     # DO NOT CALL DIRECTLY IN INSTANCES, just in class bodies.
#     def get_data_val(self, attr):
#         return self._data[attr]
#
#     def set_data_val(self, attr, value):
#         self._data[attr] = value
#
#     def expand_properties(self):
#         """
#         Method to dynamically implement property getters and setters for the
#         attributes in the data store. Respects any manually declared properties
#         in the concrete implementations.
#         """
#         # The below do not need getters or setters, or have specialised ones.
#         to_ignore = ['dt', 'uuid4', 'group_uuid4']
#
#         # Respect attributes with explicitly defined properties.
#         new_props = [k for k in list(self._data.keys()) if
#                      (k not in to_ignore and not hasattr(self.__class__, k))]
#
#         if new_props:
#             log.debug(f'Setting new {self.__class__.__name__} properties: {new_props}')
#         for attr in new_props:
#             setattr(self.__class__, attr,
#                     property(
#                         lambda self, attr=attr: self._data.get(attr),
#                         lambda self, value, attr=attr: self.set_data_val(attr, value)
#                         )
#                     )
#
#     def get_read_only_datastore_properties(self):
#         """Function to interrogate the class and find read only property attributes."""
#         data_store_attrs = [k for k in self._data.keys() if k in dir(self.__class__)]
#         read_only_attrs = [a for a in data_store_attrs if
#                            getattr(self.__class__, a).fset is None]
#         return read_only_attrs
#
#     def update_read_only(self):
#         """
#         Function to update the internal data store for class properties
#         which are read only.
#         """
#         attr_names = self.get_read_only_datastore_properties()
#         # log.warning(f'read only attrs to update: {attr_names}')
#         for attr in attr_names:
#             # log.warning(f'{attr}: {getattr(self, attr)}')
#             self.set_data_val(attr, getattr(self, attr))
#
#     @property
#     def uuid_found(self):
#         """Property to indicate if the data is loaded and valid."""
#         if self.data is None:
#             return False
#         if isinstance(self.data, dict):
#             return all(v is None for v in d.values())
#         if isinstance(self.data, pd.DataFrame):
#             return not self.data.empty
#         log.error('Unexpected data type received! Check this class.')
#         return False
#
#     @property
#     def data_store_as_strs(self):
#         """Method to return a copy of the internal data store, with all
#         attributes converted to strings."""
#         new_d = {}
#         for key, val in self.data.items():
#             if isinstance(val, str):
#                 new_d[key] = val
#             else:
#                 new_d[key] = repr(val)
#         # log.warning('Dict being saved:')
#         # print(new_d)
#         return new_d
#
#     def save(self):
#         """
#         Method to persist the data store to the relevant db.
#         All cells converted to strings.
#         """
#         if not self.has_data:
#             log.debug(f'Call to save data without any data set!')
#             return
#
#         # Update the read only attributes before the save.
#         self.update_read_only()
#
#         # If the data store is different to the state of the last save, save.
#         if not self._data_last_save == repr(self._data):
#             if self.is_group_uuid4_saved:
#                 self.delete_group()
#                 print(self._data)
#             self._data_last_save = repr(self.data)
#
#             with dataset.connect(self.db_path_sql) as db:
#                 t = db[self.table_name]
#                 t.insert(self.data_store_as_strs)
#             log.debug(f'Persisted data to table {self.table_name}')
#         else:
#             log.debug('Data unchanged since last save, not writing')
#
#     # @staticmethod
#     def return_evaluated_dict(self, d):
#         """Method to return the dict with all string values evaled or converted."""
#         new_d = {}
#         log.warning(d)
#
#         # Clumsy as hell, but import the names we need here.
#
#         for attr, val in d.items():
#             try:
#                 print(attr, val)
#                 if val is not None:
#                     attr_eval = eval(val)
#                     if not isinstance(attr_eval, str):
#                         new_d[attr] = attr_eval
#                     else:
#                         new_d[attr] = str(val)
#                 else:
#                     new_d[attr] = val
#             except (NameError, SyntaxError) as e:
#                 log.warning(e)
#                 new_d[attr] = str(val)
#         log.warning(new_d)
#         return new_d
#
#     def load(self):
#         """Method to load data. If valid data is found, returns True, else False"""
#         # Try to load the mapping from the database.
#         with dataset.connect(self.db_path_sql) as db:
#             t = db[self.table_name]
#             result = t.find(group_uuid4=str(self.group_uuid4))
#
#         result_as_list = list(result)
#         log.warning(f'result_as_list: {result_as_list}')
#         if not result_as_list:
#             log.debug(f'No match found for {self.group_uuid4}')
#             return False
#
#         # If we have a result, format it, and convert the strings
#         # to the required python objects.
#         if len(result_as_list) != 1:
#             log.error(f'db {self.table_name} contains multiple matches for group_uuid4'
#                            f' {self.group_uuid4}, figure out why!')
#             raise ValueError('Multiple uuid4s match.')
#         result = result_as_list[0]
#         result.pop('id')  # remove the automated id from the dataset module.
#         d = self.return_evaluated_dict(result)
#
#         # Assign the internal datastore and the copy for the last persistence interaction.
#         self._data_last_save = repr(d)
#         self.data = d
#
#         # A bool to be used in the inits.
#         self._has_loaded_data = True
#         return True
#
#
# # ############################################################################
# # # Test classes
# # ############################################################################
# # class TestUniqueFrame(UniqueFrame):
# #     """Test class for persisted unique frame objects."""
# #     pass
# #
# #
# # class TestMetadataFrame(MetadataFrame):
# #     """Test class for persisted metadata frame objects."""
# #     _identifiers = ['name', 'infrastructure']
# #
# #
# # class TestUniqueMapping(UniqueMapping):
# #     """Test class for persisted unique mappings."""
# #     _required_attrs = ['name', 'age', 'secret_fact']
# #     # These are parameters for the internal data store which will
# #     # be initialised to None, unless a property setter is defined here.
# #     _derived_attrs = ['times_taken_smack_squared']
# #     # Optional attributes need a map to a default.
# #     _optional_attrs = {'impure': True, 'times_taken_smack': 0}
# #
# #     @property
# #     def times_taken_smack_squared(self):
# #         return self.times_taken_smack ** 2
