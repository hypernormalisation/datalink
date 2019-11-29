import copy
import ast
import collections.abc
import json
import datalink.links
import logging

log = logging.getLogger(__name__)


class DataStore:
    """Class for a basic mapping data store."""
    db_path = None
    table_name = None
    _data_fields = {}
    lookup = 'uuid'

    def __init__(self, *args, **kwargs):

        if args and not len(args) == 1:
            raise ValueError('Only takes 0 or 1 positional arguments.')

        self._hash_previous = None
        self._data = self._data_fields

        link_id = None
        if args:
            link_id = args[0]

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
        self.link = self.get_link(link_id)

        # Check for any found data and initialise it.
        if self.link.loaded_data:
            self._format_loaded_data()

        # Initialise any variables from the declaration.
        if d:
            self.update(**d)

        # If default config and a new entry, save anyway.
        if not d and not self.link.loaded_data:
            self._save_state()

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
        for k, v in d.items():
            try:
                d[k] = ast.literal_eval(v)
            except (ValueError, SyntaxError):
                d[k] = v
        self._data = dict(d)

    # Properties for accessing and updating the data store.
    @property
    def is_loaded_from_db(self):
        if self.link.loaded_data:
            return True
        else:
            return False

    @property
    def data(self):
        return self._data

    @property
    def id(self):
        return self.link.id

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
        """Make any un-hashable values in the data store hashable."""
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
        un-hashable types to hashable where possible.
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
