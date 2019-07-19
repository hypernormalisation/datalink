import abc
import dataset
import logging
import os
import json
import uuid
import sqlalchemy
from pathlib import Path

log = logging.getLogger(__name__)


def create_database_sql(
        file_path="database.db",
        command="sqlite3 {filepath} \"create table aTable"
                "(field1 int); drop table aTable;\"",
        path_expansions=True):
    """
    Create a database at a specified filepath.
    """
    try:
        if path_expansions:
            file_path = Path(file_path).expanduser()
        log.debug(f'Creating database: {file_path}')
        os.system(command.format(filepath=file_path))
        return file_path
    except Exception:
        log.warning(f'error creating database {file_path}', exc_info=1)
        return False


class SQLInterface:
    """Class to handle all interactions with SQL databases."""

    def __init__(self, db_path=None, table_name='data', datalink_uuid=None):
        self._db_path = db_path
        self._table_name = table_name
        self._uuid = datalink_uuid
        self.ensure_database()
        self.loaded_data = None
        # Try to load.
        if datalink_uuid and self.is_uuid_saved:
            log.debug(f'Loading data corresponding to uuid {self.uuid}')
            self.loaded_data = self.load()

    @property
    def db_path(self):
        """Abstract property for the db location. Implement in derived classes."""
        return Path(self._db_path).expanduser()

    @property
    def db_path_protocol(self):
        return f'sqlite:///{self.db_path}'

    @property
    def engine(self):
        return sqlalchemy.create_engine(self.db_path_protocol)

    @property
    def does_table_exist(self):
        with dataset.connect(self.db_path_protocol) as db:
            if self.table_name in db.tables:
                return True
        return False

    @property
    def table_name(self):
        return self._table_name

    def ensure_database(self):
        """Ensure the database for the type of data exists."""
        if not self.db_path.is_file():
            s = create_database_sql(self.db_path)
            if s:
                log.info('- db created at path: {}'.format(self.db_path))
            else:
                log.error('- failed to create db at path: {}'.format(self.db_path))

    @property
    @abc.abstractmethod
    def uuid(self):
        pass

    @property
    def sql_load_query(self):
        """Abstract property for sql query to be used in loading."""
        return f'SELECT * FROM {self.table_name} WHERE datalink_uuid=\'{self.uuid}\''

    @property
    def is_uuid_saved(self):
        """
        Method to check if the uuid
         is already saved to prevent double saving.
         """
        if self.does_table_exist:
            try:
                with dataset.connect(self.db_path_protocol) as db:
                    t = db[self.table_name]
                    result = t.find(datalink_uuid=str(self.uuid))
                    if list(result):
                        # log.debug('Found uuid')
                        return True
            except Exception:
                raise
        return False

    def load(self):
        """Method to attempt a load from the relevant table."""
        with dataset.connect(self.db_path_protocol) as db:
            t = db[self.table_name]
            result = t.find(datalink_uuid=str(self.uuid))
            return result

    def save(self, data):
        if not self.is_uuid_saved:
            with dataset.connect(self.db_path_protocol) as db:
                log.debug(f'Creating new database entry with uuid {self.uuid}.')
                t = db[self.table_name]
                t.insert(data)
        else:
            with dataset.connect(self.db_path_protocol) as db:
                log.debug(f'Updating existing database entry for uuid {self.uuid}.')
                t = db[self.table_name]
                t.update(data, ['datalink_uuid'])


class UniqueLookup(SQLInterface):
    """A lookup interface based on a uuid4 for a unique entry of data."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # If a uuid4 is given, load it. Maybe better in parent classes?
        # Or have some factory method called here.
        try:
            self.uuid = kwargs.pop('uuid')
            # Do stuff here to populate the data.
        except KeyError:
            pass

    @property
    def uuid(self):
        if not self._uuid:
            self._uuid = uuid.uuid4()
        return str(self._uuid)

    @uuid.setter
    def uuid(self, val):
        try:
            uuid_obj = uuid.UUID(val)
            setattr(self, "_uuid", uuid_obj)
        except ValueError:
            log.error('Supplied uuid is not a valid string for a UUID.')
            raise


class NamespaceLookup(SQLInterface):
    """A lookup interface based on a config driven uuid5."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            config = kwargs.pop("config")
            if isinstance(config, str):
                self._config = [config]
        except KeyError:
            log.error('No config supplied!')
            raise

    @property
    def uuid(self):
        if self._uuid is None:
            self._uuid = uuid.uuid5(uuid.NAMESPACE_DNS,
                                    json.dump(self._config, sort_keys=True))
        return self._uuid
