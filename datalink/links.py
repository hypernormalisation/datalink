import abc
import dataset
import logging
import os
import uuid
import sqlalchemy
import sqlalchemy_utils
from pathlib import Path


log = logging.getLogger(__name__)


class SQLInterface:
    """Class to handle all interactions with SQL databases."""

    def __init__(self, db_path=None, table_name='data',
                 link_id=None, **kwargs):
        if not db_path:
            raise ValueError('db_path is a required field')
        self._db_path = db_path
        self._table_name = table_name
        self._id = link_id
        self.ensure_database()
        self.loaded_data = None
        try:
            self._lookup_dict = kwargs['lookup']
        except KeyError:
            pass

        # Try to load.
        if self.is_id_saved and link_id:
            log.debug(f'Loading data corresponding to ID: {self.id}')
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
        if not sqlalchemy_utils.database_exists(self.db_path_protocol):
            try:
                s = sqlalchemy_utils.create_database(self.db_path_protocol)
                log.info('- db created at path: {}'.format(self.db_path))
            except sqlalchemy.exc.SQLAlchemyError as e:
                log.error('- failed to create db at path: {}'.format(self.db_path))
                log.error(e)
                raise

    @property
    @abc.abstractmethod
    def id(self):
        pass

    @property
    def sql_load_query(self):
        """Abstract property for sql query to be used in loading."""
        return f'SELECT * FROM {self.table_name} WHERE id=\'{self.id}\''

    @property
    def is_id_saved(self):
        """
        Method to check if the uuid
        is already saved to prevent double saving.
        """
        if self.does_table_exist:
            try:
                with dataset.connect(self.db_path_protocol) as db:
                    t = db[self.table_name]
                    result = t.find(id=str(self.id))
                    if list(result):
                        # log.debug(f'Found id {self.id}')
                        return True
            except Exception:
                raise
        return False

    def load(self):
        """Method to attempt a load from the relevant table."""
        with dataset.connect(self.db_path_protocol) as db:
            t = db[self.table_name]
            result = t.find(id=str(self.id))
            return result

    def save(self, data):
        """Method to save or update the relevant entry in the table."""
        if not self.is_id_saved:
            with dataset.connect(self.db_path_protocol) as db:
                log.debug(f'Creating new database entry with id {self.id}.')
                if self.table_name in db.tables:
                    t = db[self.table_name]
                else:
                    t = db.create_table(self.table_name, primary_id=False)
                t.insert(data)
        else:
            with dataset.connect(self.db_path_protocol) as db:
                log.debug(f'Updating existing database entry for id {self.id}.')
                t = db[self.table_name]
                t.update(data, ['id'])


class UUIDLookup(SQLInterface):
    """A lookup interface with a uuid as auto-generated entry identifier."""

    @property
    def id(self):
        if not self._id:
            self._id = uuid.uuid4()
        return str(self._id)
