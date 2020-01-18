import abc
import dataset
import logging
import uuid
import sqlalchemy
import sqlalchemy_utils


log = logging.getLogger(__name__)


class SQLInterface:
    """Class to handle all interactions with SQL databases."""

    def __init__(self, table_name='data', url=None,
                 link_id=None, **kwargs):
        self._table = table_name
        self._id = link_id
        self._url = url
        self.loaded_data = None

        self.ensure_database()
        self.ensure_table()

        try:
            self._lookup_dict = kwargs['lookup']
        except KeyError:
            pass

        # Try to load.
        if link_id:
            self.loaded_data = self.load()
            if self.loaded_data:
                log.debug(f'Loaded data corresponding to ID: {self.id}')

    def ensure_database(self):
        """Ensure the database for the type of data exists."""
        if not sqlalchemy_utils.database_exists(self.url):
            try:
                s = sqlalchemy_utils.create_database(self.url)
                log.info(f'db created at: {self.url}')
            except sqlalchemy.exc.SQLAlchemyError as e:
                log.error(f'failed to create db at: {self.url}')
                log.error(e)
                raise

    def ensure_table(self):
        with dataset.connect(self.url) as db:
            if self.table not in db.tables:
                t = db[self.table]
                log.info(f'created table {self.table}')

    @property
    @abc.abstractmethod
    def id(self):
        pass

    @property
    def url(self):
        return self._url

    @property
    def table(self):
        return self._table

    def load(self):
        """Method to attempt a load from the relevant table."""
        with dataset.connect(self.url) as db:
            t = db[self.table]
            results = list(t.find(id=str(self.id)))
            if not results:
                return None
            if len(results) > 1:
                log.warning(f'Ambiguous uuid in loading of data,'
                            f' received {len(results)} results. Taking first')
            return results[0]

    def save(self, data):
        """Method to save or update the relevant entry in the table."""
        if not self.load():
            with dataset.connect(self.url) as db:
                log.debug(f'Creating new database entry with id {self.id}.')
                if self.table in db.tables:
                    t = db[self.table]
                else:
                    t = db.create_table(self.table, primary_id=False)
                t.insert(data)
        else:
            with dataset.connect(self.url) as db:
                log.debug(f'Updating existing database entry for id {self.id}.')
                t = db[self.table]
                t.update(data, ['id'])


class UUIDLookup(SQLInterface):
    """A lookup interface with a uuid as auto-generated entry identifier."""

    @property
    def id(self):
        if not self._id:
            self._id = uuid.uuid4()
        return str(self._id)
