import abc
import dataset
import logging
import uuid
import sqlalchemy
import sqlalchemy_utils
from sqlalchemy import Table, MetaData


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

        try:
            self._lookup_dict = kwargs['lookup']
        except KeyError:
            pass

        # Try to load.
        if self.is_id_saved and link_id:
            log.debug(f'Loading data corresponding to ID: {self.id}')
            self.loaded_data = self.load()

    @property
    def url(self):
        return self._url

    @property
    def engine(self):
        return sqlalchemy.create_engine(self.url)

    @property
    def does_table_exist(self):
        with dataset.connect(self.url) as db:
            if self.table in db.tables:
                return True
        return False

    @property
    def table(self):
        return self._table

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

    @property
    @abc.abstractmethod
    def id(self):
        pass

    @property
    def is_id_saved(self):
        """
        Method to check if the uuid
        is already saved to prevent double saving.
        """
        if self.does_table_exist:
            try:
                with dataset.connect(self.url) as db:
                    t = db[self.table]
                    result = t.find(id=str(self.id))
                    if list(result):
                        # log.debug(f'Found id {self.id}')
                        return True
            except Exception:
                raise
        return False

    def load(self):
        engine = self.engine
        con = engine.connect()
        metadata = MetaData()
        tab = Table(self.table, metadata, autoload=True, autoload_with=engine)
        columns = tab.columns.keys()
        query = sqlalchemy.select([tab]).where(tab.columns.id == self.id)
        proxy = con.execute(query)
        values = proxy.fetchall()
        if values:
            values = values[0]
            return {k: v for k, v in zip(columns, values)}
        else:
            log.warning(f'Ambiguous uuid in loading of data,'
                        f' received {len(values)} results.')
            return None

    # def load2(self):
    #     """Method to attempt a load from the relevant table."""
    #     with dataset.connect(self.url) as db:
    #         t = db[self.table]
    #         result = t.find(id=str(self.id))
    #         return result

    def save(self, data):
        """Method to save or update the relevant entry in the table."""
        if not self.is_id_saved:
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
