import abc
import dataset
import logging
import uuid
import pandas as pd
import sqlalchemy
import sqlalchemy_utils


log = logging.getLogger(__name__)


class SQLInterfaceBase:
    """Abstract base class to template interactions with SQL databases."""

    def __init__(self, table_name='data', url=None,
                 link_id=None, **kwargs):
        self._table = table_name
        self._id = link_id
        self._url = url
        self.loaded_data = None

        self.ensure_database()
        self.ensure_table()

        # Try to load.
        if link_id:
            self.loaded_data = self.load()

    def ensure_database(self):
        """Ensure the database for the type of data exists."""
        if not sqlalchemy_utils.database_exists(self.url):
            try:
                sqlalchemy_utils.create_database(self.url)
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
    def id(self):
        if not self._id:
            self._id = uuid.uuid4()
        return str(self._id)

    @property
    def url(self):
        return self._url

    @property
    def table(self):
        return self._table

    # Save and load are abstract methods that are implemented in
    # subclasses depending on the type of data required.
    @abc.abstractmethod
    def load(self):
        pass

    @abc.abstractmethod
    def save(self, data):
        pass


class SQLInterfaceMap(SQLInterfaceBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.loaded_data:
            log.debug(f'Loaded data corresponding to ID: {self.id}')

    def load(self):
        """Method to attempt a load from the relevant table."""
        with dataset.connect(self.url) as db:
            t = db[self.table]
            results = list(t.find(datalink_id=str(self.id)))
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
                t.update(data, ['datalink_id'])


class SQLInterfaceFrame(SQLInterfaceBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.loaded_data, pd.DataFrame) and \
                not self.loaded_data.empty:
            log.debug(f'Loaded data corresponding to ID: {self.id}')

    @property
    def engine(self):
        return sqlalchemy.create_engine(self.url)

    @property
    def sql_query(self):
        """Abstract property for sql query to be used in loading."""
        return f'SELECT * FROM {self.table} WHERE datalink_id=\'{self.id}\''

    @property
    def is_id_saved(self):
        """Method to check if the group_uuid4 is already saved to prevent double saving."""
        # if self.does_table_exist:
        if True:
            try:
                df = pd.read_sql(self.sql_query, self.engine)
                print(df, df.empty)
                if not df.empty:
                    return True
            except Exception:
                pass
        return False

    def delete_group(self):
        """
        Method to delete all entries in the table matching the id of the data.
        Should be used when overwriting
        """
        with dataset.connect(self.url) as db:
            t = db[self.table]
            t.delete(datalink_id=str(self.id))

    def ensure_unique(self):
        """Method to ensure that this frame's data is unique by deleting
        old instances before a save operation"""
        if self.is_id_saved:
            self.delete_group()

    def ensure_table(self):
        """Does nothing with pandas, override to skip."""
        pass

    def load(self):
        # Odd behaviour with dataset table creation, workaround.
        with dataset.connect(self.url) as db:
            if self.table not in db.tables:
                return False

        # If table exists, load.
        try:
            df = pd.read_sql(self.sql_query, self.engine)
            return df
        except Exception:
            raise

    def save(self, df):
        self.ensure_unique()
        df.to_sql(self.table, self.engine, if_exists="append", index=False)
