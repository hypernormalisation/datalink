"""Module containing the linker classes in datalink.

Links are lower-level objects owned by the stores and not meant to be
interacted with by users.

As a rule, the link should be responsible for holding information
on the SQL database linked to, and be capable of a basic saving and
loading of data into the store.
"""
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
    id_name = 'datalink_id'  # Name of identifier

    def __init__(self, table_name='data', url=None,
                 link_id=None, **kwargs):
        self._table = table_name
        self._id = link_id
        self._url = url
        self.loaded_data = False

        self.ensure_database()

        # Try to load.
        if link_id:
            self.loaded_data = self.load()

    @property
    def engine(self):
        return sqlalchemy.create_engine(self.url)

    @property
    def sql_query(self):
        """The SQL query to match the data."""
        return f'SELECT * FROM {self.table} WHERE {self.id_name}=\'{self.id}\''

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

    @property
    def id(self):
        """The id for the link. If not given, a uuid is assigned."""
        if not self._id:
            self._id = uuid.uuid4()
        return str(self._id)

    @property
    def url(self):
        return self._url

    @property
    def table(self):
        return self._table

    @property
    def adjunct_table(self):
        return f'{self.table}_adjunct'

    # Save and load are abstract methods that are implemented in
    # subclasses depending on how the data is stored in the application.
    @abc.abstractmethod
    def load(self):
        pass

    @abc.abstractmethod
    def save(self, data):
        pass


class SQLInterfaceMap(SQLInterfaceBase):
    """Class for handling links to SQL data that will become
    simple mappings in the application."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.loaded_data:
            log.debug(f'Loaded data corresponding to ID: {self.id}')

    def load(self):
        """Method to attempt a load from the relevant table."""
        with dataset.connect(self.url) as db:
            t = db[self.table]
            find_args = {self.id_name: str(self.id)}
            results = list(t.find(**find_args))
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
                    log.info(f'created table {self.table}')
                    t = db.create_table(self.table, primary_id=False)
                t.insert(data)
        else:
            with dataset.connect(self.url) as db:
                log.debug(f'Updating existing database entry for id {self.id}.')
                t = db[self.table]
                t.update(data, [self.id_name])


class SQLInterfaceFrame(SQLInterfaceBase):
    """Class for handling links to SQL data that will become
       pandas.Dataframe objects in the application."""
    id_name = 'datalink_frame_id'  # denotes the frame as a whole
    row_name = 'datalink_row_id'  # denotes individual rows, for internal use

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # if isinstance(self.loaded_data, pd.DataFrame) and \
        #         not self.loaded_data.empty:
        if self.loaded_data is not False:
            log.debug(f'Loaded data corresponding to ID: {self.id}')

    def find_extant_row_ids(self):
        """Function to return a list of the row ids for any data
        already in the database matching the frame ids.

        Used to delete older data once new data has been written.
        """
        row_ids = []
        with dataset.connect(self.url) as db:
            t = db[self.table]
            find_args = {self.id_name: self.id}
            results = list(t.find(**find_args))
            if not results:
                return []
            for d in results:
                row_ids.append(d[self.row_name])
        return row_ids

    def delete_row_ids(self, row_ids):
        """Delete the rows corresponding to the given row ids."""
        with dataset.connect(self.url) as db:
            t = db[self.table]
            for row_id in row_ids:
                delete_args = {self.row_name: row_id}
                t.delete(**delete_args)

    def load(self):
        """Load the frame. If no data matches return False.

        If no data matches the id pandas.read_sql
        will return an empty frame. In such cases return False
        """
        try:
            df = pd.read_sql(self.sql_query, self.engine)
            if df.empty:
                return False
            return df
        # If table does not exist raises a sqlite3.OperationalError but
        # I'm unsure how to properly except it, hence the bare exception.
        except Exception:
            return False

    def save(self, df):
        """Called by the parent FrameStore in save calls."""

        # If this is the first call for the class we make the table here
        # so alert the user.
        with dataset.connect(self.url) as db:
            if self.table not in db.tables:
                log.info(f'created table {self.table}')

        # Assign the frame and row identifiers.
        df[self.id_name] = self.id
        df[self.row_name] = [str(uuid.uuid4())
                             for _ in range(len(df.index))]

        # Check for the presence of older data matching the frame id.
        # If found, note the row ids for this data before saving, so we
        # can delete the older data once the new data has been safely stored.
        row_ids = self.find_extant_row_ids()

        df.to_sql(self.table, self.engine, if_exists="append", index=False)
        if row_ids:
            log.debug(f'Updating existing database entries for id {self.id}.')
            self.delete_row_ids(row_ids)
        else:
            log.debug(f'Creating new database entries with id {self.id}.')

        # Drop the columns we just added in place.
        df.drop(columns=['datalink_frame_id', 'datalink_row_id'], inplace=True)

    def return_datetime_list(self):
        """Function to interrogate the adjunct table and find the most
        recent timestamp, if any exist. Only used in TemporalFrameStores."""
        with dataset.connect(self.url) as db:
            if self.adjunct_table not in db.tables:
                return []
            t = db[self.adjunct_table]
            dt_list = []
            for entry in t:
                dt_list.append(entry['datetime'])
            return dt_list

    def create_adjunct_entry(self):
        """Function to put an entry into the adjunct table. Only used in
        TemporalFrameStore derivatives, and holds only the datetime of the
        entry for faster queries."""
        with dataset.connect(self.url) as db:
            t = db[self.adjunct_table]
            t.insert(
                {'datetime': self.id}
            )
