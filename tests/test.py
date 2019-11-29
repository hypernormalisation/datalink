import datalink
import sys
import logging
logging.basicConfig(format='%(levelname)s | %(message)s',
                    level=logging.DEBUG,
                    stream=sys.stdout)

MyStore = datalink.link_factory(name='MyStore', db_path='/tmp/test.db', table_name='data',
                                data_fields={'a': None, 'b': [2, 4], 'c': 'a string'})

ClientStore = datalink.link_factory(name='ClientStore', db_path='/tmp/test2.db',
                                    table_name='clients',
                                    data_fields={'name': '', 'address': None})

c1 = ClientStore('001', name='Alice Smith', address='1 Snarfington Avenue')
c2 = ClientStore('002', name='Bob Ross2', address='8 Barf Drive')
c3 = ClientStore('003')
