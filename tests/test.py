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
                                    data_fields={'name': '', 'address': None},
                                    lookup='user')

c1 = ClientStore('001', name='Alice Smith', address='1 Snarfington Avenue')
c2 = ClientStore('002', name='Bob Ross2', address='8 Barf Drive')
c3 = ClientStore('003')
# d = MyStore()
# print(d.data, d.id)
# d.a = 18
#
# d2 = MyStore('7afc6ea4-2b4d-4232-b895-38e691bd5496')
# # d2 = MyStore('7afc6ea4-2b4d-4232-b895-38e691bd54961')
# print(d2.data, d2.id)
#
