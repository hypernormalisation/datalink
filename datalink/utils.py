# from traits.api import List
# import collections.abc

#
# class DataStoreDescriptor:
#     """A descriptor for the relevant key in the data store."""
#
#     def __init__(self, key):
#         self.key = key
#
#     def __get__(self, instance, owner):
#         return instance._data[self.key]
#
#     def __set__(self, instance, value):
#         # if (isinstance(value, collections.abc.Sequence)
#         #         and not isinstance(value, str)):
#         #     instance._data[self.key] = List(value)
#         # else:
#         instance._data[self.key] = value
#         if instance._has_data_updated:
#             instance._save_state()
#             instance._set_data_hash()
