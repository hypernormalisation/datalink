# from weakref import WeakKeyDictionary
#
#
# class ConfigDescriptor(object):
#     """A descriptor that gives access to the config"""
#
#     def __init__(self, default):
#         self.default = default
#         self.data = WeakKeyDictionary()
#
#     def __get__(self, instance, owner):
#         return self.data.get(instance, self.default)
#
#     def __set__(self, instance, value):
#         self.data[instance] = value
#


class DataStoreDescriptor(object):
    """A descriptor for the relevant key in the data store."""

    def __init__(self, key):
        self.key = key

    def __get__(self, instance, owner):
        return instance._data[self.key]

    def __set__(self, instance, value):
        instance._data[self.key] = value
        if instance._has_data_updated:
            instance._save_state()
            instance._set_data_hash()
