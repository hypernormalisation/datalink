from traits.api import Int, Float, Str, List, HasTraits, Tuple, Generic, Dict


class GenericEntry(HasTraits):
    val = Generic()
    def __init__(self, v):
        self.val = v


# Classes to make traits work.
class ListEntry(HasTraits):
    val = List()
    def __init__(self, v):
        self.val = v


class DictEntry(HasTraits):
    val = Dict()
    def __init__(self, v):
        self.val = v
