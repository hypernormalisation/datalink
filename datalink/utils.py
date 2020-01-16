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


# class IntEntry(HasTraits):
#     val = Int(0)
#     def __init__(self, v):
#         self.val = v
#
#
# class FloatEntry(HasTraits):
#     val = Float(0.0)
#     def __init__(self, v):
#         self.val = v
#
#
# class StringEntry(HasTraits):
#     val = Str('')
#     def __init__(self, v):
#         self.val = v
#
#
# class TupleEntry(HasTraits):
#     val = Tuple()
#     def __init__(self, v):
#         self.val = v
#
# trait_assignment_dict = {
#     int: IntEntry,
#     float: FloatEntry,
#     list: ListEntry,
#     str: StringEntry,
#     tuple: TupleEntry,
# }
