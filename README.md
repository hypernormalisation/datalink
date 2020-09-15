<!--[![PyPI version fury.io](https://badge.fury.io/py/ansicolortags.svg)](https://pypi.python.org/pypi/ansicolortags/)-->
<!--[![PyPI license](https://img.shields.io/pypi/l/ansicolortags.svg)](https://pypi.python.org/pypi/ansicolortags/)-->

# datalink
`datalink` is a python module that lets you interact with entries of SQL
data as if you were simply using dictionaries.

The module is built on top of `sqlalchemy` and `dataset`, and all SQL
schemas supported in `sqlalchemy` are supported in `datalink`.

Using `datalink.factory`, the user creates a new class, each instance of
which is linked to a single entry of data stored in an SQL database.
Pushing changes to this SQL entry is as simple as modifying the
instance's attributes.

Loading, saving, unique identification, and database management all
take place behind the scenes, so the user doesn't need to worry about
databases at all.

# Installation
`datalink` is available on the Python Package Index.
```
pip install datalink
```

# Tutorial
A full tutorial is avaiable
[here](https://github.com/hypernormalisation/datalink/blob/master/doc/tutorial.md).

## Creating datalinks
If you've ever used `NameTuple` from the `collections` module, you know
how to use `datalink`.

Let's make a template for our data and an instance of that template,
backed up to a file-based `sqlite` database.

Our SQL table will have the columns `a`, `b`, and `c` with the defaults
set in `my_data_fields`.


```python
import datalink
my_data_fields = {'a': 0, 'b': 'a string', 'c': []}  # default entries
MyClass = datalink.factory('MyClass', 'my_table', my_data_fields,
                           database='/tmp/my.db')
                           
A = MyClass()    
```
    INFO | db created at: sqlite:////tmp/my.db
    INFO | created table my_table
    DEBUG | Creating new database entry with id ce267917-6c2e-46d1-bacc-c52affba2d2d.


We can also instantiate data in the instance declaration.
We can then use the `data` property to expose a dict containing a map of
the column name to value to make sure it has been correctly set.


```python
B = MyClass(a=100, b='a new string')
B.data
```

    DEBUG | Creating new database entry with id e2bd9f8c-af49-4188-96c2-b6f4ea771188.

    {'a': 100,
     'b': 'a new string',
     'c': [],
     'id': 'e2bd9f8c-af49-4188-96c2-b6f4ea771188'}



## Modifying data
Now let's modify some elements and see what happens. They are accessible
as properties in `A` as determined by the defaults we gave the factory.



```python
A.a = 5
A.c.append(None)
print(A.a, A.c)
```

    DEBUG | Updating existing database entry for id ce267917-6c2e-46d1-bacc-c52affba2d2d.
    DEBUG | Updating existing database entry for id ce267917-6c2e-46d1-bacc-c52affba2d2d.
    5 [None]


Multiple updates can be pushed to the SQL database at once with the
`update` method.



```python
A.update(a=1000, c=[1,2,3])
A.data
```
    DEBUG | Updating existing database entry for id ce267917-6c2e-46d1-bacc-c52affba2d2d.

    {'a': 1000,
     'b': 'a string',
     'c': [1, 2, 3],
     'id': 'ce267917-6c2e-46d1-bacc-c52affba2d2d'}

## Loading data
As we can see, each new instance of `MyClass` receives a Unique
Universal Identifier (UUID), that can be used to easily fetch rows from
the database as a positional argument.



```python
print(A.id)
id_to_load = A.id
del A
C = MyClass(id_to_load)
C.data
```
    'ce267917-6c2e-46d1-bacc-c52affba2d2d'

    DEBUG | Loaded data corresponding to ID: ce267917-6c2e-46d1-bacc-c52affba2d2d

    {'a': 1000,
     'b': 'a string',
     'c': [1, 2, 3],
     'id': 'ce267917-6c2e-46d1-bacc-c52affba2d2d'}



## User-specified identification
Users can also specify their own identifiers in the construction of new
instances of MyClass to replace the uuid.

This can be useful in design patterns that want to store easily
accessible configuration or metadata.


```python
D = MyClass('myid', a=200, b='some other string', c=[4,5,6])
```

    DEBUG | Creating new database entry with id myid.

