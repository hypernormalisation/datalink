# `datalink`

`datalink` is a simple python module that provides a way for users to generate classes that contain attributes linked to entries in a database, such as SQL.

These attributes are easy to access, and their values can be mutable objects that may be changed in place, with the link to the underlying database entries maintained.

By default, if the database entries in question are updated elsewhere in the program or by another actor, these interfaces will remain up-to-date.


```python
import logging
import warnings
import datalink
import sys
import random
warnings.filterwarnings('ignore')
```

`datalink` supports the `logging` module


```python
logging.basicConfig(format='%(levelname)s | %(message)s',
                    level=logging.DEBUG,
                    stream=sys.stdout)
log = logging.getLogger(__name__)
datalink.test_output()

# Datalink logging can be disabled with
# logging.getLogger('datalink').propagate = False
```

    INFO | Test logging output from datalink.


Datalink logging can be disabled with
```python
logging.getLogger('datalink').propagate = False
```

### Creating user datalink classes

The `factory` method is used to create new classes that the user can utilise,
in a similar fashion to `namedtuple`-based classes from the `collections` module.

Some required arguments are:
- `name`: the new class's name
- `db_path`: the simple file path to the database, without any dialect-specific protocol.
- `table_name`: the name of the table to be saved to in the database.

None of the above database structures have to exist already - `datalink` will create them for you if necessary.

The last required field is the `data_fields` property.
This must be a mapping, where each database column name (and subsequent data-store variable) is a key, and the value is the default value.
Any properties here are said to comprise the "data-store".

If any value is to be mutable, e.g. a `list` or `dict`, then the correct python typing of that value must be given as a default, e.g. ```[]``` for a list.

Immutable types can be defaulted, and changed, to any value specified, including ```None```.


```python
```python
```python
my_order_fields ={
    'client_name': None,
    'shipping_address': None, 
    'items':[],
    'cost': 0.0
    }

Order = datalink.factory(name='Order', database='/tmp/my_ledger.db',
                         table_name='Orders',
                         fields=my_order_fields)
```

### Creating and manipulating `Order` instances

Now let's make an instance of the `Order` class with the default settings.


```python
o = Order()
```

    INFO | sqlite db created at path: /tmp/my_ledger.db
    DEBUG | Creating new database entry with id 4256c6e9-6979-47e7-b483-12d744c4de78.


As this is the first `Order` instance we have created, the associated database is created.
The new order is also saved to the database immediately upon creation.
The order is automatically assigned a uuid as its identifier, accessible through the read-only `id` property.

Now lets alter some of the data properties of the `Order`.


```python
print(o.client_name, o.shipping_address, o.cost, o.items)
o.client_name = 'Alice Smith'
o.shipping_address = '123 Leaf Avenue, Sometown'
o.items.append('bracket')
o.cost += 11.50
```

    None None 0.0 []
    DEBUG | Updating existing database entry for id 4256c6e9-6979-47e7-b483-12d744c4de78.
    DEBUG | Updating existing database entry for id 4256c6e9-6979-47e7-b483-12d744c4de78.
    DEBUG | Updating existing database entry for id 4256c6e9-6979-47e7-b483-12d744c4de78.
    DEBUG | Updating existing database entry for id 4256c6e9-6979-47e7-b483-12d744c4de78.


As soon as the assignent is made to the `Order` instance's data-store attributes, the appropriate database entry is updated.

The `update` method can be used with keywords to update multiple entries at once, with only 
a single write operation to the database.


```python
o.update(shipping_address='123 Leaf Way, Sometown', client_name='Alice C Smith')
```

    DEBUG | Updating existing database entry for id 4256c6e9-6979-47e7-b483-12d744c4de78.


A dictionary containing all data-store variables is exposed through the read-only `data` property. 


```python
o.data
```




    {'client_name': 'Alice C Smith',
     'shipping_address': '123 Leaf Way, Sometown',
     'items': ['bracket'],
     'cost': 11.5,
     'id': '4256c6e9-6979-47e7-b483-12d744c4de78'}



### Key-word data-store instantiation
Attributes in the data-store can be defined upon the instantiation of an `Order` by key-word.


```python
o2 = Order(client_name='Beatrice Smith', address='456 Rock Drive, Someothertown',
          items=['paint_black', 'small_brush'], cost=22.40)
o2.data
```

    DEBUG | Creating new database entry with id dac2db7a-a275-440a-8137-1c8808949ed7.





    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 22.4,
     'id': 'dac2db7a-a275-440a-8137-1c8808949ed7'}



### Loading persisted orders

We can use the identifier of our previous order as a positional argument to load it from the database.


```python
print(o2.id)
o3 = Order(o2.id)
```

    dac2db7a-a275-440a-8137-1c8808949ed7
    DEBUG | Loading data corresponding to ID: dac2db7a-a275-440a-8137-1c8808949ed7



```python
o3.data
```




    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 22.4,
     'id': 'dac2db7a-a275-440a-8137-1c8808949ed7'}



If the user provides the order id as a positional argument, keyword arguments can also be supplied to update data-store attributes in a single expression.


```python
Order(o3.id, cost=30.0)
o3.data
```

    DEBUG | Loading data corresponding to ID: dac2db7a-a275-440a-8137-1c8808949ed7
    DEBUG | Updating existing database entry for id dac2db7a-a275-440a-8137-1c8808949ed7.





    <traits.has_traits.Order at 0x7f86cba5d110>






    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 30.0,
     'id': 'dac2db7a-a275-440a-8137-1c8808949ed7'}



### Updates to interface items

My default the links are bidirectional, in that not only will a change to an individual Order push a result back to the database, but any changes made to the database entry are propagated back to all related links.


```python
o3.items.append('paint_red')
print(o2.data) # data exposed here is guaranteed to be up-to-date.
print(o2.items)  # data exposed here is guaranteed to be up-to-date.
```

    DEBUG | Updating existing database entry for id dac2db7a-a275-440a-8137-1c8808949ed7.
    {'client_name': 'Beatrice Smith', 'shipping_address': None, 'items': ['paint_black', 'small_brush', 'paint_red'], 'cost': 30.0, 'id': 'dac2db7a-a275-440a-8137-1c8808949ed7'}
    ['paint_black', 'small_brush', 'paint_red']


To have updates only be pushed in the direction from the program using links to the database, pass ```bidirectional=False``` agrument to `datalink.factory`.

## User specified ids
For new entries, the user can supply an id themselves as the first argument, which will override the assigned uuid.


```python
o4 = Order('mynewid', name='Bob Smith')
```

    DEBUG | Creating new database entry with id mynewid.


### Metadata lookup with user specified ids

The property `is_new` is provided in linker classes to allow the user to determine if any data was loaded from the database in an instance, or if the instance is new.

This allows for user-specified ids to be used to e.g. record data/metadata with patterns like the following


```python
```python
```python
particle_defaults = {'charge': None, 'efficiency': None}
Particle = datalink.factory(name='Particle', table_name='particles',
                            database='/tmp/particles.db', fields=particle_defaults)

def run_efficiency():
    d = {}
    for p in ['electron', 'proton', 'muon', 'neutron']:
        part = Particle(p)
        if part.is_new:
            # Do some possibly intensive calculation here to save the properties
            part.charge = random.choice([-1,0,1])
            part.efficiency = random.random()
        d[p] = part
    # Now do something with the efficiencies. They only have to be calculated once
    # and will be persisted afterwards.
    proton = d['proton']
    electron = d['electron']
    efficiency_p_p_e = proton.efficiency * proton.efficiency * electron.efficiency
    return efficiency_p_p_e

run_efficiency()
```

    INFO | sqlite db created at path: /tmp/particles.db
    DEBUG | Creating new database entry with id electron.
    DEBUG | Updating existing database entry for id electron.
    DEBUG | Updating existing database entry for id electron.
    DEBUG | Creating new database entry with id proton.
    DEBUG | Updating existing database entry for id proton.
    DEBUG | Updating existing database entry for id proton.
    DEBUG | Creating new database entry with id muon.
    DEBUG | Updating existing database entry for id muon.
    DEBUG | Updating existing database entry for id muon.
    DEBUG | Creating new database entry with id neutron.
    DEBUG | Updating existing database entry for id neutron.
    DEBUG | Updating existing database entry for id neutron.





    0.0012810827265243262



Now we re-run the function and we find that the stored values are loaded up
from the database for us.


```python
run_efficiency()
```

    DEBUG | Loading data corresponding to ID: electron
    DEBUG | Loading data corresponding to ID: proton
    DEBUG | Loading data corresponding to ID: muon
    DEBUG | Loading data corresponding to ID: neutron





    0.0012810827265243262


