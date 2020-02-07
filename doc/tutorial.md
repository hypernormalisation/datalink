# Tutorial

`datalink` is a python module that lets you interact with entries of SQL
data as if you were using dictionaries.

Loading, saving, unique identification, and database management all take
place behind the scenes via the excellent `dataset` module, so the user
doesn't need to worry about databases at all.


## Logging

`datalink` supports the `logging` module


```python
logging.basicConfig(format='%(levelname)s | %(message)s',
                    level=logging.DEBUG,
                    stream=sys.stdout)
log = logging.getLogger(__name__)
datalink.test_output()
```

    INFO | Test logging output from datalink.


Datalink logging can be disabled with

```python
logging.getLogger('datalink').propagate = False
```

# Creating user datalink classes

The `datalink` module's `factory` function is used to create new classes
that the user can utilise, in a similar fashion to `namedtuple` in the
`collections` module.

The 3 required position arguments are:
- the class name
- the name of the table in the database
- the defaults for the data. This must be a mapping, where each database
  column name (and subsequent data-store variable) is a key, and the
  value is the default value.

e.g.

```python
my_album_fields = {'title': '', 'artist': '', 'tracks': []}
```

If any value is to be mutable, e.g. a `list` or `dict`, then the correct
python typing of that value must be given as a default, e.g. ```[]```
for a list. Immutable types can be defaulted, and changed, to any value
specified, including `None`.

## Database location

Additionally, factory requires at least one of the following keyword
arguments:
- to use the file-based databases of sqlite, give a location for the
  database as the `database` keyword.
- if you know the full url of the database in question, supply this via
  the `url` keyword.

## `datalink` creates everything for you

None of the databases or tables have to exist already - `datalink` will
create them for you if necessary.

# An example with factory

Below we will make an example class representing a shopping order.

The data will be located in an sqlite database at `/tmp/my_ledger.db`,in
the table `orders`.

Our class will be called "Order", with some information on the client
and their shopping cart.

We will give the `database` keyword argument to specify the use of a
file-backed sqlite database.

```python
import datalink

my_order_fields = {
    'client_name': None,
    'shipping_address': None, 
    'items': [],
    'cost': 0.0
    }

Order = datalink.factory('Order', 'orders', my_order_fields, 
                         database='/tmp/my_ledger.db')
```

## Creating and manipulating `Order` instances

Now let's make an instance of the `Order` class with the default
settings.


```python
o = Order()
```

    INFO | db created at: sqlite:////tmp/my_ledger.db
    INFO | created table orders
    DEBUG | Creating new database entry with id ac41793f-af4d-49ef-bdc4-0d807c1207c5.


As this is the first `Order` instance we have created, the associated
database and table are created. The new order is also saved to the
database immediately upon creation. The order is automatically assigned
a uuid as its identifier, accessible through the read-only `id`
property.

Now lets alter some of the data properties of the `Order`.


```python
print(o.client_name, o.shipping_address, o.cost, o.items)
o.client_name = 'Alice Smith'
o.shipping_address = '123 Leaf Avenue, Sometown'
o.items.append('bracket')
o.cost += 11.50
```

    None None 0.0 []
    DEBUG | Updating existing database entry for id ac41793f-af4d-49ef-bdc4-0d807c1207c5.
    DEBUG | Updating existing database entry for id ac41793f-af4d-49ef-bdc4-0d807c1207c5.
    DEBUG | Updating existing database entry for id ac41793f-af4d-49ef-bdc4-0d807c1207c5.
    DEBUG | Updating existing database entry for id ac41793f-af4d-49ef-bdc4-0d807c1207c5.


As soon as the assignent is made to the `Order` instance's data-store
attributes, the appropriate database entry is updated.

The `update` method can be used with keywords to update multiple entries
at once, with only a single write operation to the database.


```python
o.update(shipping_address='123 Leaf Way, Sometown', client_name='Alice C Smith')
```

    DEBUG | Updating existing database entry for id ac41793f-af4d-49ef-bdc4-0d807c1207c5.


A dictionary containing all data-store variables is exposed through the
read-only `data` property.


```python
o.data
```
    {'client_name': 'Alice C Smith',
     'shipping_address': '123 Leaf Way, Sometown',
     'items': ['bracket'],
     'cost': 11.5,
     'id': 'ac41793f-af4d-49ef-bdc4-0d807c1207c5'}


### Data-store instantiation with keywords

Attributes in the data-store can be set upon the instantiation of an
`Order` by keyword.

```python
o2 = Order(client_name='Beatrice Smith',
           address='456 Rock Drive, Someothertown',
           items=['paint_black', 'small_brush'],
           cost=22.40)
o2.data
```

    DEBUG | Creating new database entry with id 605cadde-c820-4130-bb8f-646e34981085.

    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 22.4,
     'id': '605cadde-c820-4130-bb8f-646e34981085'}


### Loading orders
We can use the identifier of our previous order as a positional argument
to load it from the database.

```python
print(o2.id)
o3 = Order(o2.id)
```

    605cadde-c820-4130-bb8f-646e34981085
    DEBUG | Loaded data corresponding to ID: 605cadde-c820-4130-bb8f-646e34981085


```python
o3.data
```


    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 22.4,
     'id': '605cadde-c820-4130-bb8f-646e34981085'}


If the user provides the order id as a positional argument, keyword
arguments can also be supplied to update data-store attributes in a
single expression.


```python
Order(o3.id, cost=30.0)
o3.data
```

    DEBUG | Loaded data corresponding to ID: 605cadde-c820-4130-bb8f-646e34981085
    DEBUG | Updating existing database entry for id 605cadde-c820-4130-bb8f-646e34981085.

    {'client_name': 'Beatrice Smith',
     'shipping_address': None,
     'items': ['paint_black', 'small_brush'],
     'cost': 30.0,
     'id': '605cadde-c820-4130-bb8f-646e34981085'}


### Updates to interface items

My default the links are bidirectional, in that not only will a change
to an individual Order push a result back to the database, but any
changes made to the database entry are propagated back to all related
links.


```python
o3.items.append('paint_red')
print(o2.data) # data exposed here is guaranteed to be up-to-date.
print(o2.items)  # data exposed here is guaranteed to be up-to-date.
```

    DEBUG | Updating existing database entry for id 605cadde-c820-4130-bb8f-646e34981085.
    {'client_name': 'Beatrice Smith', 'shipping_address': None, 'items': ['paint_black', 'small_brush', 'paint_red'], 'cost': 30.0, 'id': '605cadde-c820-4130-bb8f-646e34981085'}
    ['paint_black', 'small_brush', 'paint_red']


To have updates only be pushed in the direction from the program using
links to the database, pass the ```bidirectional=False``` agrument to
`datalink.factory`.

## User specified ids

For new entries, the user can supply an id themselves as the first
argument, which will override the assigned uuid.


```python
o4 = Order('mynewid', name='Bob Smith')
```

    DEBUG | Creating new database entry with id mynewid.


### Metadata lookup with user specified ids

The class's boolean is overridden to detect if any data was loaded from
the database, returning `True` if the internal data was loaded from the
database, and `False` if the data within the instance is new.

This allows for user-specified ids to be used to record data/metadata
which is intensive to calculate.

We give the example below of measuring particles in a detector with some
efficiency response that is hard to compute:


```python
# Make a new container to represent a particle.
particle_defaults = {'efficiency': None}
Particle = datalink.factory('Particle','particles', particle_defaults,
                            database='/tmp/particles.db')

from functools import reduce
logging.getLogger().setLevel(logging.INFO) # suppress datalink debug logging

def assign_efficiency(part):
    '''Some dummy function that is time consuming.'''
    print(f'Doing an intensive calculation for {part.id} efficiency response.')
    part.efficiency = random.random()    

def get_efficiency(*particles):
    ''' Calculate the efficiency of detecting an event with the input particles'''
    d = {}
    for p in particles:
        if p not in d:
            part = Particle(p)
            if not part:
                assign_efficiency(part)
            d[p] = part
    efficiency = reduce(lambda x, y: x*y, [d[p].efficiency for p in particles])
    for p in set(particles):
        print(f'efficiency for {p} = {d[p].efficiency}')
    print(f'efficiency for {particles} = {efficiency}')
```


```python
get_efficiency('proton', 'proton', 'electron')
```

    INFO | db created at: sqlite:////tmp/particles.db
    INFO | created table particles
    Doing an intensive calculation for proton efficiency response.
    Doing an intensive calculation for electron efficiency response.
    efficiency for proton = 0.834742453258431
    efficiency for electron = 0.782964970609321
    efficiency for ('proton', 'proton', 'electron') = 0.5455660479389091


Now we calculate another efficiency and find that the stored values for
protons are reused.


```python
get_efficiency('proton', 'proton', 'muon')
```

    Doing an intensive calculation for muon efficiency response.
    efficiency for proton = 0.834742453258431
    efficiency for muon = 0.926626185771311
    efficiency for ('proton', 'proton', 'muon') = 0.645668459081305


```python
get_efficiency('electron', 'proton', 'muon')
```

    efficiency for proton = 0.834742453258431
    efficiency for muon = 0.926626185771311
    efficiency for electron = 0.782964970609321
    efficiency for ('electron', 'proton', 'muon') = 0.6056188757557402

