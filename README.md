
# Quickstart Olastic

## description
Simple and Lightweight ELasticsearch index to object mapper for python inspired by mongoose

## overview
### Olastic connection wrapper class
when using Olastic it is required to connect to the ELsatic server or cluster first using the Olastic class, otherwise no OlasticSchema would be able to function.
for now Olastic dependens on asyncio coroutines (asyncio plays the same role as Node.js's event loop, coroutines are kind of like Promises in javascript)


```python
import asyncio
from core import Olastic

om = Olastic(hosts=['localhost'], port=9002)
```

### Schema and the @OlasticSchema decorator
after connecting using the Olastic class it is important to create a schema decorated with the OlasticSchema decorator and implementing the Schema interface of Olastic

```python
from core import OlasticSchema, Schema

@OlasticSchema
class Person(Schema):
    # NOTE: it is mandatory to define a default value or type definition like [int(), float(), str(), bool()] in the constructor args in order for Olastic to define the schema correctly on Elastic
    def __init__(self, person_id=0, name="", email="", age=0, employed=False):
        super().__init__()
        self.person_id: int = person_id
        self.name: str = name
        self.email: str = email
        self.age: int = age
        self.employed: bool = employed
```

### Usage
we define an async main function in order to wrap it with the asyncio event loop later

```python
async def main():
    # there two types of operations possible in Olastic Updates, and Queries

    # Updates:
    # to insert a bulk of instances, you need to define a list of dictionaries, with keys consistent with the fields defined in the schema 
    bulk = [
        {"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True},
        {"person_id": 2, "name": "bar", "email": "bar@baz.com", "age": 20, "employed": False},
        {"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30, "employed": True},
        {"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": False}
    ]

    await Person.insert(bulk)

    # or you can insert one instance in two ways
    # pass a dictionary to the create method
    person1: Person = Person.create({"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True})
    person1 = await person1.save()

    # or create an instance of your schema using its constructor
    person2 = Person(person_id=2, name="bar", email="bar@baz.com", age=20, employed=False)
    person2 = await person2.save()

    # Queries:
    # any query has to be built first by channing either of these functions [one of the 'find' queries, sortBy, limit] using the builder pattern
    # after building a query you need to run .exec() on it in order to get its result
    print("find query before flush:")
    people = await Person.find({}).exec()
    print(people)


# wrapping the asyncio event loop to enable the await keyword
if __name__ == "__main__":
    asyncio.run(main())

```
# Specification
## Interfaces
### Schema Interface
All of the methods that can be called by any schema representing an elastic index are defined in this interface"
```
class Schema(Olastic):
    # Schema.__init__ has to be defined in Schema because it has to replace Olastic.__init__
    def __init__(self):
        # self._id = str(functions.uuid_hex())
        pass

    def save(self):
        pass

    @classmethod
    def create(cls, doc_dict, exists=False):
        pass

    @classmethod
    def find(cls, query, start=0, end=10_000) -> OQueryInterface:
        pass

    @classmethod
    def findOne(cls, query, sort_by, asc):
        pass

    @classmethod
    def insert(cls, bulk):
        pass

    @classmethod
    def updateWhere(cls, values, query):
        pass

    @classmethod
    def deleteWhere(cls, query):
        pass

    @classmethod
    def deleteAll(cls):
        pass

    @classmethod
    def termsAgg(cls, fieldname: str):
        pass
```
### Query Builder Interface
the `Schema.find()` method returns a builer object of type `OlasticQuery` that can be used to build update, delete, count, aggregation queries, but if you only want to retrive results u can call `OlasticQuery.exec()`. All of the methods are defined in this interface:
```
class OQueryInterface:
    def sortBy(self, field, asc=True):
        pass
        
    def limit(self, start=0, size=10_000):
        pass

    async def exec(self, asDicts=False):
        pass

    async def count(self):
        pass

    async def update(self, values):
        pass

    async def delete(self):
        pass

    async def aggregate(self, aggs):
        pass
```
